package pubsub

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/timex"
)

// Subscriber runs a handler for every message of a [Source]. Create it with
// [NewSubscriber] or [NewBatchSubscriber] and call Run once.
type Subscriber struct {
	src     Source
	handler Handler
	batch   BatchHandler
	o       options
	log     *log.Logger
	tel     *Telemetry
	nackLog *sampledLog
	started atomic.Bool

	mu     sync.Mutex
	closed bool
	cancel context.CancelFunc
	done   chan struct{}
}

// NewSubscriber returns a Subscriber that calls h for each message.
//
// Unkeyed messages are handled in parallel (up to WithConcurrency). Messages
// sharing a Key are handled one at a time in delivery order; when one of them
// is finally nacked, the queued messages of that key are nacked too so the
// broker redelivers the key in order.
func NewSubscriber(src Source, h Handler, opts ...Option) (*Subscriber, error) {
	if h == nil {
		return nil, errors.New("pubsub: nil handler")
	}
	return newSubscriber(src, h, nil, opts)
}

// NewBatchSubscriber returns a Subscriber that calls h with batches.
//
// Messages are spread over WithConcurrency shards by Key (unkeyed messages
// round robin). Each shard collects up to the WithBatch size or flush interval
// and processes its batches one at a time, so same-key messages keep their
// order. See [BatchHandler] and [BatchError] for per-message results. When the
// whole batch fails after its retries, each still-failing message is run once
// on its own to isolate a poison message; handlers must be idempotent.
func NewBatchSubscriber(src Source, h BatchHandler, opts ...Option) (*Subscriber, error) {
	if h == nil {
		return nil, errors.New("pubsub: nil batch handler")
	}
	return newSubscriber(src, nil, h, opts)
}

func newSubscriber(src Source, h Handler, bh BatchHandler, opts []Option) (*Subscriber, error) {
	if src == nil {
		return nil, errors.New("pubsub: nil source")
	}
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if !o.maxInFlightSet && o.concurrency > 0 {
		o.maxInFlight = 2 * o.concurrency
		if bh != nil {
			o.maxInFlight = o.concurrency * max(o.maxBatchSize, 1)
		}
	}
	switch {
	case o.concurrency < 1:
		return nil, fmt.Errorf("pubsub: concurrency %d < 1", o.concurrency)
	case o.maxInFlight < 1:
		return nil, fmt.Errorf("pubsub: max in-flight %d < 1", o.maxInFlight)
	case o.maxRetries < 0:
		return nil, fmt.Errorf("pubsub: max retries %d < 0", o.maxRetries)
	case bh != nil && o.maxBatchSize < 1:
		return nil, fmt.Errorf("pubsub: batch size %d < 1", o.maxBatchSize)
	case bh != nil && o.flushInterval <= 0:
		return nil, fmt.Errorf("pubsub: flush interval %v <= 0", o.flushInterval)
	case bh != nil && o.maxInFlight < o.maxBatchSize:
		return nil, fmt.Errorf("pubsub: max in-flight %d < batch size %d", o.maxInFlight, o.maxBatchSize)
	case o.handlerTimeout < 0 || o.shutdownTimeout < 0:
		return nil, errors.New("pubsub: negative timeout")
	case o.keyFenceTTL <= 0:
		return nil, fmt.Errorf("pubsub: key fence TTL %v <= 0 (use WithKeyFence(KeyFenceOff))", o.keyFenceTTL)
	case o.keyFence < KeyFenceAuto || o.keyFence > KeyFenceOff:
		return nil, fmt.Errorf("pubsub: invalid %v", o.keyFence)
	}
	l := log.OrDefault(o.logger).With("messaging.system", src.System())
	tel := NewTelemetry(src.System(), o.telemetry...)
	if ci, ok := src.(ConsumerIdentifier); ok {
		c := ci.Consumer()
		tel.setConsumer(c)
		if c.Group != "" {
			l = l.With("messaging.consumer.group.name", c.Group)
		}
		if c.Subscription != "" {
			l = l.With("messaging.destination.subscription.name", c.Subscription)
		}
	}
	return &Subscriber{
		src:     src,
		handler: h,
		batch:   bh,
		o:       o,
		log:     l,
		tel:     tel,
		nackLog: newSampledLog(l, nackLogRate),
	}, nil
}

// Run receives and handles messages until ctx is cancelled or the source fails.
//
// On return every received message has been acked or nacked and every
// goroutine started by Run has exited. When ctx is cancelled, Run stops
// receiving, nacks messages that have not started, and gives running handlers
// up to WithShutdownTimeout to finish before cancelling their contexts. Run
// returns nil after a cancellation (including [Subscriber.Close]) and the
// source error otherwise. Run may be called only once; it returns ErrClosed
// when called again. After Close (even one that raced ahead of Run's start)
// it returns nil at once, as for a cancelled ctx.
func (s *Subscriber) Run(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrClosed
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.cancel = cancel
	s.done = make(chan struct{})
	defer close(s.done)
	s.mu.Unlock()

	r := &run{
		s:        s,
		inflight: make(chan struct{}, s.o.maxInFlight),
		workers:  make(chan struct{}, s.o.concurrency),
		drainCh:  make(chan struct{}),
		keys:     make(map[string]*keyQueue),
		fences:   newKeyFences(),
	}
	r.advisor, _ = s.src.(RedeliveryAdvisor)
	return r.run(ctx)
}

// Close stops a running Run as if its context was cancelled and waits until
// Run has drained and returned, or until ctx is done. A Run that starts after
// Close returns nil at once. Close is safe to call more than once and before
// Run.
func (s *Subscriber) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	cancel, done := s.cancel, s.done
	s.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("pubsub: close: %w", ctx.Err())
	}
}

type run struct {
	s        *Subscriber
	inflight chan struct{}
	workers  chan struct{}
	drainCh  chan struct{}
	hctx     context.Context
	wg       sync.WaitGroup

	mu      sync.Mutex
	keys    map[string]*keyQueue
	fences  *keyFences
	advisor RedeliveryAdvisor
	seq     atomic.Uint64

	shards []*shard
	rr     atomic.Uint64
	seed   maphash.Seed
}

type keyQueue struct {
	items []Delivery
	// stale counts the head of items to nack unprocessed: when a message of
	// the key is finally nacked without a fence (KeyFenceOff, or a source
	// that reports Redelivery.Fence false), the messages already queued were
	// handed over before that nack and must not run before its redelivery.
	// Messages that join the queue later are processed: the broker orders
	// them after the redelivery. With a fence, the fence decides.
	stale int
}

func (r *run) draining() bool {
	select {
	case <-r.drainCh:
		return true
	default:
		return false
	}
}

func (r *run) run(ctx context.Context) error {
	recvCtx, cancelRecv := context.WithCancel(ctx)
	defer cancelRecv()
	hctx, abort := context.WithCancel(context.WithoutCancel(ctx))
	defer abort()
	r.hctx = hctx

	if r.s.batch != nil {
		r.seed = maphash.MakeSeed()
		r.shards = make([]*shard, r.s.o.concurrency)
		for i := range r.shards {
			sh := &shard{signal: make(chan struct{}, 1)}
			r.shards[i] = sh
			r.wg.Go(func() { r.shardLoop(sh) }) //nolint:contextcheck // handlers run on r.hctx, which outlives the receive ctx for graceful shutdown
		}
	}

	recvErr := make(chan error, 1)
	go func() { recvErr <- r.s.src.Receive(recvCtx, r.receive) }()

	var err error
	received := false
	select {
	case <-ctx.Done():
	case err = <-recvErr:
		received = true
	}
	close(r.drainCh)
	cancelRecv()
	timer := time.AfterFunc(r.s.o.shutdownTimeout, abort)
	defer timer.Stop()
	if !received {
		err = <-recvErr
	}
	r.wg.Wait()
	if err != nil && ctx.Err() != nil && errors.Is(err, context.Canceled) {
		err = nil
	}
	if err != nil {
		return fmt.Errorf("pubsub: receive: %w", err)
	}
	return nil
}

// tracked releases the in-flight slot when settled.
type tracked struct {
	Delivery
	r    *run
	seq  uint64 // arrival order in this Run
	once sync.Once
}

// seqOf returns the arrival order of a delivery handed to the subscriber.
func seqOf(d Delivery) uint64 {
	if t, ok := d.(*tracked); ok {
		return t.seq
	}
	return 0
}

func (t *tracked) Ack()  { t.once.Do(func() { t.Delivery.Ack(); t.release() }) }
func (t *tracked) Nack() { t.once.Do(func() { t.Delivery.Nack(); t.release() }) }

func (t *tracked) release() {
	<-t.r.inflight
	t.r.s.tel.inflightAdd(t.r.hctx, -1, t.Message().Topic)
}

// nackAs nacks d without running the handler and records why.
func (r *run) nackAs(ctx context.Context, d Delivery, outcome string) {
	d.Nack()
	r.s.tel.settled(ctx, d.Message().Topic, outcome)
}

func (r *run) receive(ctx context.Context, d Delivery) {
	m := d.Message()
	r.s.tel.consumedOne(ctx, m.Topic)
	if r.draining() {
		r.nackAs(ctx, d, outcomeShutdown)
		return
	}
	seq := r.seq.Add(1)
	if !r.fences.peek(m, seq) {
		r.nackAs(ctx, d, outcomeOrdering)
		return
	}
	select {
	case r.inflight <- struct{}{}:
	case <-ctx.Done():
		r.nackAs(ctx, d, outcomeShutdown)
		return
	case <-r.drainCh:
		r.nackAs(ctx, d, outcomeShutdown)
		return
	}
	r.s.tel.inflightAdd(ctx, 1, m.Topic)
	td := &tracked{Delivery: d, r: r, seq: seq}
	if r.s.batch != nil {
		r.dispatchBatch(td)
		return
	}
	if m.Key == "" {
		r.wg.Go(func() { //nolint:contextcheck // handlers run on r.hctx, which outlives the receive ctx for graceful shutdown
			if !r.acquireWorker() {
				r.nackAs(r.hctx, td, outcomeShutdown)
				return
			}
			defer r.releaseWorker()
			r.processOne(td)
		})
		return
	}
	r.mu.Lock()
	q, ok := r.keys[m.Key]
	if !ok {
		q = &keyQueue{}
		r.keys[m.Key] = q
	}
	q.items = append(q.items, td)
	r.mu.Unlock()
	if !ok {
		r.wg.Go(func() { r.drainKey(m.Key, q) }) //nolint:contextcheck // handlers run on r.hctx, which outlives the receive ctx for graceful shutdown
	}
}

func (r *run) acquireWorker() bool {
	if r.draining() {
		return false
	}
	select {
	case r.workers <- struct{}{}:
		return true
	case <-r.drainCh:
		return false
	}
}

func (r *run) releaseWorker() { <-r.workers }

// drainKey handles the queue of one key sequentially until it is empty.
func (r *run) drainKey(key string, q *keyQueue) {
	for {
		r.mu.Lock()
		if len(q.items) == 0 {
			delete(r.keys, key)
			r.mu.Unlock()
			return
		}
		d := q.items[0]
		q.items[0] = nil
		q.items = q.items[1:]
		stale := q.stale > 0
		if stale {
			q.stale--
		}
		r.mu.Unlock()

		if stale || !r.fences.admit(d.Message(), seqOf(d)) {
			r.nackAs(r.hctx, d, outcomeOrdering)
			continue
		}
		if !r.acquireWorker() {
			r.nackAs(r.hctx, d, outcomeShutdown)
			continue
		}
		res := r.processOne(d)
		r.releaseWorker()
		if res.blocked && !res.fenced {
			r.mu.Lock()
			q.stale = len(q.items)
			r.mu.Unlock()
		}
	}
}

// errInterrupted marks attempts stopped by shutdown during a backoff.
var errInterrupted = errors.New("pubsub: interrupted by shutdown")

// processOne handles one message and settles it.
func (r *run) processOne(d Delivery) settleResult {
	m := d.Message()
	ctx, span := r.s.tel.startProcess(r.hctx, []*Message{m})
	start := time.Now()
	err := r.attempts(ctx, m.Topic, func(ctx context.Context) error { return r.callHandler(ctx, m) })
	res := r.settle(ctx, d, err)
	r.s.tel.endProcess(ctx, span, m.Topic, start, err)
	return res
}

// settleResult reports how a message was settled.
type settleResult struct {
	acked bool
	// blocked: the message was nacked and will be redelivered, so later
	// messages of its key must not run before it.
	blocked bool
	// fenced: a key fence now enforces that (see keyFences).
	fenced bool
}

// settle acks or nacks d given the final error.
func (r *run) settle(ctx context.Context, d Delivery, err error) settleResult {
	if err == nil {
		d.Ack()
		r.s.tel.settled(ctx, d.Message().Topic, outcomeAck)
		return settleResult{acked: true}
	}
	m := d.Message()
	// A handler cut short by the shutdown timeout did not really fail: never
	// dead-letter it, nack it so the broker redelivers it.
	interrupted := errors.Is(err, errInterrupted) || r.hctx.Err() != nil
	exhausted := !interrupted && r.s.o.maxDeliveryAttempts > 0 && m.DeliveryAttempt >= r.s.o.maxDeliveryAttempts
	if !interrupted && (IsPermanent(err) || exhausted) {
		return r.deadLetter(ctx, d, err)
	}
	r.s.nackLog.nacked(ctx,
		log.String("messaging.destination.name", m.Topic),
		log.String("messaging.message.id", m.ID),
		log.Int("delivery_attempt", m.DeliveryAttempt),
		log.Err(err))
	return r.nack(ctx, d)
}

// nack fences the key when that keeps it in order (see KeyFenceMode) and
// nacks d. The fence is set before the nack so a fast redelivery cannot
// race past it.
func (r *run) nack(ctx context.Context, d Delivery) settleResult {
	m := d.Message()
	res := settleResult{}
	if m.Key != "" {
		ttl, final := r.fenceTTL(m)
		res.blocked = !final
		if ttl > 0 {
			r.fences.block(m.Key, m.ID, ttl)
			res.fenced = true
		}
	}
	d.Nack()
	r.s.tel.settled(ctx, m.Topic, outcomeNack)
	return res
}

// fenceTTL returns how long to fence m's key after its final nack (0: no
// fence) and whether that nack is the broker's last delivery of m.
func (r *run) fenceTTL(m *Message) (time.Duration, bool) {
	var rd Redelivery
	if r.advisor != nil {
		rd = r.advisor.Redelivery()
	}
	if rd.MaxDeliveryAttempts > 0 && m.DeliveryAttempt >= rd.MaxDeliveryAttempts {
		return 0, true
	}
	switch r.s.o.keyFence {
	case KeyFenceOff:
		return 0, false
	case KeyFenceOn:
		return r.s.o.keyFenceTTL, false
	}
	if r.advisor == nil {
		return r.s.o.keyFenceTTL, false
	}
	if !rd.Fence {
		return 0, false
	}
	if rd.FenceTTL > 0 {
		return rd.FenceTTL, false
	}
	return r.s.o.keyFenceTTL, false
}

func (r *run) deadLetter(ctx context.Context, d Delivery, cause error) settleResult {
	m := d.Message()
	if r.s.o.deadLetter == nil {
		r.s.log.LogAttrs(ctx, log.LevelError, "pubsub: dropping permanently failed message",
			log.String("messaging.destination.name", m.Topic),
			log.String("messaging.message.id", m.ID),
			log.Int("delivery_attempt", m.DeliveryAttempt),
			log.Err(cause))
		d.Ack()
		r.s.tel.settled(ctx, m.Topic, outcomeDeadLetter)
		return settleResult{acked: true}
	}
	dctx := ctx
	if r.s.o.handlerTimeout > 0 {
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(ctx, r.s.o.handlerTimeout)
		defer cancel()
	}
	if err := r.safe(dctx, func() error { return r.s.o.deadLetter(dctx, m, cause) }); err != nil {
		r.s.log.LogAttrs(ctx, log.LevelError, "pubsub: dead-letter hook failed, nacking",
			log.String("messaging.destination.name", m.Topic),
			log.String("messaging.message.id", m.ID),
			log.Err(err), log.Any("cause", cause))
		return r.nack(ctx, d)
	}
	d.Ack()
	r.s.tel.settled(ctx, m.Topic, outcomeDeadLetter)
	return settleResult{acked: true}
}

// attempts runs f up to 1+maxRetries times with backoff. It stops early on
// success or a permanent error and returns errInterrupted (joined with the
// last error) when shutdown interrupts a backoff.
func (r *run) attempts(ctx context.Context, topic string, f func(context.Context) error) error {
	var err error
	for attempt := 0; attempt <= r.s.o.maxRetries; attempt++ {
		if attempt > 0 {
			if !r.backoff(attempt) {
				return errors.Join(errInterrupted, err)
			}
			r.s.tel.retried(ctx, topic)
		}
		err = r.withTimeout(ctx, f)
		if err == nil || IsPermanent(err) {
			return err
		}
	}
	return err
}

func (r *run) withTimeout(ctx context.Context, f func(context.Context) error) error {
	if r.s.o.handlerTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.s.o.handlerTimeout)
		defer cancel()
	}
	return f(ctx)
}

// backoff waits before retry number attempt (1-based). It returns false if
// shutdown started.
func (r *run) backoff(attempt int) bool {
	if r.draining() {
		return false
	}
	d := timex.Backoff(r.s.o.retryBase, r.s.o.retryMax, attempt)
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-r.drainCh:
		return false
	}
}

func (r *run) callHandler(ctx context.Context, m *Message) error {
	return r.safe(ctx, func() error { return r.s.handler(ctx, m) })
}

// safe runs f and converts a panic into an error.
func (r *run) safe(ctx context.Context, f func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("pubsub: handler panic: %v", p)
			r.s.log.ErrorContext(ctx, "pubsub: handler panic", log.Any("panic", p), log.String("stack", string(debug.Stack())))
		}
	}()
	return f()
}
