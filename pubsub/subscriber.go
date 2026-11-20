package pubsub

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Subscriber runs a handler for every message of a [Source]. Create it with
// [NewSubscriber] or [NewBatchSubscriber] and call Run once.
type Subscriber struct {
	src     Source
	handler Handler
	batch   BatchHandler
	o       options
	log     *slog.Logger
	tel     *Telemetry
	started atomic.Bool
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
	case o.handlerTimeout < 0 || o.shutdownTimeout < 0:
		return nil, errors.New("pubsub: negative timeout")
	}
	log := o.logger
	if log == nil {
		log = slog.Default()
	}
	return &Subscriber{
		src:     src,
		handler: h,
		batch:   bh,
		o:       o,
		log:     log.With("messaging.system", src.System()),
		tel:     NewTelemetry(src.System(), o.telemetry...),
	}, nil
}

// Run receives and handles messages until ctx is cancelled or the source fails.
//
// On return every received message has been acked or nacked and every
// goroutine started by Run has exited. When ctx is cancelled, Run stops
// receiving, nacks messages that have not started, and gives running handlers
// up to WithShutdownTimeout to finish before cancelling their contexts. Run
// returns nil after a cancellation and the source error otherwise. Run may be
// called only once.
func (s *Subscriber) Run(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrClosed
	}
	r := &run{
		s:        s,
		inflight: make(chan struct{}, s.o.maxInFlight),
		workers:  make(chan struct{}, s.o.concurrency),
		drainCh:  make(chan struct{}),
		keys:     make(map[string]*keyQueue),
	}
	return r.run(ctx)
}

type run struct {
	s        *Subscriber
	inflight chan struct{}
	workers  chan struct{}
	drainCh  chan struct{}
	hctx     context.Context
	wg       sync.WaitGroup

	mu   sync.Mutex
	keys map[string]*keyQueue

	shards []*shard
	rr     atomic.Uint64
	seed   maphash.Seed
}

type keyQueue struct {
	items  []Delivery
	failed bool
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
			sh := &shard{signal: make(chan struct{}, 1), failed: map[string]bool{}}
			r.shards[i] = sh
			r.wg.Go(func() { r.shardLoop(sh) })
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
	once sync.Once
}

func (t *tracked) Ack()  { t.once.Do(func() { t.Delivery.Ack(); <-t.r.inflight }) }
func (t *tracked) Nack() { t.once.Do(func() { t.Delivery.Nack(); <-t.r.inflight }) }

func (r *run) receive(ctx context.Context, d Delivery) {
	m := d.Message()
	r.s.tel.consumedOne(ctx, m.Topic)
	if r.draining() {
		d.Nack()
		return
	}
	select {
	case r.inflight <- struct{}{}:
	case <-ctx.Done():
		d.Nack()
		return
	case <-r.drainCh:
		d.Nack()
		return
	}
	td := &tracked{Delivery: d, r: r}
	if r.s.batch != nil {
		r.dispatchBatch(td)
		return
	}
	if m.Key == "" {
		r.wg.Go(func() {
			if !r.acquireWorker() {
				td.Nack()
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
		r.wg.Go(func() { r.drainKey(m.Key, q) })
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
		failed := q.failed
		r.mu.Unlock()

		if failed || !r.acquireWorker() {
			d.Nack()
			continue
		}
		acked := r.processOne(d)
		r.releaseWorker()
		if !acked {
			r.mu.Lock()
			q.failed = true
			r.mu.Unlock()
		}
	}
}

// errInterrupted marks attempts stopped by shutdown during a backoff.
var errInterrupted = errors.New("pubsub: interrupted by shutdown")

// processOne handles one message and settles it. It reports whether it was acked.
func (r *run) processOne(d Delivery) bool {
	m := d.Message()
	ctx, span := r.s.tel.startProcess(r.hctx, []*Message{m})
	start := time.Now()
	err := r.attempts(ctx, func(ctx context.Context) error { return r.callHandler(ctx, m) })
	acked := r.settle(ctx, d, err)
	r.s.tel.endProcess(ctx, span, m.Topic, start, err)
	return acked
}

// settle acks or nacks d given the final error and reports whether it acked.
func (r *run) settle(ctx context.Context, d Delivery, err error) bool {
	if err == nil {
		d.Ack()
		return true
	}
	m := d.Message()
	interrupted := errors.Is(err, errInterrupted)
	exhausted := !interrupted && r.s.o.maxDeliveryAttempts > 0 && m.DeliveryAttempt >= r.s.o.maxDeliveryAttempts
	if !interrupted && (IsPermanent(err) || exhausted) {
		return r.deadLetter(ctx, d, err)
	}
	r.s.log.LogAttrs(ctx, slog.LevelWarn, "pubsub: message nacked",
		slog.String("messaging.destination.name", m.Topic),
		slog.String("messaging.message.id", m.ID),
		slog.Int("delivery_attempt", m.DeliveryAttempt),
		slog.Any("error", err))
	d.Nack()
	return false
}

func (r *run) deadLetter(ctx context.Context, d Delivery, cause error) bool {
	m := d.Message()
	if r.s.o.deadLetter == nil {
		r.s.log.LogAttrs(ctx, slog.LevelError, "pubsub: dropping permanently failed message",
			slog.String("messaging.destination.name", m.Topic),
			slog.String("messaging.message.id", m.ID),
			slog.Int("delivery_attempt", m.DeliveryAttempt),
			slog.Any("error", cause))
		d.Ack()
		return true
	}
	dctx := ctx
	if r.s.o.handlerTimeout > 0 {
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(ctx, r.s.o.handlerTimeout)
		defer cancel()
	}
	if err := r.safe(func() error { return r.s.o.deadLetter(dctx, m, cause) }); err != nil {
		r.s.log.LogAttrs(ctx, slog.LevelError, "pubsub: dead-letter hook failed, nacking",
			slog.String("messaging.destination.name", m.Topic),
			slog.String("messaging.message.id", m.ID),
			slog.Any("error", err), slog.Any("cause", cause))
		d.Nack()
		return false
	}
	d.Ack()
	return true
}

// attempts runs f up to 1+maxRetries times with backoff. It stops early on
// success or a permanent error and returns errInterrupted (joined with the
// last error) when shutdown interrupts a backoff.
func (r *run) attempts(ctx context.Context, f func(context.Context) error) error {
	var err error
	for attempt := 0; attempt <= r.s.o.maxRetries; attempt++ {
		if attempt > 0 {
			if !r.backoff(attempt) {
				return errors.Join(errInterrupted, err)
			}
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
	d := backoffDelay(r.s.o.retryBase, r.s.o.retryMax, attempt)
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

// backoffDelay returns base·2^(attempt-1) capped at maxDelay, with "equal
// jitter" (between half and the full value).
func backoffDelay(base, maxDelay time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	if maxDelay < base {
		maxDelay = base
	}
	d := base
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	d = min(d, maxDelay)
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half+1)
}

func (r *run) callHandler(ctx context.Context, m *Message) error {
	return r.safe(func() error { return r.s.handler(ctx, m) })
}

// safe runs f and converts a panic into an error.
func (r *run) safe(f func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("pubsub: handler panic: %v", p)
			r.s.log.Error("pubsub: handler panic", slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
		}
	}()
	return f()
}
