package kafka

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/dungnh3/kitchen/pubsub"
)

// SourceOption configures a Source.
type SourceOption func(*Source)

// WithClientOptions adds franz-go options (seed brokers, TLS, SASL, ...).
// Group, topic, commit and rebalance-callback options are set by the Source
// and must not be passed.
func WithClientOptions(opts ...kgo.Opt) SourceOption {
	return func(s *Source) { s.clientOpts = append(s.clientOpts, opts...) }
}

// WithCommitInterval sets how often settled offsets are committed (default 1s).
func WithCommitInterval(d time.Duration) SourceOption { return func(s *Source) { s.commitEvery = d } }

// WithRedeliveryBackoff sets the delay before a nacked record is redelivered:
// base·2^(attempt-1) capped at maxDelay, with jitter (default 1s, 1m).
func WithRedeliveryBackoff(base, maxDelay time.Duration) SourceOption {
	return func(s *Source) { s.backoffBase, s.backoffMax = base, maxDelay }
}

// WithRevokeTimeout bounds how long a rebalance or shutdown waits for records
// of revoked partitions to be settled before committing (default 30s). Keep
// it below the group's rebalance timeout.
func WithRevokeTimeout(d time.Duration) SourceOption { return func(s *Source) { s.revokeTimeout = d } }

// WithPollRecords sets the maximum records per poll (default 1000).
func WithPollRecords(n int) SourceOption { return func(s *Source) { s.pollRecords = n } }

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) SourceOption { return func(s *Source) { s.log = l } }

// Source consumes topics as a member of a consumer group. Each Receive call
// creates its own franz-go client and leaves the group when it returns.
type Source struct {
	group         string
	topics        []string
	clientOpts    []kgo.Opt
	commitEvery   time.Duration
	backoffBase   time.Duration
	backoffMax    time.Duration
	revokeTimeout time.Duration
	pollRecords   int
	log           *slog.Logger
}

var _ pubsub.Source = (*Source)(nil)

// NewSource returns a Source for group consuming topics.
func NewSource(group string, topics []string, opts ...SourceOption) (*Source, error) {
	if group == "" || len(topics) == 0 {
		return nil, errors.New("kafka: empty group or topics")
	}
	s := &Source{
		group: group, topics: slices.Clone(topics),
		commitEvery: time.Second, backoffBase: time.Second, backoffMax: time.Minute,
		revokeTimeout: 30 * time.Second, pollRecords: 1000,
	}
	for _, o := range opts {
		o(s)
	}
	if s.commitEvery <= 0 || s.backoffBase <= 0 || s.backoffMax < s.backoffBase || s.revokeTimeout <= 0 || s.pollRecords < 1 {
		return nil, errors.New("kafka: invalid source options")
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	return s, nil
}

// System implements [pubsub.Source].
func (s *Source) System() string { return System }

type topicPart struct {
	topic string
	part  int32
}

type rec struct {
	r        *kgo.Record
	ps       *partState
	attempts int
	done     bool
	out      bool // handed to fn, not settled
}

type partState struct {
	pending  []*rec            // unsettled records in offset order
	blocked  map[string][]*rec // keys waiting for an in-order redelivery
	commitTo kgo.EpochOffset
	dirty    bool
	out      int
	revoked  bool // no more redeliveries; acks still count
	gone     bool // forgotten; settlements ignored
}

type receiver struct {
	s       *Source
	cl      *kgo.Client
	fn      func(context.Context, pubsub.Delivery)
	ctx     context.Context
	stopCh  chan struct{}
	settled chan struct{}
	wg      sync.WaitGroup

	mu      sync.Mutex
	stopped bool
	parts   map[topicPart]*partState
}

// Receive implements [pubsub.Source].
func (s *Source) Receive(ctx context.Context, fn func(context.Context, pubsub.Delivery)) error {
	r := &receiver{
		s: s, fn: fn, ctx: ctx,
		stopCh: make(chan struct{}), settled: make(chan struct{}, 1),
		parts: map[topicPart]*partState{},
	}
	opts := append(slices.Clone(s.clientOpts),
		kgo.ConsumerGroup(s.group),
		kgo.ConsumeTopics(s.topics...),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.OnPartitionsRevoked(r.onRevoked),
		kgo.OnPartitionsLost(r.onLost),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	r.cl = cl

	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		r.commitLoop()
	}()

	for {
		fetches := cl.PollRecords(ctx, s.pollRecords)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(topic string, part int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			s.log.Warn("kafka: fetch error", slog.String("topic", topic), slog.Int("partition", int(part)), slog.Any("error", err))
		})
		fetches.EachRecord(r.onRecord)
	}

	// Shutdown: stop redeliveries, wait for the engine to settle what it
	// holds, commit, then leave the group.
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	close(r.stopCh)
	r.waitSettled(nil, s.revokeTimeout)
	r.wg.Wait()
	<-commitDone
	r.commit()
	cl.Close()
	return nil
}

func (r *receiver) state(tp topicPart) *partState {
	ps := r.parts[tp]
	if ps == nil {
		ps = &partState{blocked: map[string][]*rec{}}
		r.parts[tp] = ps
	}
	return ps
}

// onRecord runs on the poll goroutine.
func (r *receiver) onRecord(kr *kgo.Record) {
	r.mu.Lock()
	ps := r.state(topicPart{kr.Topic, kr.Partition})
	if ps.revoked {
		r.mu.Unlock()
		return
	}
	rc := &rec{r: kr, ps: ps}
	ps.pending = append(ps.pending, rc)
	if k := string(kr.Key); k != "" && len(ps.blocked[k]) > 0 {
		ps.blocked[k] = append(ps.blocked[k], rc) // offsets increase
		r.mu.Unlock()
		return
	}
	d := r.markOutLocked(rc)
	r.mu.Unlock()
	r.fn(r.ctx, d)
}

func (r *receiver) markOutLocked(rc *rec) *delivery {
	rc.attempts++
	rc.out = true
	rc.ps.out++
	kr := rc.r
	attrs := make(map[string]string, len(kr.Headers))
	for _, h := range kr.Headers {
		attrs[h.Key] = string(h.Value)
	}
	return &delivery{rv: r, rc: rc, msg: &pubsub.Message{
		ID: recordID(kr), Topic: kr.Topic, Key: string(kr.Key), Data: kr.Value,
		Attributes: attrs, PublishTime: kr.Timestamp, DeliveryAttempt: rc.attempts,
	}}
}

type delivery struct {
	rv   *receiver
	rc   *rec
	msg  *pubsub.Message
	once sync.Once
}

func (d *delivery) Message() *pubsub.Message { return d.msg }
func (d *delivery) Ack()                     { d.once.Do(func() { d.rv.settle(d.rc, true) }) }
func (d *delivery) Nack()                    { d.once.Do(func() { d.rv.settle(d.rc, false) }) }

func (r *receiver) settle(rc *rec, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.notifySettled()
	ps := rc.ps
	rc.out = false
	ps.out--
	if ps.gone {
		return
	}
	key := string(rc.r.Key)
	if ok {
		rc.done = true
		for len(ps.pending) > 0 && ps.pending[0].done {
			h := ps.pending[0]
			ps.commitTo = kgo.EpochOffset{Epoch: h.r.LeaderEpoch, Offset: h.r.Offset + 1}
			ps.dirty = true
			ps.pending[0] = nil
			ps.pending = ps.pending[1:]
		}
		if key != "" {
			if q := ps.blocked[key]; len(q) > 0 && q[0] == rc {
				q = q[1:]
				if len(q) == 0 {
					delete(ps.blocked, key)
				} else {
					ps.blocked[key] = q
					r.scheduleLocked(ps, key, nil, 0)
				}
			}
		}
		return
	}
	if r.stopped || ps.revoked {
		return // not committed; redelivered by the next owner
	}
	delay := r.backoff(rc.attempts)
	if key == "" {
		r.scheduleLocked(ps, "", rc, delay)
		return
	}
	q := ps.blocked[key]
	if i := slices.Index(q, rc); i < 0 {
		// Insert in offset order: a later record may have been nacked first.
		i, _ = slices.BinarySearchFunc(q, rc, func(a, b *rec) int { return cmp.Compare(a.r.Offset, b.r.Offset) })
		q = slices.Insert(q, i, rc)
		ps.blocked[key] = q
	}
	if q[0] == rc {
		r.scheduleLocked(ps, key, nil, delay)
	}
}

// scheduleLocked redelivers rc (unkeyed) or the head of key's blocked queue
// after delay.
func (r *receiver) scheduleLocked(ps *partState, key string, rc *rec, delay time.Duration) {
	r.wg.Go(func() {
		if delay > 0 {
			t := time.NewTimer(delay)
			defer t.Stop()
			select {
			case <-t.C:
			case <-r.stopCh:
				return
			}
		}
		r.mu.Lock()
		if r.stopped || ps.revoked || ps.gone {
			r.mu.Unlock()
			return
		}
		target := rc
		if key != "" {
			q := ps.blocked[key]
			if len(q) == 0 || q[0].out || q[0].done {
				r.mu.Unlock()
				return
			}
			target = q[0]
		}
		d := r.markOutLocked(target)
		r.mu.Unlock()
		r.fn(r.ctx, d)
	})
}

func (r *receiver) backoff(attempt int) time.Duration {
	d := r.s.backoffBase
	for i := 1; i < attempt && d < r.s.backoffMax; i++ {
		d *= 2
	}
	d = min(d, r.s.backoffMax)
	return d/2 + rand.N(d/2+1)
}

func (r *receiver) notifySettled() {
	select {
	case r.settled <- struct{}{}:
	default:
	}
}

// waitSettled waits until the given partitions (all if nil) have no records
// held by the engine, or timeout.
func (r *receiver) waitSettled(tps []topicPart, timeout time.Duration) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		r.mu.Lock()
		busy := false
		for tp, ps := range r.parts {
			if (tps == nil || slices.Contains(tps, tp)) && ps.out > 0 {
				busy = true
				break
			}
		}
		r.mu.Unlock()
		if !busy {
			return
		}
		select {
		case <-r.settled:
		case <-deadline.C:
			r.s.log.Warn("kafka: records still unsettled after revoke timeout; they will be redelivered")
			return
		}
	}
}

func flatten(m map[string][]int32) []topicPart {
	var out []topicPart
	for t, ps := range m {
		for _, p := range ps {
			out = append(out, topicPart{t, p})
		}
	}
	return out
}

func (r *receiver) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	tps := flatten(revoked)
	r.mu.Lock()
	for _, tp := range tps {
		if ps := r.parts[tp]; ps != nil {
			ps.revoked = true
			clear(ps.blocked)
		}
	}
	r.mu.Unlock()
	r.waitSettled(tps, r.s.revokeTimeout)
	r.commitParts(tps)
	r.mu.Lock()
	for _, tp := range tps {
		if ps := r.parts[tp]; ps != nil {
			ps.gone = true
			delete(r.parts, tp)
		}
	}
	r.mu.Unlock()
}

func (r *receiver) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, tp := range flatten(lost) {
		if ps := r.parts[tp]; ps != nil {
			ps.revoked, ps.gone = true, true
			delete(r.parts, tp)
		}
	}
}

func (r *receiver) commitLoop() {
	t := time.NewTicker(r.s.commitEvery)
	defer t.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-t.C:
			r.commit()
		}
	}
}

func (r *receiver) commit() { r.commitParts(nil) }

// commitParts synchronously commits the settled prefix of the given
// partitions (all if nil).
func (r *receiver) commitParts(tps []topicPart) {
	offsets := map[string]map[int32]kgo.EpochOffset{}
	r.mu.Lock()
	for tp, ps := range r.parts {
		if !ps.dirty || ps.gone || (tps != nil && !slices.Contains(tps, tp)) {
			continue
		}
		if offsets[tp.topic] == nil {
			offsets[tp.topic] = map[int32]kgo.EpochOffset{}
		}
		offsets[tp.topic][tp.part] = ps.commitTo
		ps.dirty = false
	}
	r.mu.Unlock()
	if len(offsets) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.s.revokeTimeout)
	defer cancel()
	r.cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			r.s.log.Warn("kafka: commit failed", slog.Any("error", err))
			r.mu.Lock()
			for t, parts := range offsets {
				for p := range parts {
					if ps := r.parts[topicPart{t, p}]; ps != nil {
						ps.dirty = true
					}
				}
			}
			r.mu.Unlock()
		}
	})
}
