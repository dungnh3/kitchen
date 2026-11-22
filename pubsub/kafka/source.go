package kafka

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/timex"
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
// of revoked partitions to be settled before committing (default 30s).
//
// Keep it below the group's rebalance timeout (kgo.RebalanceTimeout, default
// 60s), and at or above the Subscriber's [pubsub.WithShutdownTimeout]
// (default 30s): on shutdown, Receive waits up to RevokeTimeout for the
// handlers the engine is still draining, and records settled after that are
// not committed, so they are redelivered to the next owner.
func WithRevokeTimeout(d time.Duration) SourceOption { return func(s *Source) { s.revokeTimeout = d } }

// WithPollRecords sets the maximum records per poll (default 1000).
func WithPollRecords(n int) SourceOption { return func(s *Source) { s.pollRecords = n } }

// WithMaxPendingPerPartition bounds the records of one partition that are
// received but not yet committable (default 10000). When reached, fetching
// that partition is paused until half of them are settled, so a stuck record
// at the head of a partition cannot make memory grow without limit; the other
// partitions keep flowing.
func WithMaxPendingPerPartition(n int) SourceOption { return func(s *Source) { s.maxPending = n } }

// WithLogger sets the logger (nil means log.Default()).
func WithLogger(l *log.Logger) SourceOption { return func(s *Source) { s.log = l } }

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
	maxPending    int
	log           *log.Logger
}

var (
	_ pubsub.Source             = (*Source)(nil)
	_ pubsub.RedeliveryAdvisor  = (*Source)(nil)
	_ pubsub.ConsumerIdentifier = (*Source)(nil)
)

// NewSource returns a Source for group consuming topics.
func NewSource(group string, topics []string, opts ...SourceOption) (*Source, error) {
	if group == "" || len(topics) == 0 {
		return nil, errors.New("kafka: empty group or topics")
	}
	s := &Source{
		group: group, topics: slices.Clone(topics),
		commitEvery: time.Second, backoffBase: time.Second, backoffMax: time.Minute,
		revokeTimeout: 30 * time.Second, pollRecords: 1000, maxPending: 10000,
	}
	for _, o := range opts {
		o(s)
	}
	if s.commitEvery <= 0 || s.backoffBase <= 0 || s.backoffMax < s.backoffBase || s.revokeTimeout <= 0 || s.pollRecords < 1 || s.maxPending < 1 {
		return nil, errors.New("kafka: invalid source options")
	}
	s.log = log.OrDefault(s.log)
	return s, nil
}

// System implements [pubsub.Source].
func (s *Source) System() string { return System }

// Consumer implements [pubsub.ConsumerIdentifier]: the consumer group.
func (s *Source) Consumer() pubsub.Consumer { return pubsub.Consumer{Group: s.group} }

// Redelivery implements [pubsub.RedeliveryAdvisor]. A nacked record is
// redelivered in process by this consumer, with the same ID and before later
// records of its key, so the key fence keeps the key in order.
func (s *Source) Redelivery() pubsub.Redelivery { return pubsub.Redelivery{Fence: true} }

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
	gen      uint64 // assignment that created this state
	revoked  bool   // no more redeliveries; acks still count
	gone     bool   // forgotten; settlements ignored
	paused   bool   // fetching paused because pending reached maxPending
}

type receiver struct {
	s       *Source
	cl      *kgo.Client
	fn      func(context.Context, pubsub.Delivery)
	ctx     context.Context
	stopCh  chan struct{}
	settled chan struct{}
	wg      sync.WaitGroup

	// commitSync is cl.CommitOffsetsSync; tests replace it.
	commitSync func(context.Context, map[string]map[int32]kgo.EpochOffset, func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error))
	// commitSem (capacity 1) is held from the offset snapshot until the
	// broker answers, so an older snapshot never lands after a newer one.
	commitSem chan struct{}

	mu      sync.Mutex
	stopped bool
	drained bool   // Receive finished waiting for the engine on shutdown
	gen     uint64 // bumped on every assignment
	// parts holds the partitions this member owns. State is created only by
	// onAssigned, never by a record: records of a partition that is not
	// owned (revoked, lost, or reassigned after the poll) are dropped.
	parts map[topicPart]*partState
}

func newReceiver(ctx context.Context, s *Source, fn func(context.Context, pubsub.Delivery)) *receiver {
	return &receiver{
		s: s, fn: fn, ctx: ctx,
		stopCh: make(chan struct{}), settled: make(chan struct{}, 1), commitSem: make(chan struct{}, 1),
		parts: map[topicPart]*partState{},
	}
}

// all matches every partition.
func all(topicPart) bool { return true }

// in matches the partitions in tps.
func in(tps []topicPart) func(topicPart) bool {
	return func(tp topicPart) bool { return slices.Contains(tps, tp) }
}

// Receive implements [pubsub.Source].
func (s *Source) Receive(ctx context.Context, fn func(context.Context, pubsub.Delivery)) error {
	r := newReceiver(ctx, s, fn)
	opts := append(slices.Clone(s.clientOpts),
		kgo.ConsumerGroup(s.group),
		kgo.ConsumeTopics(s.topics...),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.OnPartitionsAssigned(r.onAssigned),
		kgo.OnPartitionsRevoked(r.onRevoked),
		kgo.OnPartitionsLost(r.onLost),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	r.cl = cl
	r.commitSync = cl.CommitOffsetsSync

	commitDone := make(chan struct{})
	go func() { //nolint:contextcheck // commits must complete after ctx is cancelled (shutdown, rebalance)
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
			s.log.WarnContext(ctx, "kafka: fetch error", log.String("topic", topic), log.Int("partition", int(part)), log.Err(err))
		})
		// Records polled now belong to the assignment current at this point:
		// franz-go calls onAssigned before fetching a partition, so a state
		// created after this read is a later assignment of the partition and
		// these records are stale for it.
		r.mu.Lock()
		gen := r.gen
		r.mu.Unlock()
		fetches.EachRecord(func(kr *kgo.Record) { r.onRecord(kr, gen) })
	}

	// Shutdown: stop redeliveries, wait for the engine to settle what it
	// holds, commit, then leave the group.
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	close(r.stopCh)
	r.waitSettled(all, s.revokeTimeout)
	r.mu.Lock()
	r.drained = true
	r.mu.Unlock()
	r.wg.Wait()
	<-commitDone
	r.commit() //nolint:contextcheck // commits must complete after ctx is cancelled (shutdown, rebalance)
	cl.Close()
	return nil
}

// onAssigned creates the state of newly assigned partitions.
func (r *receiver) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gen++
	for _, tp := range flatten(assigned) {
		if old := r.parts[tp]; old != nil {
			old.revoked, old.gone = true, true
		}
		r.parts[tp] = &partState{blocked: map[string][]*rec{}, gen: r.gen}
	}
}

// onRecord runs on the poll goroutine. pollGen is r.gen read after the poll
// that returned kr.
func (r *receiver) onRecord(kr *kgo.Record, pollGen uint64) {
	tp := topicPart{kr.Topic, kr.Partition}
	r.mu.Lock()
	ps := r.parts[tp]
	// Records polled before a revoke are still in hand when the poll
	// goroutine was blocked in fn. Their partition now belongs to another
	// member (or to a later assignment here, which fetches again from the
	// committed offset): drop them, neither delivered nor committed.
	if ps == nil || ps.revoked || ps.gone || ps.gen > pollGen {
		r.mu.Unlock()
		return
	}
	rc := &rec{r: kr, ps: ps}
	ps.pending = append(ps.pending, rc)
	pause := !ps.paused && len(ps.pending) >= r.s.maxPending
	if pause {
		ps.paused = true
	}
	var d *delivery
	if k := string(kr.Key); k != "" && len(ps.blocked[k]) > 0 {
		ps.blocked[k] = append(ps.blocked[k], rc) // offsets increase
	} else {
		d = r.markOutLocked(rc)
	}
	r.mu.Unlock()
	if pause {
		// Buffered records of a paused partition are dropped by the client and
		// fetched again after resume.
		r.s.log.Warn("kafka: partition paused, too many unsettled records",
			log.String("topic", tp.topic), log.Int("partition", int(tp.part)), log.Int("pending", r.s.maxPending))
		r.cl.PauseFetchPartitions(map[string][]int32{tp.topic: {tp.part}})
	}
	if d != nil {
		r.fn(r.ctx, d)
	}
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
	if resume := r.settleLocked(rc, ok); resume != nil {
		r.cl.ResumeFetchPartitions(resume)
	}
}

// settleLocked settles rc and returns the partition to resume, if any.
func (r *receiver) settleLocked(rc *rec, ok bool) (resume map[string][]int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.notifySettled()
	ps := rc.ps
	rc.out = false
	ps.out--
	if ps.gone {
		return resume
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
		if ps.paused && len(ps.pending) <= r.s.maxPending/2 {
			ps.paused = false
			resume = map[string][]int32{rc.r.Topic: {rc.r.Partition}}
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
		return resume
	}
	if r.stopped || ps.revoked {
		return nil // not committed; redelivered by the next owner
	}
	delay := r.backoff(rc.attempts)
	if key == "" {
		r.scheduleLocked(ps, "", rc, delay)
		return resume
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
	return resume
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
	return timex.Backoff(r.s.backoffBase, r.s.backoffMax, attempt)
}

func (r *receiver) notifySettled() {
	select {
	case r.settled <- struct{}{}:
	default:
	}
}

// waitSettled waits until the matching partitions have no records held by
// the engine, or timeout.
func (r *receiver) waitSettled(match func(topicPart) bool, timeout time.Duration) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		r.mu.Lock()
		busy := false
		for tp, ps := range r.parts {
			if ps.out > 0 && match(tp) {
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
	if len(tps) == 0 {
		// End of a cooperative session: nothing is taken away. Commit what
		// is settled and let the rebalance go on.
		r.commit() //nolint:contextcheck // commits must complete after ctx is cancelled (shutdown, rebalance)
		return
	}
	r.mu.Lock()
	drained := r.drained
	for _, tp := range tps {
		if ps := r.parts[tp]; ps != nil {
			ps.revoked = true
			clear(ps.blocked)
		}
	}
	r.mu.Unlock()
	if !drained {
		// On shutdown Receive has already waited for the engine to drain;
		// the revoke from Close only commits what settled since.
		r.waitSettled(in(tps), r.s.revokeTimeout)
	}
	r.commitParts(in(tps)) //nolint:contextcheck // commits must complete after ctx is cancelled (shutdown, rebalance)
	resume := map[string][]int32{}
	r.mu.Lock()
	for _, tp := range tps {
		if ps := r.parts[tp]; ps != nil {
			if ps.paused {
				resume[tp.topic] = append(resume[tp.topic], tp.part)
			}
			ps.gone = true
			delete(r.parts, tp)
		}
	}
	r.mu.Unlock()
	r.resumeAll(resume)
}

// resumeAll resumes partitions paused by this receiver. Pauses persist across
// rebalances, so a partition must not stay paused once it is forgotten.
func (r *receiver) resumeAll(parts map[string][]int32) {
	if len(parts) > 0 {
		r.cl.ResumeFetchPartitions(parts)
	}
}

func (r *receiver) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	resume := map[string][]int32{}
	r.mu.Lock()
	for _, tp := range flatten(lost) {
		if ps := r.parts[tp]; ps != nil {
			if ps.paused {
				resume[tp.topic] = append(resume[tp.topic], tp.part)
			}
			ps.revoked, ps.gone = true, true
			delete(r.parts, tp)
		}
	}
	r.mu.Unlock()
	r.resumeAll(resume)
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

func (r *receiver) commit() { r.commitParts(all) }

// commitParts synchronously commits the settled prefix of the matching
// partitions. Commits are serialized from snapshot to response, so a commit
// never overwrites a newer one.
func (r *receiver) commitParts(match func(topicPart) bool) {
	r.commitSem <- struct{}{}
	defer func() { <-r.commitSem }()
	offsets := map[string]map[int32]kgo.EpochOffset{}
	states := map[topicPart]*partState{}
	r.mu.Lock()
	for tp, ps := range r.parts {
		if !ps.dirty || ps.gone || !match(tp) {
			continue
		}
		if offsets[tp.topic] == nil {
			offsets[tp.topic] = map[int32]kgo.EpochOffset{}
		}
		offsets[tp.topic][tp.part] = ps.commitTo
		states[tp] = ps
		ps.dirty = false
	}
	r.mu.Unlock()
	if len(offsets) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.s.revokeTimeout)
	defer cancel()
	r.commitSync(ctx, offsets, func(_ *kgo.Client, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		// A request error fails every partition; otherwise each partition
		// reports its own error code, which must be checked too.
		var failed []topicPart
		switch {
		case err != nil:
			r.s.log.Warn("kafka: commit failed", log.Err(err))
			for t, parts := range offsets {
				for p := range parts {
					failed = append(failed, topicPart{t, p})
				}
			}
		case resp != nil:
			// Newer protocol versions identify topics by ID only.
			names := map[[16]byte]string{}
			if req != nil {
				for _, t := range req.Topics {
					names[t.TopicID] = t.Topic
				}
			}
			for _, t := range resp.Topics {
				topic := t.Topic
				if topic == "" {
					topic = names[t.TopicID]
				}
				for _, p := range t.Partitions {
					if perr := kerr.ErrorForCode(p.ErrorCode); perr != nil {
						r.s.log.Warn("kafka: commit failed for partition",
							log.String("topic", topic), log.Int("partition", int(p.Partition)), log.Err(perr))
						failed = append(failed, topicPart{topic, p.Partition})
					}
				}
			}
		}
		if len(failed) == 0 {
			return
		}
		r.mu.Lock()
		for _, tp := range failed {
			if ps := states[tp]; ps != nil && !ps.gone {
				ps.dirty = true
			}
		}
		r.mu.Unlock()
	})
}
