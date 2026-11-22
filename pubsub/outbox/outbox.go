// Package outbox implements the transactional outbox pattern on top of
// [pubsub.Publisher].
//
// Application code writes messages into an outbox table in the same database
// transaction as its business data (see
// [github.com/dungnh3/kitchen/pubsub/outbox/pgx] for Postgres). A [Relay]
// claims pending rows, publishes them and deletes them, so a message is
// published if and only if its transaction committed (at least once).
//
// Per-key order: a Store hands out the records of an ordering key only as a
// run that starts at the key's oldest pending record, and only to one relay
// at a time, so several relays may run concurrently. The relay publishes a
// run in one Publish call, in order; when a record of the run fails, the
// later records of its key are released unsent (even if the broker accepted
// them) and are published again after it. Per-key order is the commit order
// of the enqueueing transactions (the pgx store serialises same-key writers).
//
// Failures: a record whose publish fails with a transient error (see
// [WithMaxAttempts]) backs off without spending an attempt; any other
// failure backs off and counts. A record whose publish fails with an error
// wrapping [pubsub.ErrPermanent], or whose counted failures reach
// [WithMaxAttempts], is marked dead with [Store.Dead]. A dead record is kept
// (for inspection and manual replay) but never claimed again, so it stops
// blocking its ordering key; the next record of that key is then published,
// which deliberately gives up the order for that one message. The relay logs
// every dead record at Error level.
//
// Claims and leases: a claimed record is leased to the relay for
// [WithLease]. Settling it is fenced to that claim (see [Ref]): if the lease
// lapsed during a slow publish and another relay claimed the record again,
// the first relay's late settle changes nothing; the relay logs it at Warn
// level and carries on. The record is then published by both relays, which
// at-least-once delivery allows.
//
// Deduplication: every relayed message carries the [IDAttribute] attribute
// ("outbox-id", the record ID in decimal) unless the caller set it. Delivery
// is at least once (a publish whose settlement fails is repeated), so
// consumers can use it to drop duplicates.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/timex"
)

// IDAttribute is the message attribute that carries the outbox record ID
// (decimal) on every relayed message, unless the message already has it. It
// is stable across re-publishes of a record, so consumers can deduplicate on
// it. It is part of the wire contract.
const IDAttribute = "outbox-id"

// Record is a claimed outbox row.
type Record struct {
	ID      int64
	Topic   string
	Message *pubsub.Message
	// Attempts counts the publish attempts so far, including the current
	// one: Claim adds one, and [Store.Release] takes it back for an attempt
	// that must not count (transient failure, or not published at all).
	Attempts int
	// Claim identifies this claim of the record (see [Ref]). The store
	// assigns it; every Claim of the record gets a new value.
	Claim int64
}

// Ref returns the reference that settles this claim of the record.
func (r Record) Ref() Ref { return Ref{ID: r.ID, Claim: r.Claim} }

// Ref names one claim of a record. Settle calls take a Ref and change the
// record only while that claim is the record's latest: once the lease has
// lapsed and another relay has claimed the record again, a settle with the
// old Ref is a no-op for it.
type Ref struct {
	ID    int64
	Claim int64
}

// ErrClaimLost is wrapped by settle errors for records whose claim was
// superseded by a newer Claim, or that no longer exist. See
// [ClaimLostError].
var ErrClaimLost = errors.New("outbox: claim lost")

// ClaimLostError reports the records of a settle call that were left
// unchanged because the caller no longer holds their claim. The other
// records of the call were settled. It wraps [ErrClaimLost].
type ClaimLostError struct {
	IDs []int64
}

func (e *ClaimLostError) Error() string {
	return fmt.Sprintf("outbox: claim lost for records %v", e.IDs)
}

// Unwrap returns ErrClaimLost.
func (e *ClaimLostError) Unwrap() error { return ErrClaimLost }

// Store is the persistence seam of the outbox.
//
// Settle calls (Sent, Failed, Release, Dead) are fenced to the claim: they
// change a record only while the given [Ref] is its latest claim. For any
// record whose claim was superseded or that no longer exists they change
// nothing and return a [*ClaimLostError] listing it (after settling the
// rest of the call).
type Store interface {
	// Claim leases up to limit records that are due, for lease, adds one to
	// their attempts and gives each a new Record.Claim. For a non-empty
	// ordering key it returns either nothing or a run: the key's oldest live
	// (not dead) record followed by the next records of the key in insertion
	// order, all due, unleased and for the same topic, ending before the
	// first record that is not; and only if no other relay holds a record of
	// the key. Records are returned in insertion order.
	Claim(ctx context.Context, limit int, lease time.Duration) ([]Record, error)
	// Sent removes successfully published records.
	Sent(ctx context.Context, refs []Ref) error
	// Failed releases a record for a new attempt at retryAt and records
	// cause. The attempt counts.
	Failed(ctx context.Context, ref Ref, cause error, retryAt time.Time) error
	// Release releases records for a new attempt at retryAt without counting
	// the current one (attempts minus one). A non-nil cause is recorded.
	Release(ctx context.Context, refs []Ref, cause error, retryAt time.Time) error
	// Dead marks a record as permanently failed: it is kept but never claimed
	// again and no longer counts as the head of its ordering key.
	Dead(ctx context.Context, ref Ref, cause error) error
}

// Waker is optionally implemented by a Store that can signal new records
// (e.g. Postgres LISTEN/NOTIFY). Run selects on the channel in addition to
// polling.
type Waker interface {
	Wake() <-chan struct{}
}

// RelayOption configures a Relay.
type RelayOption func(*Relay)

// WithBatchSize sets how many records are claimed per round (default 100).
func WithBatchSize(n int) RelayOption { return func(r *Relay) { r.batch = n } }

// WithPollInterval sets how often the store is polled when idle (default 1s).
func WithPollInterval(d time.Duration) RelayOption { return func(r *Relay) { r.poll = d } }

// WithLease sets how long claimed records are reserved (default 30s). The
// relay bounds the publishing of a batch so that it ends a settle timeout
// (min(5s, lease/4)) before the lease does; each settle call (Sent, Failed,
// Release, Dead) then gets its own settle timeout, even if Run's ctx is
// cancelled. A publish cut by that bound is a transient failure.
func WithLease(d time.Duration) RelayOption { return func(r *Relay) { r.lease = d } }

// WithRetryBackoff sets the delay before re-publishing a failed record:
// base·2^(attempts-1) capped at maxDelay, with jitter (default 1s, 5m).
func WithRetryBackoff(base, maxDelay time.Duration) RelayOption {
	return func(r *Relay) { r.retryBase, r.retryMax = base, maxDelay }
}

// WithLogger sets the logger (nil means log.Default()).
func WithLogger(l *log.Logger) RelayOption { return func(r *Relay) { r.log = l } }

// WithMaxAttempts marks a record dead (see [Store.Dead]) once n counted
// publish attempts have failed. 0 (default) retries without limit; errors
// wrapping [pubsub.ErrPermanent] are dead at once regardless.
//
// Transient failures back off but do not count: errors whose errs code is
// UNAVAILABLE or DEADLINE_EXCEEDED (such as [errs.ErrDependencyUnavailable],
// which kitchen publishers return for broker failures), context deadline
// and cancellation, and [pubsub.ErrKeyPaused]; the same holds per entry of a
// [*pubsub.PublishError]. An UNAVAILABLE error that wraps a more specific
// gRPC or errs code (for example a broker's INVALID_ARGUMENT) counts. A
// relay that was killed mid-publish also spends the attempt. While
// transient failures continue, their backoff grows with the number of
// consecutive failed rounds.
func WithMaxAttempts(n int) RelayOption { return func(r *Relay) { r.maxAttempts = n } }

// Relay moves records from a Store to a Publisher.
type Relay struct {
	store     Store
	pub       pubsub.Publisher
	batch     int
	poll      time.Duration
	lease     time.Duration
	retryBase time.Duration
	retryMax  time.Duration
	log       *log.Logger

	maxAttempts int
	streak      atomic.Int64 // consecutive rounds with a transient failure and nothing sent
}

// NewRelay returns a Relay. It does not close pub.
func NewRelay(store Store, pub pubsub.Publisher, opts ...RelayOption) (*Relay, error) {
	if store == nil || pub == nil {
		return nil, errors.New("outbox: nil store or publisher")
	}
	r := &Relay{
		store: store, pub: pub,
		batch: 100, poll: time.Second, lease: 30 * time.Second,
		retryBase: time.Second, retryMax: 5 * time.Minute,
	}
	for _, o := range opts {
		o(r)
	}
	if r.batch < 1 || r.poll <= 0 || r.lease <= 0 || r.retryBase <= 0 || r.retryMax < r.retryBase || r.maxAttempts < 0 {
		return nil, errors.New("outbox: invalid relay options")
	}
	r.log = log.OrDefault(r.log)
	return r, nil
}

// Run relays records until ctx is cancelled and returns nil then. A batch in
// progress is finished (bounded by the lease) before Run returns. After a
// round that sent a record or marked one dead, Run claims again at once.
func (r *Relay) Run(ctx context.Context) error {
	var wake <-chan struct{}
	if w, ok := r.store.(Waker); ok {
		wake = w.Wake()
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		case <-wake:
		}
		res, err := r.round(ctx)
		next := r.poll
		if err != nil && ctx.Err() == nil {
			r.log.ErrorContext(ctx, "outbox: relay round failed", log.Err(err))
		}
		if err == nil && (res.claimed == r.batch || res.progressed) {
			next = 0 // more may be waiting
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(next)
	}
}

// RunOnce claims and publishes one batch and returns the number of records
// claimed.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	res, err := r.round(ctx)
	return res.claimed, err
}

type topicKey struct{ topic, key string }

type roundResult struct {
	claimed    int
	progressed bool // a record was sent or marked dead
}

// settleTimeout bounds each settle call.
func (r *Relay) settleTimeout() time.Duration { return min(5*time.Second, r.lease/4) }

func (r *Relay) round(ctx context.Context) (roundResult, error) {
	if ctx.Err() != nil {
		return roundResult{}, ctx.Err()
	}
	claimedAt := time.Now()
	recs, err := r.store.Claim(ctx, r.batch, r.lease)
	if err != nil {
		return roundResult{}, fmt.Errorf("outbox: claim: %w", err)
	}
	res := roundResult{claimed: len(recs)}
	if len(recs) == 0 {
		return res, nil
	}
	// Publish within the lease, leaving one settle timeout; a claimed batch
	// is finished even if ctx is cancelled meanwhile.
	pctx, cancel := context.WithDeadline(context.WithoutCancel(ctx), claimedAt.Add(r.lease-r.settleTimeout()))
	defer cancel()

	var order []string
	byTopic := map[string][]Record{}
	for _, rec := range recs {
		if _, ok := byTopic[rec.Topic]; !ok {
			order = append(order, rec.Topic)
		}
		byTopic[rec.Topic] = append(byTopic[rec.Topic], rec)
	}
	var (
		sent, followers []Ref
		failedKeys      []topicKey // for ResumeKey
		keyFailed       = map[string]bool{}
		transientSeen   bool
		errList         []error
	)
	settle := func(what string, id int64, fn func(context.Context) error) {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.settleTimeout())
		defer cancel()
		err := fn(sctx)
		if lost, ok := errors.AsType[*ClaimLostError](err); ok {
			// The lease lapsed and another relay claimed these records again:
			// they are its to settle now. Not a failure of this round.
			r.log.WarnContext(ctx, "outbox: claim lost before settling, skipped",
				log.String("outbox.settle", what), log.Any("outbox.ids", lost.IDs))
			err = nil
		}
		if err != nil {
			if id != 0 {
				errList = append(errList, fmt.Errorf("outbox: mark %d %s: %w", id, what, err))
			} else {
				errList = append(errList, fmt.Errorf("outbox: mark %s: %w", what, err))
			}
		}
	}
	for _, topic := range order {
		group := byTopic[topic]
		msgs := make([]*pubsub.Message, len(group))
		for i, rec := range group {
			m := rec.Message.Clone()
			if _, ok := m.Attributes[IDAttribute]; !ok {
				if m.Attributes == nil {
					m.Attributes = make(map[string]string, 1)
				}
				m.Attributes[IDAttribute] = strconv.FormatInt(rec.ID, 10)
			}
			msgs[i] = m
		}
		_, perr := r.pub.Publish(pctx, topic, msgs...)
		var pe *pubsub.PublishError
		hasPE := errors.As(perr, &pe)
		for i, rec := range group {
			key := rec.Message.Key
			if key != "" && keyFailed[key] {
				// An earlier record of the key failed: this one must follow it,
				// whatever the broker did with it.
				followers = append(followers, rec.Ref())
				continue
			}
			var e error
			switch {
			case perr == nil:
			case hasPE:
				e = pe.Failed[i]
			default:
				e = perr
			}
			if e == nil {
				sent = append(sent, rec.Ref())
				continue
			}
			if key != "" {
				keyFailed[key] = true
				failedKeys = append(failedKeys, topicKey{topic, key})
			}
			counted := !isTransient(e)
			switch {
			case pubsub.IsPermanent(e) || (counted && r.maxAttempts > 0 && rec.Attempts >= r.maxAttempts):
				r.log.ErrorContext(ctx, "outbox: record is dead, giving up",
					log.Int64("outbox.id", rec.ID), log.String("messaging.destination.name", topic),
					log.String("messaging.message.ordering_key", key),
					log.Int("attempts", rec.Attempts), log.Err(e))
				settle("dead", rec.ID, func(c context.Context) error { return r.store.Dead(c, rec.Ref(), e) })
				res.progressed = true
			case counted:
				retryAt := time.Now().Add(r.backoff(rec.Attempts))
				r.log.WarnContext(ctx, "outbox: publish failed",
					log.Int64("outbox.id", rec.ID), log.String("messaging.destination.name", topic),
					log.Int("attempts", rec.Attempts), log.Time("retry_at", retryAt), log.Err(e))
				settle("failed", rec.ID, func(c context.Context) error { return r.store.Failed(c, rec.Ref(), e, retryAt) })
			default:
				transientSeen = true
				retryAt := time.Now().Add(r.backoff(max(rec.Attempts-1, int(r.streak.Load())+1)))
				r.log.WarnContext(ctx, "outbox: publish failed (transient, attempt not counted)",
					log.Int64("outbox.id", rec.ID), log.String("messaging.destination.name", topic),
					log.Int("attempts", rec.Attempts-1), log.Time("retry_at", retryAt), log.Err(e))
				settle("released", rec.ID, func(c context.Context) error { return r.store.Release(c, []Ref{rec.Ref()}, e, retryAt) })
			}
		}
	}
	if len(sent) > 0 {
		settle("sent", 0, func(c context.Context) error { return r.store.Sent(c, sent) })
		res.progressed = true
	}
	if len(followers) > 0 {
		// Not attempted on their own account: due again at once, still
		// behind the failed record of their key.
		now := time.Now()
		settle("released", 0, func(c context.Context) error { return r.store.Release(c, followers, nil, now) })
	}
	switch {
	case len(sent) > 0:
		r.streak.Store(0)
	case transientSeen:
		r.streak.Add(1)
	}
	if kr, ok := r.pub.(pubsub.KeyResumer); ok {
		// A failed record stays the head of its key (its followers were
		// released behind it), so resuming cannot reorder. A dead record
		// releases the key on purpose.
		for _, k := range failedKeys {
			kr.ResumeKey(k.topic, k.key)
		}
	}
	return res, errors.Join(errList...)
}

func (r *Relay) backoff(attempts int) time.Duration {
	return timex.Backoff(r.retryBase, r.retryMax, max(attempts, 1))
}

// isTransient reports whether a publish failure must not count toward
// WithMaxAttempts: context errors, a paused key, and errors whose code is
// UNAVAILABLE or DEADLINE_EXCEEDED unless a more specific code is wrapped
// underneath. Permanent errors are never transient.
func isTransient(err error) bool {
	if err == nil || pubsub.IsPermanent(err) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, pubsub.ErrKeyPaused) {
		return true
	}
	return transientCode(err)
}

// transientCode reports whether err's code is UNAVAILABLE or
// DEADLINE_EXCEEDED and no cause under it carries a more specific code.
func transientCode(err error) bool {
	switch errs.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
	default:
		return false
	}
	var e *errs.Error
	if !errors.As(err, &e) {
		return true // a gRPC status or a context error
	}
	cause := e.Unwrap()
	if cause == nil || errs.Code(cause) == codes.Unknown {
		return true
	}
	return transientCode(cause)
}
