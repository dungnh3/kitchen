// Package outbox implements the transactional outbox pattern on top of
// [pubsub.Publisher].
//
// Application code writes messages into an outbox table in the same database
// transaction as its business data (see
// [github.com/dungnh3/kitchen/pubsub/outbox/pgx] for Postgres). A [Relay]
// claims pending rows, publishes them and deletes them, so a message is
// published if and only if its transaction committed (at least once).
//
// Per-key order: a Store hands out at most the oldest pending record of each
// ordering key at a time, so several relays may run concurrently.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/dungnh3/kitchen/pubsub"
)

// Record is a claimed outbox row.
type Record struct {
	ID       int64
	Topic    string
	Message  *pubsub.Message
	Attempts int // publish attempts so far, including the current one
}

// Store is the persistence seam of the outbox.
type Store interface {
	// Claim leases up to limit records that are due, for lease. It returns at
	// most one record per non-empty ordering key: the oldest unsent one, and
	// only if no other relay holds it. Records are returned in insertion order.
	Claim(ctx context.Context, limit int, lease time.Duration) ([]Record, error)
	// Sent removes successfully published records.
	Sent(ctx context.Context, ids []int64) error
	// Failed releases a record for a new attempt at retryAt.
	Failed(ctx context.Context, id int64, cause error, retryAt time.Time) error
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

// WithLease sets how long claimed records are reserved (default 30s). It must
// exceed the time needed to publish one batch.
func WithLease(d time.Duration) RelayOption { return func(r *Relay) { r.lease = d } }

// WithRetryBackoff sets the delay before re-publishing a failed record:
// base·2^(attempts-1) capped at maxDelay, with jitter (default 1s, 5m).
func WithRetryBackoff(base, maxDelay time.Duration) RelayOption {
	return func(r *Relay) { r.retryBase, r.retryMax = base, maxDelay }
}

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) RelayOption { return func(r *Relay) { r.log = l } }

// Relay moves records from a Store to a Publisher.
type Relay struct {
	store     Store
	pub       pubsub.Publisher
	batch     int
	poll      time.Duration
	lease     time.Duration
	retryBase time.Duration
	retryMax  time.Duration
	log       *slog.Logger
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
	if r.batch < 1 || r.poll <= 0 || r.lease <= 0 || r.retryBase <= 0 || r.retryMax < r.retryBase {
		return nil, errors.New("outbox: invalid relay options")
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	return r, nil
}

// Run relays records until ctx is cancelled and returns nil then. A batch in
// progress is finished (bounded by the lease) before Run returns.
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
		n, err := r.RunOnce(ctx)
		next := r.poll
		switch {
		case err != nil && ctx.Err() == nil:
			r.log.Error("outbox: relay round failed", slog.Any("error", err))
		case n == r.batch:
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
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	recs, err := r.store.Claim(ctx, r.batch, r.lease)
	if err != nil {
		return 0, fmt.Errorf("outbox: claim: %w", err)
	}
	if len(recs) == 0 {
		return 0, nil
	}
	// Settle claimed records even if ctx is cancelled meanwhile.
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.lease)
	defer cancel()

	var order []string
	byTopic := map[string][]Record{}
	for _, rec := range recs {
		if _, ok := byTopic[rec.Topic]; !ok {
			order = append(order, rec.Topic)
		}
		byTopic[rec.Topic] = append(byTopic[rec.Topic], rec)
	}
	var sent []int64
	var errs []error
	for _, topic := range order {
		group := byTopic[topic]
		msgs := make([]*pubsub.Message, len(group))
		for i, rec := range group {
			msgs[i] = rec.Message
		}
		_, perr := r.pub.Publish(sctx, topic, msgs...)
		var pe *pubsub.PublishError
		hasPE := errors.As(perr, &pe)
		for i, rec := range group {
			var e error
			switch {
			case perr == nil:
			case hasPE:
				e = pe.Failed[i]
			default:
				e = perr
			}
			if e == nil {
				sent = append(sent, rec.ID)
				continue
			}
			retryAt := time.Now().Add(r.backoff(rec.Attempts))
			r.log.Warn("outbox: publish failed",
				slog.Int64("outbox.id", rec.ID), slog.String("messaging.destination.name", topic),
				slog.Int("attempts", rec.Attempts), slog.Time("retry_at", retryAt), slog.Any("error", e))
			if ferr := r.store.Failed(sctx, rec.ID, e, retryAt); ferr != nil {
				errs = append(errs, fmt.Errorf("outbox: mark %d failed: %w", rec.ID, ferr))
			}
			if kr, ok := r.pub.(pubsub.KeyResumer); ok && rec.Message.Key != "" {
				// The record stays the head of its key, so resuming cannot
				// reorder: nothing later for the key is claimed before it.
				kr.ResumeKey(topic, rec.Message.Key)
			}
		}
	}
	if len(sent) > 0 {
		if err := r.store.Sent(sctx, sent); err != nil {
			errs = append(errs, fmt.Errorf("outbox: mark sent: %w", err))
		}
	}
	return len(recs), errors.Join(errs...)
}

func (r *Relay) backoff(attempts int) time.Duration {
	d := r.retryBase
	for i := 1; i < attempts && d < r.retryMax; i++ {
		d *= 2
	}
	d = min(d, r.retryMax)
	return d/2 + rand.N(d/2+1)
}
