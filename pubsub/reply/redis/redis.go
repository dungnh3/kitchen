// Package redis implements [reply.Broker] on Redis.
//
// Reply pushes the payload onto a per-ID list ("<prefix><id>") with a TTL in
// one MULTI/EXEC; Wait pops it with BLPOP. Because the reply is stored, a
// reply that arrives before Wait is not lost, and only the waiter of that ID
// receives it (no fan-out). Each waiting call holds one connection while
// blocked: size the client's pool for the expected number of concurrent
// waiters, preferably on a client dedicated to the broker.
//
// Wait needs Redis 6.0 or later: it sends BLPOP with a fractional timeout
// that ends just before ctx's deadline (and before the client's read
// timeout), so a timed-out Wait returns context.DeadlineExceeded and keeps
// its pooled connection.
package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/pubsub/reply"
)

// Option configures a Broker.
type Option func(*Broker)

// WithPrefix sets the key prefix (default "reply:"). Use a hash tag such as
// "{reply}:" on Redis Cluster only if you need keys co-located.
func WithPrefix(p string) Option { return func(b *Broker) { b.prefix = p } }

// WithTTL sets how long an unclaimed reply is kept (default 1m).
func WithTTL(d time.Duration) Option { return func(b *Broker) { b.ttl = d } }

// WithMaxPayload limits reply size in bytes (default 1 MiB).
func WithMaxPayload(n int) Option { return func(b *Broker) { b.maxPayload = n } }

// Broker is a Redis reply broker.
type Broker struct {
	rdb        goredis.UniversalClient
	prefix     string
	ttl        time.Duration
	maxPayload int

	readTimeout time.Duration // the client's socket read timeout; BLPOP blocks end before it
}

var _ reply.Broker = (*Broker)(nil)

// New returns a Broker using rdb, which the caller owns. Create rdb with
// [github.com/dungnh3/kitchen/redisx.Open] (tracing, metrics, context
// deadlines).
func New(rdb goredis.UniversalClient, opts ...Option) (*Broker, error) {
	if rdb == nil {
		return nil, errors.New("reply/redis: nil client")
	}
	b := &Broker{rdb: rdb, prefix: "reply:", ttl: time.Minute, maxPayload: 1 << 20, readTimeout: readTimeoutOf(rdb)}
	for _, o := range opts {
		o(b)
	}
	if b.ttl < time.Millisecond || b.maxPayload < 1 || b.prefix == "" || strings.ContainsAny(b.prefix, " \r\n") {
		return nil, errors.New("reply/redis: invalid options")
	}
	return b, nil
}

func (b *Broker) key(id string) (string, error) {
	if err := reply.ValidateID(id); err != nil {
		return "", err
	}
	return b.prefix + id, nil
}

// Expect implements [reply.Broker]. Registration is implicit in Redis (the
// reply is stored until its TTL), so Expect only validates id.
func (b *Broker) Expect(_ context.Context, id string) (reply.Waiter, error) {
	k, err := b.key(id)
	if err != nil {
		return nil, err
	}
	return &waiter{b: b, key: k}, nil
}

// Reply implements [reply.Broker].
func (b *Broker) Reply(ctx context.Context, id string, payload []byte) error {
	k, err := b.key(id)
	if err != nil {
		return err
	}
	if len(payload) > b.maxPayload {
		return errs.Wrap(fmt.Errorf("reply/redis: payload of %d bytes exceeds %d", len(payload), b.maxPayload), errs.ErrInvalidArgument)
	}
	_, err = b.rdb.TxPipelined(ctx, func(p goredis.Pipeliner) error {
		p.LPush(ctx, k, payload)
		p.PExpire(ctx, k, b.ttl)
		return nil
	})
	if err != nil {
		return errs.Wrap(fmt.Errorf("reply/redis: %w", err), errs.ErrDependencyUnavailable)
	}
	return nil
}

type waiter struct {
	b      *Broker
	key    string
	popped atomic.Bool // the reply was consumed; Close has nothing to delete
}

const (
	// maxBlock bounds one BLPOP so that cancellation without a deadline is
	// noticed promptly.
	maxBlock = 2 * time.Second
	// blockMargin ends the server-side block this long before ctx's deadline
	// and before the client's socket read timeout. It covers the round trip
	// and Redis' timeout granularity (its event loop wakes at least every
	// 1000/hz ms, 100ms by default), so the server normally answers before
	// the socket deadline: the pooled connection survives and the deadline
	// is reported as context.DeadlineExceeded.
	blockMargin = 150 * time.Millisecond
	// minBlock is the shortest block worth a round trip.
	minBlock = 10 * time.Millisecond
)

// Wait implements [reply.Waiter]. It sends BLPOP with a fractional timeout
// (Redis 6.0 or later) that ends just before ctx's deadline; within the last
// blockMargin before the deadline it waits without a round trip.
func (w *waiter) Wait(ctx context.Context) ([]byte, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		block := maxBlock
		if rt := w.b.readTimeout; rt > 0 {
			block = min(block, max(rt-blockMargin, rt/2))
		}
		dl, hasDL := ctx.Deadline()
		if hasDL {
			block = min(block, time.Until(dl)-blockMargin)
		}
		if block < minBlock {
			// Too close to the deadline for a blocking round trip.
			<-ctx.Done()
			return nil, ctx.Err()
		}
		res, err := w.b.rdb.Do(ctx, "BLPOP", w.key, strconv.FormatFloat(block.Seconds(), 'f', 3, 64)).StringSlice()
		switch {
		case err == nil && len(res) == 2:
			w.popped.Store(true)
			return []byte(res[1]), nil
		case err == nil:
			return nil, errs.Wrap(fmt.Errorf("reply/redis: unexpected BLPOP reply of %d elements", len(res)), errs.ErrDependencyUnavailable)
		case errors.Is(err, goredis.Nil):
			continue
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case hasDL && !time.Now().Before(dl):
			// The socket deadline (the ctx deadline) fired a moment before
			// ctx's own timer.
			return nil, context.DeadlineExceeded
		default:
			return nil, errs.Wrap(fmt.Errorf("reply/redis: %w", err), errs.ErrDependencyUnavailable)
		}
	}
}

// Close deletes an unclaimed reply. After a successful Wait it does nothing.
func (w *waiter) Close() error {
	if w.popped.Load() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.b.rdb.Del(ctx, w.key).Err(); err != nil {
		return errs.Wrap(fmt.Errorf("reply/redis: close: %w", err), errs.ErrDependencyUnavailable)
	}
	return nil
}

// readTimeoutOf returns the client's socket read timeout (<= 0: none), or
// the go-redis default for clients whose options are not reachable.
func readTimeoutOf(rdb goredis.UniversalClient) time.Duration {
	switch c := rdb.(type) {
	case interface{ Options() *goredis.Options }:
		return c.Options().ReadTimeout
	case interface {
		Options() *goredis.ClusterOptions
	}:
		return c.Options().ReadTimeout
	case interface{ Options() *goredis.RingOptions }:
		return c.Options().ReadTimeout
	}
	return 3 * time.Second
}
