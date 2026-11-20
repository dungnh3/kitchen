// Package redis implements [reply.Broker] on Redis.
//
// Reply pushes the payload onto a per-ID list ("<prefix><id>") with a TTL in
// one MULTI/EXEC; Wait pops it with BLPOP. Because the reply is stored, a
// reply that arrives before Wait is not lost, and only the waiter of that ID
// receives it (no fan-out). Each waiting call holds one connection while
// blocked: size the client's pool for the expected number of concurrent
// waiters.
package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

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
}

var _ reply.Broker = (*Broker)(nil)

// New returns a Broker using rdb, which the caller owns.
func New(rdb goredis.UniversalClient, opts ...Option) (*Broker, error) {
	if rdb == nil {
		return nil, errors.New("reply/redis: nil client")
	}
	b := &Broker{rdb: rdb, prefix: "reply:", ttl: time.Minute, maxPayload: 1 << 20}
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
		return fmt.Errorf("reply/redis: payload of %d bytes exceeds %d", len(payload), b.maxPayload)
	}
	_, err = b.rdb.TxPipelined(ctx, func(p goredis.Pipeliner) error {
		p.LPush(ctx, k, payload)
		p.PExpire(ctx, k, b.ttl)
		return nil
	})
	if err != nil {
		return fmt.Errorf("reply/redis: %w", err)
	}
	return nil
}

type waiter struct {
	b   *Broker
	key string
}

// maxBlock bounds one BLPOP so that cancellation without a deadline is
// noticed promptly.
const maxBlock = 2 * time.Second

func (w *waiter) Wait(ctx context.Context) ([]byte, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		block := maxBlock
		if dl, ok := ctx.Deadline(); ok {
			block = min(block, time.Until(dl))
		}
		if block < time.Millisecond {
			return nil, context.DeadlineExceeded
		}
		res, err := w.b.rdb.BLPop(ctx, block, w.key).Result()
		switch {
		case err == nil:
			return []byte(res[1]), nil
		case errors.Is(err, goredis.Nil):
			continue
		case ctx.Err() != nil:
			return nil, ctx.Err()
		default:
			return nil, fmt.Errorf("reply/redis: %w", err)
		}
	}
}

// Close deletes an unclaimed reply.
func (w *waiter) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return w.b.rdb.Del(ctx, w.key).Err()
}
