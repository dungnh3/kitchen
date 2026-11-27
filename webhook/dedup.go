package webhook

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Results of [Dedup.Begin] that are not failures of the store.
var (
	// ErrDuplicate: the message was handled already.
	ErrDuplicate = errors.New("webhook: message already handled")
	// ErrInProgress: another request is handling the message now.
	ErrInProgress = errors.New("webhook: message is being handled")
)

// Dedup runs a handler at most once per message id. Senders retry until
// they get a 2xx, so the same message can arrive several times.
type Dedup interface {
	// Begin claims id. It returns a Ticket when the caller should handle
	// the message, ErrDuplicate or ErrInProgress when it should not, and
	// any other error when the store cannot say (the middleware then
	// refuses the request with 503).
	Begin(ctx context.Context, id string) (Ticket, error)
}

// Ticket is a claim on a message id.
type Ticket interface {
	// Done marks the message as handled.
	Done(ctx context.Context) error
	// Release gives the claim up so a later delivery is handled again.
	Release(ctx context.Context) error
}

// RedisDedupConfig configures [NewRedisDedup].
type RedisDedupConfig struct {
	// Prefix starts every key (default "webhook:dedup:"). Give each
	// webhook source its own prefix when message ids of different senders
	// may collide.
	Prefix string
	// TTL is how long a handled id is remembered (default 72h). Keep it
	// longer than the sender's retry window.
	TTL time.Duration
	// LockTTL is how long a claim lasts while the handler runs (default 2
	// minutes). A handler that runs longer may see a retry of the same
	// message run concurrently.
	LockTTL time.Duration
}

// RedisDedup is a [Dedup] in Redis. A claim is SET NX with LockTTL; Done
// replaces it with a marker kept for TTL; Release deletes it. Both check the
// claim's token, so a claim that expired and was taken by another request
// is left alone.
type RedisDedup struct {
	rdb redis.UniversalClient
	cfg RedisDedupConfig
}

// NewRedisDedup returns a Dedup on rdb (open it with redisx.Open).
func NewRedisDedup(rdb redis.UniversalClient, cfg RedisDedupConfig) (*RedisDedup, error) {
	if rdb == nil {
		return nil, errors.New("webhook: nil redis client")
	}
	if cfg.TTL < 0 || cfg.LockTTL < 0 {
		return nil, errors.New("webhook: negative dedup TTL")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "webhook:dedup:"
	}
	if cfg.TTL == 0 {
		cfg.TTL = 72 * time.Hour
	}
	if cfg.LockTTL == 0 {
		cfg.LockTTL = 2 * time.Minute
	}
	return &RedisDedup{rdb: rdb, cfg: cfg}, nil
}

const doneMarker = "done"

var (
	doneScript = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
end
return false`)
	releaseScript = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`)
)

// Begin implements [Dedup].
func (d *RedisDedup) Begin(ctx context.Context, id string) (Ticket, error) {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	token := "claim:" + hex.EncodeToString(b)
	key := d.cfg.Prefix + id
	ok, err := d.rdb.SetNX(ctx, key, token, d.cfg.LockTTL).Result()
	if err != nil {
		return nil, err
	}
	if ok {
		return &redisTicket{d: d, key: key, token: token}, nil
	}
	v, err := d.rdb.Get(ctx, key).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return nil, ErrInProgress // released or expired in between; the sender retries
	case err != nil:
		return nil, err
	case v == doneMarker:
		return nil, ErrDuplicate
	default:
		return nil, ErrInProgress
	}
}

type redisTicket struct {
	d     *RedisDedup
	key   string
	token string
}

func (t *redisTicket) Done(ctx context.Context) error {
	err := doneScript.Run(ctx, t.d.rdb, []string{t.key}, t.token, doneMarker, t.d.cfg.TTL.Milliseconds()).Err()
	if errors.Is(err, redis.Nil) {
		return nil // the claim expired; nothing to mark
	}
	return err
}

func (t *redisTicket) Release(ctx context.Context) error {
	return releaseScript.Run(ctx, t.d.rdb, []string{t.key}, t.token).Err()
}
