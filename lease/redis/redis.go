// Package redis implements [lease.Locker] on Redis.
//
// A lease is the key "<prefix>{<name>}" holding "<owner>|<token>" with a
// millisecond TTL. The fencing token comes from INCR on "<prefix>{<name>}:fence",
// which has no TTL, so tokens strictly increase per name while that key
// survives. Acquire, Renew and Release are Lua scripts, so each one is
// atomic; Renew and Release check owner and token. The hash tag keeps both
// keys in one Redis Cluster slot.
//
// Redis replication is asynchronous: after a failover a lease can be granted
// twice and a token can repeat. Always pass the fencing token to the
// resource you protect.
package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/lease"
)

var (
	acquireScript = goredis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then return -1 end
local t = redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[1], ARGV[1] .. '|' .. t, 'PX', ARGV[2])
return t`)
	renewScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
return 0`)
	releaseScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0`)
)

// Option configures a Locker.
type Option func(*Locker)

// WithPrefix sets the key prefix (default "lease:").
func WithPrefix(p string) Option { return func(l *Locker) { l.prefix = p } }

// Locker is a Redis lease.Locker.
type Locker struct {
	rdb    goredis.UniversalClient
	prefix string
}

var _ lease.Locker = (*Locker)(nil)

// New returns a Locker using rdb, which the caller owns.
//
// Create rdb with [github.com/dungnh3/kitchen/redisx.Open].
func New(rdb goredis.UniversalClient, opts ...Option) (*Locker, error) {
	if rdb == nil {
		return nil, errors.New("lease/redis: nil client")
	}
	l := &Locker{rdb: rdb, prefix: "lease:"}
	for _, o := range opts {
		o(l)
	}
	if strings.ContainsAny(l.prefix, "{} \r\n") {
		return nil, errors.New("lease/redis: invalid prefix")
	}
	return l, nil
}

func (l *Locker) keys(name string) ([]string, error) {
	if name == "" || len(name) > 200 || strings.ContainsAny(name, "{} \r\n") {
		return nil, fmt.Errorf("lease/redis: invalid name %q", name)
	}
	k := l.prefix + "{" + name + "}"
	return []string{k, k + ":fence"}, nil
}

func value(l lease.Lease) string { return l.Owner + "|" + strconv.FormatUint(l.Token, 10) }

// Acquire implements [lease.Locker].
func (l *Locker) Acquire(ctx context.Context, name, owner string, ttl time.Duration) (lease.Lease, error) {
	keys, err := l.keys(name)
	if err != nil {
		return lease.Lease{}, err
	}
	if owner == "" || ttl < time.Millisecond {
		return lease.Lease{}, errors.New("lease/redis: empty owner or ttl below 1ms")
	}
	t, err := acquireScript.Run(ctx, l.rdb, keys, owner, ttl.Milliseconds()).Int64()
	if err != nil {
		return lease.Lease{}, fmt.Errorf("lease/redis: acquire %s: %w", name, err)
	}
	if t < 0 {
		return lease.Lease{}, lease.ErrNotHeld
	}
	return lease.Lease{Name: name, Owner: owner, Token: uint64(t), TTL: ttl}, nil
}

// Renew implements [lease.Locker].
func (l *Locker) Renew(ctx context.Context, ls lease.Lease) (lease.Lease, error) {
	keys, err := l.keys(ls.Name)
	if err != nil {
		return lease.Lease{}, err
	}
	n, err := renewScript.Run(ctx, l.rdb, keys[:1], value(ls), ls.TTL.Milliseconds()).Int64()
	if err != nil {
		return lease.Lease{}, fmt.Errorf("lease/redis: renew %s: %w", ls.Name, err)
	}
	if n != 1 {
		return lease.Lease{}, lease.ErrNotHeld
	}
	return ls, nil
}

// Release implements [lease.Locker].
func (l *Locker) Release(ctx context.Context, ls lease.Lease) error {
	keys, err := l.keys(ls.Name)
	if err != nil {
		return err
	}
	if err := releaseScript.Run(ctx, l.rdb, keys[:1], value(ls)).Err(); err != nil {
		return fmt.Errorf("lease/redis: release %s: %w", ls.Name, err)
	}
	return nil
}
