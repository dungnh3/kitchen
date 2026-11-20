package ratelimit

import (
	"container/list"
	"context"
	"hash/maphash"
	"math"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const localShards = 32

// LocalStore is an in-memory Store for one instance: token buckets from
// golang.org/x/time/rate, bounded by an LRU so memory stays capped. When it
// is full the least recently used bucket is dropped, which resets that key.
// It starts no goroutines and needs no Close.
type LocalStore struct {
	seed   maphash.Seed
	shards [localShards]localShard
	now    func() time.Time
}

type localShard struct {
	mu    sync.Mutex
	cap   int
	items map[string]*list.Element
	lru   list.List // front = most recent
}

type localEntry struct {
	key  string
	rule Rule
	lim  *rate.Limiter
}

// NewLocalStore returns a LocalStore holding at most about maxKeys buckets
// (minimum one per shard).
func NewLocalStore(maxKeys int) *LocalStore {
	s := &LocalStore{seed: maphash.MakeSeed(), now: time.Now}
	per := max(1, maxKeys/localShards)
	for i := range s.shards {
		s.shards[i].cap = per
		s.shards[i].items = make(map[string]*list.Element)
	}
	return s
}

// Allow implements Store. It never returns an error.
func (s *LocalStore) Allow(_ context.Context, key string, r Rule) (Decision, error) {
	r = r.normalize()
	if r.validate() != nil {
		return Decision{Allowed: true}, nil
	}
	now := s.now()
	sh := &s.shards[maphash.String(s.seed, key)%localShards]

	sh.mu.Lock()
	defer sh.mu.Unlock()
	lim := sh.get(key, r)

	interval := r.Per / time.Duration(r.Rate)
	d := Decision{Limit: r.Rate}
	res := lim.ReserveN(now, 1)
	if !res.OK() {
		return d, nil // burst < 1: never allowed
	}
	if delay := res.DelayFrom(now); delay > 0 {
		res.CancelAt(now)
		d.RetryAfter = delay
		d.ResetAfter = fullAfter(lim, now, r.Burst, interval)
		return d, nil
	}
	tokens := lim.TokensAt(now)
	d.Allowed = true
	d.Remaining = max(0, int(math.Floor(tokens)))
	d.ResetAfter = fullAfter(lim, now, r.Burst, interval)
	return d, nil
}

func fullAfter(lim *rate.Limiter, now time.Time, burst int, interval time.Duration) time.Duration {
	missing := float64(burst) - lim.TokensAt(now)
	if missing <= 0 {
		return 0
	}
	return time.Duration(missing * float64(interval))
}

// get returns the limiter for key, creating it (and evicting the LRU entry)
// when needed. A changed rule replaces the bucket.
func (sh *localShard) get(key string, r Rule) *rate.Limiter {
	if el, ok := sh.items[key]; ok {
		e := el.Value.(*localEntry)
		if e.rule == r {
			sh.lru.MoveToFront(el)
			return e.lim
		}
		sh.lru.Remove(el)
		delete(sh.items, key)
	}
	for len(sh.items) >= sh.cap {
		oldest := sh.lru.Back()
		sh.lru.Remove(oldest)
		delete(sh.items, oldest.Value.(*localEntry).key)
	}
	e := &localEntry{key: key, rule: r, lim: rate.NewLimiter(rate.Every(r.Per/time.Duration(r.Rate)), r.Burst)}
	sh.items[key] = sh.lru.PushFront(e)
	return e.lim
}

// Len returns the number of buckets held.
func (s *LocalStore) Len() int {
	n := 0
	for i := range s.shards {
		s.shards[i].mu.Lock()
		n += len(s.shards[i].items)
		s.shards[i].mu.Unlock()
	}
	return n
}
