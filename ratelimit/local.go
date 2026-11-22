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

const (
	localShards = 32
	evictScan   = 8
)

// LocalStore is an in-memory Store for one instance: token buckets from
// golang.org/x/time/rate, bounded by an LRU so memory stays capped.
//
// When a shard is full it evicts, among the evictScan least recently used
// buckets, one that has refilled completely: such a bucket is identical to a
// new one, so nothing is lost. Only if none of them is full is the least
// recently used bucket dropped (which resets that key). Flooding the store
// with new keys therefore cannot reset an active key's partly used bucket
// unless every scanned bucket is active. It starts no goroutines and needs
// no Close.
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
	lim := sh.get(key, r, now)

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
func (sh *localShard) get(key string, r Rule, now time.Time) *rate.Limiter {
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
		sh.evict(now)
	}
	e := &localEntry{key: key, rule: r, lim: rate.NewLimiter(rate.Every(r.Per/time.Duration(r.Rate)), r.Burst)}
	sh.items[key] = sh.lru.PushFront(e)
	return e.lim
}

// evict drops one bucket, preferring a full one near the LRU tail.
func (sh *localShard) evict(now time.Time) {
	victim := sh.lru.Back()
	for el, i := victim, 0; el != nil && i < evictScan; el, i = el.Prev(), i+1 {
		e := el.Value.(*localEntry)
		if e.lim.TokensAt(now) >= float64(e.rule.Burst) {
			victim = el
			break
		}
	}
	sh.lru.Remove(victim)
	delete(sh.items, victim.Value.(*localEntry).key)
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
