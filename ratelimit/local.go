package ratelimit

import (
	"container/list"
	"context"
	"fmt"
	"hash/maphash"
	"sync"
	"time"
)

const (
	localShards = 32
	evictScan   = 8
)

// LocalStore is an in-memory Store for one instance: GCRA buckets in integer
// microseconds, the same arithmetic as the ratelimit/redis Lua script, so
// both stores return the same decisions for the same requests and clock.
// An LRU bounds memory.
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
	key string
	tat int64 // theoretical arrival time, unix microseconds
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

// gcraParams returns the GCRA emission interval and burst tolerance of r in
// microseconds. The interval is rounded up, so a rule is never looser than
// configured. ratelimit/redis computes the same values.
func gcraParams(r Rule) (interval, tolerance int64) {
	per, rate := int64(r.Per/time.Microsecond), int64(r.Rate)
	if r.Per%time.Microsecond != 0 {
		per++ // ceil to whole microseconds
	}
	interval = (per + rate - 1) / rate
	return interval, interval * int64(r.Burst)
}

// gcra is one GCRA step at now (microseconds) on a bucket whose theoretical
// arrival time is *tat. It updates *tat only when the request is allowed.
func gcra(tat *int64, now int64, r Rule) Decision {
	interval, tolerance := gcraParams(r)
	t := max(*tat, now)
	newTAT := t + interval
	allowAt := newTAT - tolerance
	d := Decision{Limit: r.Rate}
	if now < allowAt {
		d.RetryAfter = time.Duration(allowAt-now) * time.Microsecond
		d.ResetAfter = time.Duration(t-now) * time.Microsecond
		return d
	}
	*tat = newTAT
	d.Allowed = true
	d.Remaining = int((now - allowAt) / interval)
	d.ResetAfter = time.Duration(newTAT-now) * time.Microsecond
	return d
}

// Allow implements Store. It returns an error only for an invalid rule.
func (s *LocalStore) Allow(_ context.Context, key string, r Rule) (Decision, error) {
	r = r.normalize()
	if err := r.validate(); err != nil {
		return Decision{}, fmt.Errorf("ratelimit: invalid rule: %w", err)
	}
	now := s.now().UnixMicro()
	sh := &s.shards[maphash.String(s.seed, key)%localShards]

	sh.mu.Lock()
	defer sh.mu.Unlock()
	e := sh.get(key, now)
	return gcra(&e.tat, now, r), nil
}

// get returns the bucket for key, creating it (and evicting an LRU entry)
// when needed.
func (sh *localShard) get(key string, now int64) *localEntry {
	if el, ok := sh.items[key]; ok {
		sh.lru.MoveToFront(el)
		return el.Value.(*localEntry)
	}
	for len(sh.items) >= sh.cap {
		sh.evict(now)
	}
	e := &localEntry{key: key, tat: now}
	sh.items[key] = sh.lru.PushFront(e)
	return e
}

// evict drops one bucket, preferring a full one (tat <= now) near the LRU
// tail.
func (sh *localShard) evict(now int64) {
	victim := sh.lru.Back()
	for el, i := victim, 0; el != nil && i < evictScan; el, i = el.Prev(), i+1 {
		if el.Value.(*localEntry).tat <= now {
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
