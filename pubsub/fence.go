package pubsub

import (
	"slices"
	"sync"
	"time"
)

// keyFences keeps the order of a key after one of its messages was finally
// nacked. A fence holds, in nack order, the IDs of every message of the key
// nacked since: the failed message first, then each later message of the key
// nacked to keep the order (handed over before or after the failure). Until
// the broker has redelivered all of them, in that order, every other message
// of the key is nacked and joins the list; so a message handed over early
// (Kafka polls ahead, Pub/Sub ordered delivery sends later messages while
// earlier ones are unacked) can never run before an earlier one that is on
// its way back.
//
// Kafka redelivers in process with the same IDs, so this is exact there; for
// Pub/Sub ordered delivery it covers this process. A fence expires ttl after
// it last made progress, in case a message never comes back (moved to a
// dead-letter topic, or redelivered to another consumer). Whether to fence
// at all is decided by the caller (see KeyFenceMode).
type keyFences struct {
	now func() time.Time

	mu sync.Mutex
	m  map[string]*keyFence
	// sweepAt is the map size that triggers the next sweep of expired
	// entries. It doubles with the live entries left after a sweep, so the
	// sweep cost stays amortised O(1) per block even with many live fences
	// (an outage that nacks every key).
	sweepAt int
}

type keyFence struct {
	ids   []awaited // nacked messages still to be redelivered, in arrival order
	ttl   time.Duration
	until time.Time
}

// awaited is a nacked message of a fenced key. seq is its arrival order in
// this Run (the source hands messages of one key over in order), so the
// fence awaits redeliveries in the order the broker will send them, however
// the nacks interleaved (a late message can be rejected at receive before
// an earlier one is rejected at the head of its queue).
type awaited struct {
	id  string
	seq uint64
}

const fenceSweepAt = 1024

func newKeyFences() *keyFences {
	return &keyFences{now: time.Now, m: map[string]*keyFence{}, sweepAt: fenceSweepAt}
}

// live returns the unexpired fence of key, deleting an expired one. f.mu is
// held.
func (f *keyFences) live(key string, now time.Time) *keyFence {
	kf, ok := f.m[key]
	if !ok {
		return nil
	}
	if !now.Before(kf.until) {
		delete(f.m, key)
		return nil
	}
	return kf
}

// block fences key for ttl after the final nack of message id, a message
// the handler ran. When the key is already fenced (id was the awaited
// redelivery and failed again), id goes back to the front: every message
// still awaited is later than a message that ran.
func (f *keyFences) block(key, id string, ttl time.Duration) {
	if ttl <= 0 || key == "" || id == "" {
		return
	}
	now := f.now()
	f.mu.Lock()
	defer f.mu.Unlock()
	if kf := f.live(key, now); kf != nil {
		kf.ids = slices.DeleteFunc(kf.ids, func(a awaited) bool { return a.id == id })
		kf.ids = slices.Insert(kf.ids, 0, awaited{id: id})
		kf.until = now.Add(kf.ttl)
		return
	}
	if len(f.m) >= f.sweepAt {
		for k, v := range f.m {
			if !now.Before(v.until) {
				delete(f.m, k)
			}
		}
		f.sweepAt = max(fenceSweepAt, 2*len(f.m))
	}
	f.m[key] = &keyFence{ids: []awaited{{id: id}}, ttl: ttl, until: now.Add(ttl)}
}

// follow records that message id of a fenced key was nacked to keep the
// order, so its redelivery is awaited too. It does nothing for an unfenced
// key.
func (f *keyFences) follow(m *Message, seq uint64) {
	if m.Key == "" || m.ID == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if kf := f.live(m.Key, f.now()); kf != nil {
		kf.add(m.ID, seq)
	}
}

// add inserts id by arrival order unless it is already awaited. The head
// (a message that ran, or the next redelivery) stays first.
func (kf *keyFence) add(id string, seq uint64) {
	if slices.ContainsFunc(kf.ids, func(a awaited) bool { return a.id == id }) {
		return
	}
	i := len(kf.ids)
	for i > 1 && kf.ids[i-1].seq > seq {
		i--
	}
	kf.ids = slices.Insert(kf.ids, i, awaited{id: id, seq: seq})
}

// peek reports whether m may enter the subscriber: true unless its key is
// fenced and m is not the next awaited redelivery. A rejected message joins
// the fence (the caller nacks it). peek never lifts a fence: messages of the
// key handed over earlier may still be queued ahead of the redelivery, so
// the fence advances in queue order, in admit.
func (f *keyFences) peek(m *Message, seq uint64) bool {
	if m.Key == "" {
		return true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.m) == 0 {
		return true
	}
	kf := f.live(m.Key, f.now())
	switch {
	case kf == nil, m.ID == kf.ids[0].id:
		return true
	default:
		kf.add(m.ID, seq)
		return false
	}
}

// admit reports whether m may be processed, at the head of its key's queue
// or batch. The next awaited redelivery advances the fence (and lifts it
// when it was the last); any other message of a fenced key is rejected and
// joins the fence (the caller nacks it).
func (f *keyFences) admit(m *Message, seq uint64) bool {
	if m.Key == "" {
		return true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.m) == 0 {
		return true
	}
	now := f.now()
	kf := f.live(m.Key, now)
	switch {
	case kf == nil:
		return true
	case m.ID == kf.ids[0].id:
		kf.ids = kf.ids[1:]
		if len(kf.ids) == 0 {
			delete(f.m, m.Key)
		} else {
			kf.until = now.Add(kf.ttl)
		}
		return true
	default:
		kf.add(m.ID, seq)
		return false
	}
}
