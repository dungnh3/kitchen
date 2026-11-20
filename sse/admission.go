package sse

import (
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"
)

var (
	// ErrTooManyConnections means the server is at ServerConfig.MaxConnections (map to 503).
	ErrTooManyConnections = errors.New("sse: too many connections")
	// ErrTooManyForStream means the stream is at MaxConnectionsPerStream (map to 429).
	ErrTooManyForStream = errors.New("sse: too many connections for stream")
	// ErrServerClosed means Shutdown was called.
	ErrServerClosed = errors.New("sse: server closed")
)

const admissionShards = 64

type admission struct {
	max, perStream int64
	total          atomic.Int64
	seed           maphash.Seed
	shards         [admissionShards]admissionShard
}

type admissionShard struct {
	mu sync.Mutex
	m  map[string]int64
}

func newAdmission(maxTotal, perStream int) *admission {
	a := &admission{max: int64(maxTotal), perStream: int64(perStream), seed: maphash.MakeSeed()}
	for i := range a.shards {
		a.shards[i].m = make(map[string]int64)
	}
	return a
}

func (a *admission) shard(id string) *admissionShard {
	return &a.shards[maphash.String(a.seed, id)%admissionShards]
}

func (a *admission) acquire(streamID string) error {
	if n := a.total.Add(1); a.max > 0 && n > a.max {
		a.total.Add(-1)
		return ErrTooManyConnections
	}
	sh := a.shard(streamID)
	sh.mu.Lock()
	if a.perStream > 0 && sh.m[streamID] >= a.perStream {
		sh.mu.Unlock()
		a.total.Add(-1)
		return ErrTooManyForStream
	}
	sh.m[streamID]++
	sh.mu.Unlock()
	return nil
}

func (a *admission) release(streamID string) {
	sh := a.shard(streamID)
	sh.mu.Lock()
	if n := sh.m[streamID] - 1; n > 0 {
		sh.m[streamID] = n
	} else {
		delete(sh.m, streamID)
	}
	sh.mu.Unlock()
	a.total.Add(-1)
}

// Ticket is a reserved connection slot from Server.Admit. Pass it to Serve,
// which takes ownership; call Release only if Serve is never called.
type Ticket struct {
	a        *admission
	streamID string
	done     atomic.Bool
}

// StreamID returns the stream the ticket was admitted for.
func (t *Ticket) StreamID() string { return t.streamID }

// Release returns the slot. It is idempotent and safe after Serve.
func (t *Ticket) Release() {
	if t != nil && t.done.CompareAndSwap(false, true) {
		t.a.release(t.streamID)
	}
}
