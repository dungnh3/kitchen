package pubsub

import (
	"context"
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dungnh3/kitchen/log"
)

type shard struct {
	mu     sync.Mutex
	buf    []Delivery
	closed bool
	signal chan struct{}
	// load counts messages assigned to the shard and not yet settled.
	load atomic.Int64
}

// isolationMaxFailures is how many isolated messages in a row may fail
// before the rest of a failed batch is treated as a systemic failure (a
// dependency outage) and nacked without running them one by one.
const isolationMaxFailures = 2

func (r *run) dispatchBatch(d Delivery) {
	var sh *shard
	if k := d.Message().Key; k != "" {
		sh = r.shards[maphash.String(r.seed, k)%uint64(len(r.shards))]
	} else {
		// The least loaded shard (ties rotate), so unkeyed messages do not
		// queue behind a busy shard while others are idle.
		n := uint64(len(r.shards))
		start := r.rr.Add(1) % n
		sh = r.shards[start]
		for j := uint64(1); j < n; j++ {
			if c := r.shards[(start+j)%n]; c.load.Load() < sh.load.Load() {
				sh = c
			}
		}
	}
	sh.mu.Lock()
	if sh.closed {
		sh.mu.Unlock()
		r.nackAs(r.hctx, d, outcomeShutdown)
		return
	}
	sh.load.Add(1)
	sh.buf = append(sh.buf, d)
	full := len(sh.buf) >= r.s.o.maxBatchSize
	sh.mu.Unlock()
	if full {
		select {
		case sh.signal <- struct{}{}:
		default:
		}
	}
}

func (r *run) shardLoop(sh *shard) {
	timer := time.NewTimer(r.s.o.flushInterval)
	defer timer.Stop()
	for {
		select {
		case <-sh.signal:
		case <-timer.C:
		case <-r.drainCh:
			sh.mu.Lock()
			sh.closed = true
			rest := sh.buf
			sh.buf = nil
			sh.mu.Unlock()
			for _, d := range rest {
				r.nackAs(r.hctx, d, outcomeShutdown)
			}
			sh.load.Add(-int64(len(rest)))
			return
		}
		for {
			sh.mu.Lock()
			n := min(len(sh.buf), r.s.o.maxBatchSize)
			batch := make([]Delivery, n)
			copy(batch, sh.buf[:n])
			clear(sh.buf[:n])
			sh.buf = sh.buf[n:]
			more := len(sh.buf) >= r.s.o.maxBatchSize
			sh.mu.Unlock()
			if n > 0 {
				r.processBatch(batch)
				sh.load.Add(-int64(n))
			}
			if !more || r.draining() {
				break
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(r.s.o.flushInterval)
	}
}

// processBatch runs the batch handler with retries and settles every delivery.
func (r *run) processBatch(ds []Delivery) {
	// Nack the messages of fenced keys (a message of the key was finally
	// nacked and has not been redelivered yet), whatever batch they reach.
	live := ds[:0:0]
	for _, d := range ds {
		if !r.fences.admit(d.Message(), seqOf(d)) {
			r.nackAs(r.hctx, d, outcomeOrdering)
			continue
		}
		live = append(live, d)
	}
	if r.draining() {
		for _, d := range live {
			r.nackAs(r.hctx, d, outcomeShutdown)
		}
		return
	}
	if len(live) == 0 {
		return
	}

	msgs := make([]*Message, len(live))
	for i, d := range live {
		msgs[i] = d.Message()
	}
	ctx, span := r.s.tel.startProcess(r.hctx, msgs)
	start := time.Now()

	results := make([]error, len(msgs))
	pending := make([]int, len(msgs))
	for i := range pending {
		pending[i] = i
	}
	wholeFailed := false
	for attempt := 0; attempt <= r.s.o.maxRetries && len(pending) > 0; attempt++ {
		if attempt > 0 && !r.backoff(attempt) {
			for _, i := range pending {
				results[i] = errors.Join(errInterrupted, results[i])
			}
			pending = nil
			wholeFailed = false
			break
		}
		if attempt > 0 {
			r.s.tel.retried(ctx, msgs[0].Topic)
		}
		sub := make([]*Message, len(pending))
		for j, i := range pending {
			sub[j] = msgs[i]
		}
		err := r.withTimeout(ctx, func(ctx context.Context) error { return r.callBatch(ctx, sub) })
		var be *BatchError
		switch {
		case err == nil:
			for _, i := range pending {
				results[i] = nil
			}
			pending = nil
		case errors.As(err, &be) && be != nil:
			wholeFailed = false
			next := pending[:0:0]
			for j, i := range pending {
				e := be.Failed[j]
				results[i] = e
				if e != nil && !IsPermanent(e) {
					next = append(next, i)
				}
			}
			for j := range be.Failed {
				if j < 0 || j >= len(pending) {
					r.s.log.Warn("pubsub: BatchError index out of range", log.Int("index", j), log.Int("batch_size", len(pending)))
				}
			}
			pending = next
		case IsPermanent(err):
			for _, i := range pending {
				results[i] = err
			}
			pending = nil
		default:
			wholeFailed = true
			for _, i := range pending {
				results[i] = err
			}
		}
	}
	// Whole-batch failure after all retries: isolate the poison message.
	if wholeFailed && len(pending) > 1 {
		r.isolate(ctx, msgs, pending, results)
	}

	var firstErr error
	failedKeys := map[string]bool{}
	for i, d := range live {
		k := msgs[i].Key
		if k != "" && failedKeys[k] {
			r.fences.follow(msgs[i], seqOf(d))
			r.nackAs(ctx, d, outcomeOrdering)
			continue
		}
		if results[i] != nil && firstErr == nil {
			firstErr = results[i]
		}
		if res := r.settle(ctx, d, results[i]); res.blocked && k != "" {
			failedKeys[k] = true
		}
	}
	r.s.tel.endProcess(ctx, span, msgs[0].Topic, start, firstErr)
}

// isolate runs each message of a batch that failed as a whole once on its
// own, in batch order, to find the poison message. A message whose key had
// an earlier message fail is not run (it keeps its error and is nacked
// behind it), so per-key order holds. After isolationMaxFailures isolated
// messages in a row fail, the failure is taken as systemic (a dependency
// outage) and the rest are not run, so an outage does not multiply the load
// on the failing dependency or hold the shard for one handler timeout per
// message.
func (r *run) isolate(ctx context.Context, msgs []*Message, pending []int, results []error) {
	failedKeys := map[string]bool{}
	streak := 0
	for _, i := range pending {
		k := msgs[i].Key
		switch {
		case r.draining():
			results[i] = errors.Join(errInterrupted, results[i])
			continue
		case k != "" && failedKeys[k], streak >= isolationMaxFailures:
			if k != "" {
				failedKeys[k] = true
			}
			continue // keeps the whole-batch error: nacked, not run
		}
		err := r.withTimeout(ctx, func(ctx context.Context) error { return r.callBatch(ctx, msgs[i:i+1]) })
		var be *BatchError
		if errors.As(err, &be) && be != nil {
			err = be.Failed[0]
		}
		results[i] = err
		switch {
		case err == nil:
			streak = 0
		case IsPermanent(err):
			streak = 0 // the poison message was found
		default:
			streak++
			if k != "" {
				failedKeys[k] = true
			}
		}
	}
}

func (r *run) callBatch(ctx context.Context, ms []*Message) error {
	return r.safe(ctx, func() error { return r.s.batch(ctx, ms) })
}
