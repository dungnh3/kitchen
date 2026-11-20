package pubsub

import (
	"context"
	"errors"
	"hash/maphash"
	"log/slog"
	"sync"
	"time"
)

type shard struct {
	mu     sync.Mutex
	buf    []Delivery
	closed bool
	signal chan struct{}
	// failed holds keys with a nacked message; later messages of these keys
	// are nacked so the broker redelivers the key in order. Only the shard
	// goroutine touches it.
	failed map[string]bool
}

func (r *run) dispatchBatch(d Delivery) {
	var sh *shard
	if k := d.Message().Key; k != "" {
		sh = r.shards[maphash.String(r.seed, k)%uint64(len(r.shards))]
	} else {
		sh = r.shards[r.rr.Add(1)%uint64(len(r.shards))]
	}
	sh.mu.Lock()
	if sh.closed {
		sh.mu.Unlock()
		d.Nack()
		return
	}
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
				d.Nack()
			}
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
				r.processBatch(sh, batch)
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
func (r *run) processBatch(sh *shard, ds []Delivery) {
	// Nack messages of keys that failed earlier; forget keys absent from this
	// batch (their failed messages have been nacked and will be redelivered).
	seen := map[string]bool{}
	live := ds[:0:0]
	for _, d := range ds {
		k := d.Message().Key
		if k != "" {
			seen[k] = true
			if sh.failed[k] {
				d.Nack()
				continue
			}
		}
		live = append(live, d)
	}
	for k := range sh.failed {
		if !seen[k] {
			delete(sh.failed, k)
		}
	}
	if r.draining() {
		for _, d := range live {
			d.Nack()
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
					r.s.log.Warn("pubsub: BatchError index out of range", slog.Int("index", j), slog.Int("batch_size", len(pending)))
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
	// Whole-batch failure after all retries: run each message once alone to
	// isolate the poison message.
	if wholeFailed && len(pending) > 1 {
		for _, i := range pending {
			if r.draining() {
				results[i] = errors.Join(errInterrupted, results[i])
				continue
			}
			err := r.withTimeout(ctx, func(ctx context.Context) error { return r.callBatch(ctx, msgs[i:i+1]) })
			var be *BatchError
			if errors.As(err, &be) && be != nil {
				err = be.Failed[0]
			}
			results[i] = err
		}
	}

	var firstErr error
	failedKeys := map[string]bool{}
	for i, d := range live {
		k := msgs[i].Key
		if k != "" && failedKeys[k] {
			d.Nack()
			continue
		}
		if results[i] != nil && firstErr == nil {
			firstErr = results[i]
		}
		if !r.settle(ctx, d, results[i]) && k != "" {
			failedKeys[k] = true
		}
	}
	for k := range failedKeys {
		sh.failed[k] = true
	}
	r.s.tel.endProcess(ctx, span, msgs[0].Topic, start, firstErr)
}

func (r *run) callBatch(ctx context.Context, ms []*Message) error {
	return r.safe(func() error { return r.s.batch(ctx, ms) })
}
