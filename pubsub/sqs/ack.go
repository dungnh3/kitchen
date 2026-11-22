package sqs

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/timex"
)

type ackItem struct{ handle, id string }

// acker batches acks into DeleteMessageBatch requests (up to 10 entries,
// at most ackFlush apart) run by ackConcurrency workers. send only blocks
// while its bounded queue is full.
type acker struct {
	mu      sync.Mutex
	closed  bool
	senders sync.WaitGroup
	ch      chan ackItem
	done    chan struct{}
}

func (s *Source) startAcker() *acker {
	a := &acker{ch: make(chan ackItem, 4*maxBatch*s.ackConcurrency), done: make(chan struct{})}
	jobs := make(chan []ackItem, s.ackConcurrency)
	var workers sync.WaitGroup
	for range s.ackConcurrency {
		workers.Go(func() {
			for b := range jobs {
				s.deleteBatch(b)
			}
		})
	}
	go func() {
		defer close(a.done)
		s.batchAcks(a.ch, jobs)
		close(jobs)
		workers.Wait()
	}()
	return a
}

// send queues it for deletion and reports false once the acker is closed.
func (a *acker) send(it ackItem) bool {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return false
	}
	a.senders.Add(1)
	a.mu.Unlock()
	defer a.senders.Done()
	a.ch <- it
	return true
}

// close flushes queued acks and waits for their deletes.
func (a *acker) close() {
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
	a.senders.Wait()
	close(a.ch)
	<-a.done
}

// batchAcks groups acks into batches for the delete workers until in is
// closed, then flushes what is left.
func (s *Source) batchAcks(in <-chan ackItem, jobs chan<- []ackItem) {
	var buf []ackItem
	t := time.NewTimer(s.ackFlush)
	t.Stop()
	var tick <-chan time.Time
	flush := func() {
		if len(buf) > 0 {
			jobs <- buf
			buf = nil
		}
		t.Stop()
		tick = nil
	}
	for {
		select {
		case it, ok := <-in:
			if !ok {
				flush()
				return
			}
			if len(buf) == 0 {
				t.Reset(s.ackFlush)
				tick = t.C
			}
			buf = append(buf, it)
			if len(buf) == maxBatch {
				flush()
			}
		case <-tick:
			flush()
		}
	}
}

// deleteBatch deletes items (at most 10), retrying entries that failed
// without sender fault. Messages it cannot delete are redelivered later.
func (s *Source) deleteBatch(items []ackItem) {
	pending := items
	for attempt := 1; ; attempt++ {
		entries := make([]types.DeleteMessageBatchRequestEntry, len(pending))
		for i, it := range pending {
			entries[i] = types.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(it.handle)}
		}
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
		out, err := s.api.DeleteMessageBatch(ctx, &awssqs.DeleteMessageBatchInput{QueueUrl: aws.String(s.queue), Entries: entries})
		cancel()
		var retry []ackItem
		var codes []string
		if err != nil {
			retry = pending
		} else {
			for _, fe := range out.Failed {
				i, convErr := strconv.Atoi(aws.ToString(fe.Id))
				if convErr != nil || i < 0 || i >= len(pending) {
					continue
				}
				if fe.SenderFault {
					s.log.Warn("sqs: delete refused; message will be redelivered",
						log.String("queue", s.queue), log.String("messaging.message.id", pending[i].id),
						log.String("code", aws.ToString(fe.Code)), log.String("reason", aws.ToString(fe.Message)))
					continue
				}
				retry = append(retry, pending[i])
				codes = append(codes, aws.ToString(fe.Code))
			}
		}
		if len(retry) == 0 {
			return
		}
		if attempt == deleteAttempts {
			for i, it := range retry {
				attrs := []any{log.String("queue", s.queue), log.String("messaging.message.id", it.id)}
				if err != nil {
					attrs = append(attrs, log.Err(err))
				} else {
					attrs = append(attrs, log.String("code", codes[i]))
				}
				s.log.Warn("sqs: delete failed; message will be redelivered", attrs...)
			}
			return
		}
		time.Sleep(timex.Backoff(100*time.Millisecond, 2*time.Second, attempt))
		pending = retry
	}
}
