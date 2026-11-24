package sqs_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/sqs"
)

// apiErr is an AWS API error as the SDK reports it (smithy.APIError).
type apiErr struct{ code string }

func (e apiErr) Error() string     { return e.code + ": test" }
func (e apiErr) ErrorCode() string { return e.code }

func publish(t *testing.T, f *fakeSQS, queue string, msgs ...*pubsub.Message) []string {
	t.Helper()
	pub, err := sqs.NewPublisher(f)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pub.Publish(context.Background(), queue, msgs...)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	return ids
}

// drive runs a Subscriber inside the current synctest bubble until until
// reports true or limit (fake time) passes, then cancels and waits for Run.
func drive(t *testing.T, f *fakeSQS, queue string, h pubsub.Handler, until func() bool, limit time.Duration,
	srcOpts []sqs.SourceOption, subOpts ...pubsub.Option,
) {
	t.Helper()
	srcOpts = append([]sqs.SourceOption{sqs.WithWaitTime(time.Second), sqs.WithSourceLogger(quiet)}, srcOpts...)
	src, err := sqs.NewSource(f, queue, srcOpts...)
	if err != nil {
		t.Fatal(err)
	}
	subOpts = append([]pubsub.Option{pubsub.WithLogger(quiet), pubsub.WithRetry(0, 0, 0)}, subOpts...)
	s, err := pubsub.NewSubscriber(src, h, subOpts...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- s.Run(ctx) }()
	deadline := time.Now().Add(limit)
	step := max(10*time.Millisecond, limit/10000)
	for !until() && time.Now().Before(deadline) {
		time.Sleep(step)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Messages of one ReceiveMessage response waiting behind a blocked fn must
// have their visibility extended too.
func TestHeldMessagesAreExtended(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const q = "https://sqs/q"
		f := newFake(false)
		for i := range 3 {
			publish(t, f, q, &pubsub.Message{Data: []byte(fmt.Sprint(i))})
		}
		var mu sync.Mutex
		calls := map[string]int{}
		drive(t, f, q, func(_ context.Context, m *pubsub.Message) error {
			mu.Lock()
			calls[string(m.Data)]++
			mu.Unlock()
			time.Sleep(5 * time.Second) // slow handler, engine saturated
			return nil
		}, func() bool { return f.len() == 0 }, 40*time.Second,
			[]sqs.SourceOption{sqs.WithVisibilityTimeout(2 * time.Second), sqs.WithPollers(2)},
			pubsub.WithMaxInFlight(1), pubsub.WithConcurrency(1))
		mu.Lock()
		defer mu.Unlock()
		for k, n := range calls {
			if n != 1 {
				t.Errorf("message %s handled %d times; its visibility expired while the poller held it", k, n)
			}
		}
		if len(calls) != 3 {
			t.Errorf("handled %v, want 3 messages", calls)
		}
	})
}

// Valid UTF-8 outside the SQS character set is base64-encoded and decoded
// back.
func TestBodyOutsideSQSCharsetRoundTrips(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"protobuf varint", []byte{0x08, 0x01}},
		{"NUL", []byte("a\x00b")},
		{"U+FFFF", []byte("￿")},
		{"U+FFFE", []byte("x￾")},
		{"C0 control", []byte("bell\a")},
		{"invalid UTF-8", []byte{0xff, 0x00, 0xfe}},
		{"plain text", []byte("héllo\tworld\r\n😀")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const q = "https://sqs/q"
				f := newFake(false)
				publish(t, f, q, &pubsub.Message{Data: tc.data})
				var mu sync.Mutex
				var got []byte
				drive(t, f, q, func(_ context.Context, m *pubsub.Message) error {
					mu.Lock()
					got = m.Data
					mu.Unlock()
					if m.Attributes[sqs.EncodingAttribute] != "" {
						t.Errorf("encoding attribute leaked to the handler")
					}
					return nil
				}, func() bool { return f.len() == 0 }, 10*time.Second, nil)
				mu.Lock()
				defer mu.Unlock()
				if string(got) != string(tc.data) {
					t.Fatalf("received %q, want %q", got, tc.data)
				}
			})
		})
	}
}

// SendMessageBatch requests stay within the 1 MiB total, attributes
// included; one oversized message fails alone, permanently.
func TestPublishSplitsBatchBySize(t *testing.T) {
	big := func(n int) []byte { return []byte(strings.Repeat("x", n)) }
	attr := map[string]string{"pad": strings.Repeat("a", 20_000)}
	cases := []struct {
		name      string
		msgs      []*pubsub.Message
		permanent []int // indexes that must fail permanently
	}{
		{"ten 200 KiB bodies", func() (ms []*pubsub.Message) {
			for range 10 {
				ms = append(ms, &pubsub.Message{Data: big(200 << 10)})
			}
			return ms
		}(), nil},
		{"attributes count", func() (ms []*pubsub.Message) {
			for range 4 {
				ms = append(ms, &pubsub.Message{Data: big(250_000), Attributes: attr})
			}
			return ms
		}(), nil},
		{"one oversized message", []*pubsub.Message{
			{Data: []byte("a")}, {Data: big(maxRequest + 1)}, {Data: []byte("b")},
		}, []int{1}},
		{"oversized by attributes", []*pubsub.Message{
			{Data: big(maxRequest - 100), Attributes: map[string]string{"k": strings.Repeat("v", 200)}}, {Data: []byte("b")},
		}, []int{0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(false)
			pub, _ := sqs.NewPublisher(f)
			ids, err := pub.Publish(context.Background(), "https://sqs/q", tc.msgs...)
			failed := map[int]error{}
			if err != nil {
				var pe *pubsub.PublishError
				if errors.As(err, &pe) {
					failed = pe.Failed
				} else {
					failed[0] = err
				}
			}
			if len(failed) != len(tc.permanent) {
				t.Fatalf("failed = %v, want only %v", failed, tc.permanent)
			}
			for _, i := range tc.permanent {
				if !pubsub.IsPermanent(failed[i]) || !errors.Is(failed[i], errs.ErrInvalidArgument) {
					t.Errorf("message %d: %v, want permanent InvalidArgument", i, failed[i])
				}
			}
			for i, id := range ids {
				if _, bad := failed[i]; !bad && id == "" {
					t.Errorf("message %d has no id", i)
				}
			}
		})
	}
}

// SenderFault and transport failures are classified per message.
func TestPublishErrorClassification(t *testing.T) {
	cases := []struct {
		name        string
		setup       func(f *fakeSQS)
		permanent   bool
		unavailable bool
	}{
		{"sender fault", func(f *fakeSQS) {
			f.sendFailed = func(string) *types.BatchResultErrorEntry {
				return &types.BatchResultErrorEntry{Code: aws.String("InvalidParameterValue"), Message: aws.String("bad"), SenderFault: true}
			}
		}, true, false},
		{"broker fault", func(f *fakeSQS) {
			f.sendFailed = func(string) *types.BatchResultErrorEntry {
				return &types.BatchResultErrorEntry{Code: aws.String("InternalError"), Message: aws.String("oops")}
			}
		}, false, true},
		{"request failed", func(f *fakeSQS) { f.failSends = 1 }, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(false)
			tc.setup(f)
			pub, _ := sqs.NewPublisher(f)
			_, err := pub.Publish(context.Background(), "https://sqs/q", &pubsub.Message{Data: []byte("a")}, &pubsub.Message{Data: []byte("b")})
			var pe *pubsub.PublishError
			if !errors.As(err, &pe) || len(pe.Failed) != 2 {
				t.Fatalf("err = %v", err)
			}
			for i, e := range pe.Failed {
				if got := pubsub.IsPermanent(e); got != tc.permanent {
					t.Errorf("message %d permanent = %v, want %v (%v)", i, got, tc.permanent, e)
				}
				if got := errors.Is(e, errs.ErrDependencyUnavailable); got != tc.unavailable {
					t.Errorf("message %d unavailable = %v, want %v (%v)", i, got, tc.unavailable, e)
				}
			}
		})
	}
}

// Acks are deleted by several DeleteMessageBatch requests in parallel.
func TestAcksDeleteConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const q = "https://sqs/q"
		f := newFake(false)
		var mu sync.Mutex
		cur, peak := 0, 0
		f.deleteHook = func([]types.DeleteMessageBatchRequestEntry) ([]types.BatchResultErrorEntry, error) {
			mu.Lock()
			cur++
			peak = max(peak, cur)
			mu.Unlock()
			time.Sleep(20 * time.Millisecond) // in-region latency
			mu.Lock()
			cur--
			mu.Unlock()
			return nil, nil
		}
		var msgs []*pubsub.Message
		for i := range 300 {
			msgs = append(msgs, &pubsub.Message{Data: []byte(fmt.Sprint(i))})
		}
		publish(t, f, q, msgs...)
		drive(t, f, q, func(context.Context, *pubsub.Message) error { return nil },
			func() bool { return f.len() == 0 }, 20*time.Second,
			[]sqs.SourceOption{sqs.WithPollers(4)}, pubsub.WithConcurrency(100))
		if f.len() != 0 {
			t.Fatalf("%d messages not deleted", f.len())
		}
		mu.Lock()
		defer mu.Unlock()
		if peak < 2 {
			t.Fatalf("max concurrent DeleteMessageBatch = %d, want parallel deletes", peak)
		}
	})
}

// A delete entry that fails without sender fault is retried.
func TestDeleteRetriesTransientEntryFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const q = "https://sqs/q"
		f := newFake(false)
		var mu sync.Mutex
		seen := map[string]bool{}
		f.deleteHook = func(entries []types.DeleteMessageBatchRequestEntry) ([]types.BatchResultErrorEntry, error) {
			mu.Lock()
			defer mu.Unlock()
			var failed []types.BatchResultErrorEntry
			for _, e := range entries {
				if h := aws.ToString(e.ReceiptHandle); !seen[h] {
					seen[h] = true
					failed = append(failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("InternalError"), SenderFault: false})
				}
			}
			return failed, nil
		}
		publish(t, f, q, &pubsub.Message{Data: []byte("a")}, &pubsub.Message{Data: []byte("b")}, &pubsub.Message{Data: []byte("c")})
		var handled atomic.Int32
		drive(t, f, q, func(context.Context, *pubsub.Message) error { handled.Add(1); return nil },
			func() bool { return f.len() == 0 }, 10*time.Second, nil) // visibility 30s: no redelivery in the window
		if f.len() != 0 || handled.Load() != 3 {
			t.Fatalf("queue len = %d, handled = %d; transient delete failures were not retried", f.len(), handled.Load())
		}
	})
}

// Receive flushes pending deletes before returning.
func TestReceiveFlushesDeletesBeforeReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const q = "https://sqs/q"
		f := newFake(false)
		publish(t, f, q, &pubsub.Message{Data: []byte("a")}, &pubsub.Message{Data: []byte("b")})
		var handled atomic.Int32
		drive(t, f, q, func(context.Context, *pubsub.Message) error { handled.Add(1); return nil },
			func() bool { return handled.Load() == 2 }, 10*time.Second,
			[]sqs.SourceOption{sqs.WithAckFlushInterval(time.Hour)})
		if f.len() != 0 {
			t.Fatalf("%d acked messages not deleted when Receive returned", f.len())
		}
	})
}

// During shutdown, visibility is extended until every in-flight message is
// settled, even past one visibility timeout.
func TestShutdownKeepsExtendingUntilSettled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const q = "https://sqs/q"
		f := newFake(false)
		publish(t, f, q, &pubsub.Message{Data: []byte("x")})
		src, _ := sqs.NewSource(f, q, sqs.WithWaitTime(time.Second), sqs.WithVisibilityTimeout(4*time.Second), sqs.WithSourceLogger(quiet))
		started := make(chan struct{})
		visible := false
		s, _ := pubsub.NewSubscriber(src, func(context.Context, *pubsub.Message) error {
			close(started)
			for range 20 { // 20s of work, inside the 60s shutdown timeout
				time.Sleep(time.Second)
				f.mu.Lock()
				if !f.msgs[0].visibleAt.After(time.Now()) {
					visible = true
				}
				f.mu.Unlock()
			}
			return nil
		}, pubsub.WithLogger(quiet), pubsub.WithShutdownTimeout(60*time.Second), pubsub.WithHandlerTimeout(0))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = s.Run(ctx); close(done) }()
		<-started
		cancel()
		<-done
		if visible {
			t.Error("message became visible while its handler was still running in the drain")
		}
		if f.len() != 0 {
			t.Errorf("queue len = %d after drain, want 0", f.len())
		}
	})
}

// A receipt that SQS reports invalid is not extended again.
func TestHeartbeatDropsInvalidReceipts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const q = "https://sqs/q"
		f := newFake(false)
		publish(t, f, q, &pubsub.Message{Data: []byte("x")})
		var mu sync.Mutex
		tries := map[string]int{}
		f.cmvbHook = func(in *awssqs.ChangeMessageVisibilityBatchInput) ([]types.BatchResultErrorEntry, error) {
			mu.Lock()
			defer mu.Unlock()
			var failed []types.BatchResultErrorEntry
			for _, e := range in.Entries {
				tries[aws.ToString(e.ReceiptHandle)]++
				failed = append(failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("ReceiptHandleIsInvalid"), SenderFault: true})
			}
			return failed, nil
		}
		// Ticks every 10s; the handler spans three of them (the message is
		// also redelivered after 20s, under a new receipt handle).
		drive(t, f, q, func(context.Context, *pubsub.Message) error { time.Sleep(35 * time.Second); return nil },
			func() bool { return f.len() == 0 }, 50*time.Second,
			[]sqs.SourceOption{sqs.WithVisibilityTimeout(20 * time.Second)})
		mu.Lock()
		defer mu.Unlock()
		for h, n := range tries {
			if n != 1 {
				t.Errorf("invalid receipt %s extended %d times, want 1", h, n)
			}
		}
		if len(tries) == 0 {
			t.Fatal("no extension attempted")
		}
	})
}

// A failed extension is retried before the next tick.
func TestHeartbeatRetriesFailedCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const q = "https://sqs/q"
		f := newFake(false)
		publish(t, f, q, &pubsub.Message{Data: []byte("x")})
		start := time.Now()
		var mu sync.Mutex
		var calls []time.Duration
		f.cmvbHook = func(*awssqs.ChangeMessageVisibilityBatchInput) ([]types.BatchResultErrorEntry, error) {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, time.Since(start))
			if len(calls) == 1 {
				return nil, errors.New("throttled")
			}
			return nil, nil
		}
		drive(t, f, q, func(context.Context, *pubsub.Message) error { time.Sleep(12 * time.Second); return nil },
			func() bool { return f.len() == 0 }, 20*time.Second,
			[]sqs.SourceOption{sqs.WithVisibilityTimeout(10 * time.Second)})
		mu.Lock()
		defer mu.Unlock()
		if len(calls) < 2 || calls[1] >= 9*time.Second {
			t.Fatalf("extension calls at %v; the failed one was not retried before the message expired", calls)
		}
	})
}

// Extensions never exceed 12 hours from receipt.
func TestHeartbeatStopsAt12Hours(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const q = "https://sqs/q"
		f := newFake(false)
		publish(t, f, q, &pubsub.Message{Data: []byte("x")})
		var receives atomic.Int32
		f.receiveHook = func(ctx context.Context, _ *awssqs.ReceiveMessageInput) error {
			if receives.Add(1) > 1 {
				<-ctx.Done() // one receive only: no busy long polls for 13 hours
				return ctx.Err()
			}
			return nil
		}
		drive(t, f, q, func(context.Context, *pubsub.Message) error {
			time.Sleep(13 * time.Hour)
			return nil
		}, func() bool { return f.len() == 0 }, 14*time.Hour,
			[]sqs.SourceOption{sqs.WithVisibilityTimeout(4 * time.Hour)})
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.visRejected > 0 {
			t.Fatalf("%d visibility extensions went past 12 hours from receipt", f.visRejected)
		}
	})
}

// A heartbeat in flight while the message is nacked must not hide it again.
func TestHeartbeatDoesNotUndoNack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const q = "https://sqs/q"
		f := newFake(false)
		publish(t, f, q, &pubsub.Message{Data: []byte("x")})
		var receives atomic.Int32
		f.receiveHook = func(ctx context.Context, _ *awssqs.ReceiveMessageInput) error {
			if receives.Add(1) > 1 {
				<-ctx.Done() // keep the nacked message in the queue for inspection
				return ctx.Err()
			}
			return nil
		}
		inHB, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		f.cmvbHook = func(*awssqs.ChangeMessageVisibilityBatchInput) ([]types.BatchResultErrorEntry, error) {
			once.Do(func() { close(inHB); <-release })
			return nil, nil
		}
		src, _ := sqs.NewSource(f, q, sqs.WithWaitTime(time.Second), sqs.WithVisibilityTimeout(10*time.Second), sqs.WithSourceLogger(quiet))
		got := make(chan pubsub.Delivery, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error)
		go func() { done <- src.Receive(ctx, func(_ context.Context, d pubsub.Delivery) { got <- d }) }()
		d := <-got
		<-inHB
		d.Nack()
		close(release)
		synctest.Wait()
		f.mu.Lock()
		hidden := f.msgs[0].visibleAt.After(time.Now())
		f.mu.Unlock()
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if hidden {
			t.Fatal("nacked message hidden again by a concurrent heartbeat")
		}
	})
}

// Fatal receive errors end Receive; transient ones are retried.
func TestReceiveErrors(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		fatal bool
	}{
		{"queue does not exist", &types.QueueDoesNotExist{Message: aws.String("gone")}, true},
		{"legacy non-existent queue", apiErr{"AWS.SimpleQueueService.NonExistentQueue"}, true},
		{"access denied", apiErr{"AccessDenied"}, true},
		{"invalid address", &types.InvalidAddress{}, true},
		{"throttled", &types.RequestThrottled{}, false},
		{"network", errors.New("connection reset"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFake(false)
				var calls atomic.Int32
				f.receiveHook = func(context.Context, *awssqs.ReceiveMessageInput) error {
					calls.Add(1)
					return tc.err
				}
				src, _ := sqs.NewSource(f, "https://sqs/q", sqs.WithSourceLogger(quiet))
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				err := src.Receive(ctx, func(context.Context, pubsub.Delivery) {})
				if tc.fatal {
					if err == nil || !errors.Is(err, tc.err) || calls.Load() != 1 {
						t.Fatalf("err = %v after %d calls, want %v at once", err, calls.Load(), tc.err)
					}
					return
				}
				if err != nil || calls.Load() < 3 {
					t.Fatalf("err = %v after %d calls, want retries until ctx is done", err, calls.Load())
				}
			})
		})
	}
}

// FIFO receives carry a ReceiveRequestAttemptId, reused only to retry a
// failed call.
func TestFIFOReceiveRequestAttemptID(t *testing.T) {
	for _, fifo := range []bool{true, false} {
		t.Run(fmt.Sprint("fifo=", fifo), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := "https://sqs/q"
				if fifo {
					q += ".fifo"
				}
				f := newFake(fifo)
				var mu sync.Mutex
				var ids []string
				f.receiveHook = func(_ context.Context, in *awssqs.ReceiveMessageInput) error {
					mu.Lock()
					defer mu.Unlock()
					ids = append(ids, aws.ToString(in.ReceiveRequestAttemptId))
					if len(ids) == 1 {
						return errors.New("connection reset")
					}
					return nil
				}
				src, _ := sqs.NewSource(f, q, sqs.WithWaitTime(time.Second), sqs.WithSourceLogger(quiet))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := src.Receive(ctx, func(context.Context, pubsub.Delivery) {}); err != nil {
					t.Fatal(err)
				}
				mu.Lock()
				defer mu.Unlock()
				if len(ids) < 3 {
					t.Fatalf("only %d receives", len(ids))
				}
				if !fifo {
					for _, id := range ids {
						if id != "" {
							t.Fatalf("standard queue sent ReceiveRequestAttemptId %q", id)
						}
					}
					return
				}
				if ids[0] == "" || ids[1] != ids[0] || ids[2] == ids[1] || ids[2] == "" {
					t.Fatalf("attempt ids = %q; want a retry to reuse the id and a new receive to get a fresh one", ids[:3])
				}
			})
		})
	}
}

// The Source reports SQS redelivery: no key fence, attempts from the
// redrive policy.
func TestRedeliveryAdvisor(t *testing.T) {
	cases := []struct {
		name    string
		queue   string
		redrive string
		attrErr error
		want    pubsub.Redelivery
	}{
		{"fifo string count", "https://sqs/q.fifo", `{"deadLetterTargetArn":"arn:aws:sqs:r:1:dlq.fifo","maxReceiveCount":"5"}`, nil, pubsub.Redelivery{MaxDeliveryAttempts: 5}},
		{"standard numeric count", "https://sqs/q", `{"deadLetterTargetArn":"arn:aws:sqs:r:1:dlq","maxReceiveCount":7}`, nil, pubsub.Redelivery{MaxDeliveryAttempts: 7}},
		{"no redrive policy", "https://sqs/q.fifo", "", nil, pubsub.Redelivery{}},
		{"unreadable", "https://sqs/q.fifo", "", apiErr{"AccessDenied"}, pubsub.Redelivery{}},
		{"malformed", "https://sqs/q", `{"maxReceiveCount":"many"}`, nil, pubsub.Redelivery{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFake(false)
				f.redrive, f.attrErr = tc.redrive, tc.attrErr
				src, _ := sqs.NewSource(f, tc.queue, sqs.WithWaitTime(time.Second), sqs.WithSourceLogger(quiet))
				adv, ok := any(src).(pubsub.RedeliveryAdvisor)
				if !ok {
					t.Fatal("Source does not implement pubsub.RedeliveryAdvisor")
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error)
				go func() { done <- src.Receive(ctx, func(context.Context, pubsub.Delivery) {}) }()
				time.Sleep(10 * time.Millisecond)
				synctest.Wait()
				got := adv.Redelivery()
				cancel()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if got != tc.want {
					t.Fatalf("Redelivery() = %+v, want %+v", got, tc.want)
				}
			})
		})
	}
}

// A FIFO request split by size that fails pauses the key before the next
// message of that key is sent.
func TestFIFOSizeSplitKeepsKeyOrder(t *testing.T) {
	const q = "https://sqs/q.fifo"
	f := newFake(true)
	f.failSends = 1
	pub, _ := sqs.NewPublisher(f)
	body := []byte(strings.Repeat("x", 600_000))
	_, err := pub.Publish(context.Background(), q,
		&pubsub.Message{Key: "g", Data: body}, &pubsub.Message{Key: "g", Data: body})
	var pe *pubsub.PublishError
	if !errors.As(err, &pe) || len(pe.Failed) != 2 {
		t.Fatalf("err = %v", err)
	}
	if !errors.Is(pe.Failed[1], pubsub.ErrKeyPaused) {
		t.Fatalf("second message: %v, want ErrKeyPaused", pe.Failed[1])
	}
	if f.len() != 0 {
		t.Fatalf("%d messages sent after their key failed", f.len())
	}
}

func TestNewSourceValidatesOptions(t *testing.T) {
	cases := []struct {
		name string
		opt  sqs.SourceOption
		ok   bool
	}{
		{"defaults", func(*sqs.Source) {}, true},
		{"ack concurrency 1", sqs.WithAckConcurrency(1), true},
		{"ack concurrency 0", sqs.WithAckConcurrency(0), false},
		{"ack concurrency negative", sqs.WithAckConcurrency(-1), false},
		{"visibility too short", sqs.WithVisibilityTimeout(time.Second), false},
		{"visibility over 12h", sqs.WithVisibilityTimeout(13 * time.Hour), false},
		{"wait over 20s", sqs.WithWaitTime(21 * time.Second), false},
		{"no pollers", sqs.WithPollers(0), false},
		{"zero ack flush", sqs.WithAckFlushInterval(0), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqs.NewSource(newFake(false), "https://sqs/q", tc.opt)
			if (err == nil) != tc.ok {
				t.Fatalf("err = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}
