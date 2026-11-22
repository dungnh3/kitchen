package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/kafka"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var quiet = log.Discard()

func cluster(t *testing.T) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "orders"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func publish(t *testing.T, seeds []string, from, n int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pub, err := kafka.NewPublisher(cl)
	if err != nil {
		t.Fatal(err)
	}
	var msgs []*pubsub.Message
	for i := from; i < from+n; i++ {
		msgs = append(msgs, &pubsub.Message{Key: fmt.Sprint("k", i%4), Data: []byte(fmt.Sprint(i)), Attributes: map[string]string{"n": fmt.Sprint(i)}})
	}
	ids, err := pub.Publish(context.Background(), "orders", msgs...)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(ids, "") {
		t.Fatalf("ids = %v", ids)
	}
	if err := pub.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// consume runs a subscriber on group "billing" until stop returns true or the
// timeout, then shuts it down.
func consume(t *testing.T, seeds []string, timeout time.Duration, h pubsub.Handler, stop func() bool) {
	t.Helper()
	src, err := kafka.NewSource("billing", []string{"orders"},
		kafka.WithClientOptions(kgo.SeedBrokers(seeds...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())),
		kafka.WithCommitInterval(20*time.Millisecond),
		kafka.WithRedeliveryBackoff(5*time.Millisecond, 10*time.Millisecond),
		kafka.WithRevokeTimeout(2*time.Second),
		kafka.WithLogger(quiet))
	if err != nil {
		t.Fatal(err)
	}
	s, err := pubsub.NewSubscriber(src, h, pubsub.WithLogger(quiet), pubsub.WithRetry(0, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- s.Run(ctx) }()
	deadline := time.Now().Add(timeout)
	for !stop() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestConsumeInKeyOrderWithRedeliveryAndCommit(t *testing.T) {
	seeds := cluster(t)
	publish(t, seeds, 0, 40)

	var mu sync.Mutex
	got := map[string][]int{}
	count := 0
	failed := false
	handler := func(_ context.Context, m *pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		var n int
		_, _ = fmt.Sscan(string(m.Data), &n)
		if n == 5 && !failed {
			failed = true
			return errors.New("transient") // nack -> in-process redelivery
		}
		if m.Attributes["n"] != fmt.Sprint(n) {
			t.Errorf("headers lost: %v", m.Attributes)
		}
		if n == 5 && m.DeliveryAttempt != 2 {
			t.Errorf("redelivery attempt = %d", m.DeliveryAttempt)
		}
		got[m.Key] = append(got[m.Key], n)
		count++
		return nil
	}
	consume(t, seeds, 10*time.Second, handler, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count == 40
	})
	mu.Lock()
	if count != 40 {
		t.Fatalf("handled %d of 40", count)
	}
	for k, seq := range got {
		if !slices.IsSorted(seq) {
			t.Fatalf("key %s out of order: %v", k, seq)
		}
	}
	mu.Unlock()

	// Offsets were committed: a new member of the group sees only new records.
	publish(t, seeds, 40, 4)
	var again []int
	consume(t, seeds, 10*time.Second, func(_ context.Context, m *pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		var n int
		_, _ = fmt.Sscan(string(m.Data), &n)
		again = append(again, n)
		return nil
	}, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(again) >= 4
	})
	mu.Lock()
	defer mu.Unlock()
	slices.Sort(again)
	if !slices.Equal(again, []int{40, 41, 42, 43}) {
		t.Fatalf("after restart got %v, want only the new records", again)
	}
}

// receiveRaw runs src.Receive with fn until the returned stop is called.
func receiveRaw(t *testing.T, src *kafka.Source, fn func(context.Context, pubsub.Delivery)) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- src.Receive(ctx, fn) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestPartitionPausedAtMaxPending(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "orders"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	seeds := c.ListenAddrs()
	publish(t, seeds, 0, 30)

	src, err := kafka.NewSource("billing", []string{"orders"},
		kafka.WithClientOptions(kgo.SeedBrokers(seeds...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchMaxWait(50*time.Millisecond)),
		kafka.WithPollRecords(1), kafka.WithMaxPendingPerPartition(5),
		kafka.WithRevokeTimeout(time.Second), kafka.WithLogger(quiet))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []pubsub.Delivery
	seen := map[string]bool{}
	stop := receiveRaw(t, src, func(_ context.Context, d pubsub.Delivery) {
		mu.Lock()
		defer mu.Unlock()
		seen[string(d.Message().Data)] = true
		held = append(held, d)
	})
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(held) }
	waitFor := func(cond func() bool, msg string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatal(msg)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor(func() bool { return count() >= 5 }, "first records not delivered")
	time.Sleep(300 * time.Millisecond)
	if n := count(); n != 5 {
		t.Fatalf("delivered %d records while paused, want 5", n)
	}
	// Settling everything resumes the partition; nothing is skipped.
	acked := 0
	waitFor(func() bool {
		mu.Lock()
		for _, d := range held[acked:] {
			d.Ack()
		}
		acked = len(held)
		n := len(seen)
		mu.Unlock()
		return n == 30
	}, "not every record was delivered after resume")
	stop()
}

func TestCommitRetriesPartitionError(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "orders"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	seeds := c.ListenAddrs()
	publish(t, seeds, 0, 1)

	var mu sync.Mutex
	commits := 0
	// Fail the first OffsetCommit with a per-partition error code (the request
	// itself succeeds); let later commits through.
	c.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		mu.Lock()
		defer mu.Unlock()
		commits++
		if commits > 1 {
			return nil, nil, false
		}
		r := req.(*kmsg.OffsetCommitRequest)
		resp := r.ResponseKind().(*kmsg.OffsetCommitResponse)
		for _, rt := range r.Topics {
			st := kmsg.NewOffsetCommitResponseTopic()
			st.Topic, st.TopicID = rt.Topic, rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewOffsetCommitResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.RebalanceInProgress.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	src, err := kafka.NewSource("billing", []string{"orders"},
		kafka.WithClientOptions(kgo.SeedBrokers(seeds...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())),
		kafka.WithCommitInterval(20*time.Millisecond), kafka.WithRevokeTimeout(time.Second), kafka.WithLogger(quiet))
	if err != nil {
		t.Fatal(err)
	}
	stop := receiveRaw(t, src, func(_ context.Context, d pubsub.Delivery) { d.Ack() })
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := commits
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("offset not committed again after a partition error (commits=%d)", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop()
}
