package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/kafka"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

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
	handler := func(ctx context.Context, m *pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		var n int
		fmt.Sscan(string(m.Data), &n)
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
	consume(t, seeds, 10*time.Second, func(ctx context.Context, m *pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		var n int
		fmt.Sscan(string(m.Data), &n)
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
