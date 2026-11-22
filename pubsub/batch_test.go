package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
)

func data(ms []*pubsub.Message) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = string(m.Data)
	}
	return out
}

func TestBatchFlushBySizeAndInterval(t *testing.T) {
	e := newEnv(t)
	batches := make(chan []string, 10)
	s, err := pubsub.NewBatchSubscriber(e.sub, func(_ context.Context, ms []*pubsub.Message) error {
		batches <- data(ms)
		return nil
	}, fast(pubsub.WithConcurrency(1), pubsub.WithBatch(3, 50*time.Millisecond))...)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)
	e.publish(msg("", "a"), msg("", "b"), msg("", "c"), msg("", "d"))
	if b := <-batches; !slices.Equal(b, []string{"a", "b", "c"}) {
		t.Fatalf("first batch = %v", b)
	}
	if b := <-batches; !slices.Equal(b, []string{"d"}) {
		t.Fatalf("second batch = %v", b)
	}
	eventually(t, func() bool { return e.sub.Pending() == 0 }, "pending")
}

func TestBatchPartialFailureRetriesOnlyFailed(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	var calls [][]string
	s, err := pubsub.NewBatchSubscriber(e.sub, func(_ context.Context, ms []*pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, data(ms))
		if len(calls) == 1 {
			return &pubsub.BatchError{Failed: map[int]error{2: errors.New("transient")}}
		}
		return nil
	}, fast(pubsub.WithConcurrency(1), pubsub.WithBatch(5, 20*time.Millisecond))...)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)
	e.publish(msg("", "0"), msg("", "1"), msg("", "2"), msg("", "3"), msg("", "4"))
	eventually(t, func() bool { return e.sub.Pending() == 0 }, "pending")
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 || !slices.Equal(calls[1], []string{"2"}) {
		t.Fatalf("calls = %v", calls)
	}
}

func TestBatchWholeFailureIsolatesPoison(t *testing.T) {
	e := newEnv(t, memory.WithNackDelay(time.Hour))
	var mu sync.Mutex
	var acked []string
	s, err := pubsub.NewBatchSubscriber(e.sub, func(_ context.Context, ms []*pubsub.Message) error {
		if slices.Contains(data(ms), "poison") {
			return errors.New("batch insert failed")
		}
		mu.Lock()
		acked = append(acked, data(ms)...)
		mu.Unlock()
		return nil
	}, fast(pubsub.WithConcurrency(1), pubsub.WithBatch(4, 20*time.Millisecond))...)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)
	e.publish(msg("", "a"), msg("", "poison"), msg("", "b"), msg("", "c"))
	eventually(t, func() bool { return e.sub.Pending() == 1 }, "pending %d", e.sub.Pending())
	mu.Lock()
	defer mu.Unlock()
	slices.Sort(acked)
	if !slices.Equal(acked, []string{"a", "b", "c"}) {
		t.Fatalf("acked = %v", acked)
	}
}

func TestBatchPermanentEntryDeadLetters(t *testing.T) {
	e := newEnv(t)
	dead := make(chan string, 1)
	s, err := pubsub.NewBatchSubscriber(e.sub, func(_ context.Context, _ []*pubsub.Message) error {
		return &pubsub.BatchError{Failed: map[int]error{0: pubsub.Permanent(errors.New("invalid"))}}
	}, fast(pubsub.WithConcurrency(1), pubsub.WithBatch(2, 10*time.Millisecond),
		pubsub.WithDeadLetter(func(_ context.Context, m *pubsub.Message, _ error) error {
			dead <- string(m.Data)
			return nil
		}))...)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)
	e.publish(msg("", "bad"), msg("", "good"))
	if d := <-dead; d != "bad" {
		t.Fatalf("dead-lettered %q", d)
	}
	eventually(t, func() bool { return e.sub.Pending() == 0 }, "pending")
}

func TestBatchKeyOrderAcrossShards(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	got := map[string][]int{}
	s, err := pubsub.NewBatchSubscriber(e.sub, func(_ context.Context, ms []*pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		for _, m := range ms {
			var n int
			_, _ = fmt.Sscan(string(m.Data), &n)
			got[m.Key] = append(got[m.Key], n)
		}
		return nil
	}, fast(pubsub.WithConcurrency(4), pubsub.WithBatch(7, 5*time.Millisecond))...)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)
	for i := range 30 {
		for k := range 5 {
			e.publish(msg(fmt.Sprint("k", k), fmt.Sprint(i)))
		}
	}
	eventually(t, func() bool { return e.sub.Pending() == 0 }, "pending %d", e.sub.Pending())
	mu.Lock()
	defer mu.Unlock()
	for k, seq := range got {
		if len(seq) != 30 || !slices.IsSorted(seq) {
			t.Fatalf("key %s = %v", k, seq)
		}
	}
}

func TestJSONBatch(t *testing.T) {
	type order struct {
		ID int `json:"id"`
	}
	e := newEnv(t)
	dead := make(chan string, 4)
	var mu sync.Mutex
	var seen []int
	calls := 0
	h := pubsub.JSONBatch(func(_ context.Context, vs []order, ms []*pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if len(vs) != len(ms) {
			t.Errorf("misaligned %d values, %d messages", len(vs), len(ms))
		}
		for i, v := range vs {
			if v.ID == 3 {
				// Index of the decoded slice: JSONBatch maps it back.
				return &pubsub.BatchError{Failed: map[int]error{i: pubsub.Permanent(errors.New("rejected"))}}
			}
			seen = append(seen, v.ID)
		}
		return nil
	})
	s, err := pubsub.NewBatchSubscriber(e.sub, h, fast(pubsub.WithConcurrency(1), pubsub.WithBatch(4, 10*time.Millisecond),
		pubsub.WithDeadLetter(func(_ context.Context, m *pubsub.Message, _ error) error {
			dead <- string(m.Data)
			return nil
		}))...)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)
	e.publish(msg("", `{"id":1}`), msg("", `not json`), msg("", `{"id":2}`), msg("", `{"id":3}`))
	got := []string{<-dead, <-dead}
	slices.Sort(got)
	if !slices.Equal(got, []string{`not json`, `{"id":3}`}) {
		t.Fatalf("dead-lettered %v", got)
	}
	eventually(t, func() bool { return e.sub.Pending() == 0 }, "pending")
	mu.Lock()
	defer mu.Unlock()
	if !slices.Contains(seen, 1) || !slices.Contains(seen, 2) {
		t.Fatalf("seen %v", seen)
	}
}
