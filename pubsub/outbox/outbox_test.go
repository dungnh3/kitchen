package outbox_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/outbox"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// memStore is a Store with the head-of-key claim semantics.
type memStore struct {
	mu   sync.Mutex
	next int64
	rows []*row
	wake chan struct{}
}

type row struct {
	rec          outbox.Record
	availableAt  time.Time
	claimedUntil time.Time
}

func (s *memStore) add(topic string, m *pubsub.Message) {
	s.mu.Lock()
	s.next++
	s.rows = append(s.rows, &row{rec: outbox.Record{ID: s.next, Topic: topic, Message: m}})
	s.mu.Unlock()
}

func (s *memStore) Claim(_ context.Context, limit int, lease time.Duration) ([]outbox.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	seen := map[string]bool{}
	var out []outbox.Record
	for _, r := range s.rows {
		k := r.rec.Message.Key
		head := k == "" || !seen[k]
		if k != "" {
			seen[k] = true
		}
		if !head || r.availableAt.After(now) || r.claimedUntil.After(now) {
			continue
		}
		r.claimedUntil = now.Add(lease)
		r.rec.Attempts++
		out = append(out, r.rec)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (s *memStore) Sent(_ context.Context, ids []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = slices.DeleteFunc(s.rows, func(r *row) bool { return slices.Contains(ids, r.rec.ID) })
	return nil
}

func (s *memStore) Failed(_ context.Context, id int64, _ error, retryAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.rec.ID == id {
			r.availableAt, r.claimedUntil = retryAt, time.Time{}
		}
	}
	return nil
}

func (s *memStore) Wake() <-chan struct{} { return s.wake }

func (s *memStore) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

// flakyPub fails the first publish of message "fail-once".
type flakyPub struct {
	mu      sync.Mutex
	got     []string
	failed  bool
	resumed []string
}

func (p *flakyPub) Publish(_ context.Context, _ string, msgs ...*pubsub.Message) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]string, len(msgs))
	failed := map[int]error{}
	for i, m := range msgs {
		if string(m.Data) == "fail-once" && !p.failed {
			p.failed = true
			failed[i] = errors.New("unavailable")
			continue
		}
		p.got = append(p.got, string(m.Data))
		ids[i] = "id"
	}
	if len(failed) > 0 {
		return ids, &pubsub.PublishError{IDs: ids, Failed: failed}
	}
	return ids, nil
}
func (p *flakyPub) Close(context.Context) error { return nil }
func (p *flakyPub) ResumeKey(_, key string) {
	p.mu.Lock()
	p.resumed = append(p.resumed, key)
	p.mu.Unlock()
}

func TestRelayKeepsKeyOrderAcrossFailures(t *testing.T) {
	st := &memStore{wake: make(chan struct{}, 1)}
	pub := &flakyPub{}
	st.add("t", &pubsub.Message{Key: "k", Data: []byte("fail-once")})
	st.add("t", &pubsub.Message{Key: "k", Data: []byte("after")})
	st.add("t", &pubsub.Message{Data: []byte("unkeyed")})

	r, err := outbox.NewRelay(st, pub,
		outbox.WithPollInterval(5*time.Millisecond),
		outbox.WithRetryBackoff(time.Millisecond, 2*time.Millisecond),
		outbox.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- r.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for st.len() > 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if st.len() != 0 {
		t.Fatalf("rows left: %d", st.len())
	}
	if i, j := slices.Index(pub.got, "fail-once"), slices.Index(pub.got, "after"); i < 0 || j < i {
		t.Fatalf("published order %v breaks key order", pub.got)
	}
	if !slices.Equal(pub.resumed, []string{"k"}) {
		t.Fatalf("resumed = %v", pub.resumed)
	}
}

func TestNewRelayValidation(t *testing.T) {
	if _, err := outbox.NewRelay(nil, &flakyPub{}); err == nil {
		t.Fatal("nil store accepted")
	}
	if _, err := outbox.NewRelay(&memStore{}, &flakyPub{}, outbox.WithBatchSize(0)); err == nil {
		t.Fatal("batch 0 accepted")
	}
}
