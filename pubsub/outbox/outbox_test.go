package outbox_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/outbox"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// memStore is a Store with the semantics of the pgx store: it claims the run
// of consecutive due records of a key (same topic, starting at the oldest
// live record of the key), and settle calls fail once their ctx is done, like
// database calls do.
type memStore struct {
	mu     sync.Mutex
	next   int64
	rows   []*row
	wake   chan struct{}
	maxRun int // longest run claimed per key; 0 means no limit
}

type row struct {
	rec          outbox.Record
	availableAt  time.Time
	claimedUntil time.Time
	dead         bool
	deleted      bool
	lastErr      string
}

func (s *memStore) add(topic string, m *pubsub.Message) {
	s.mu.Lock()
	s.next++
	s.rows = append(s.rows, &row{rec: outbox.Record{ID: s.next, Topic: topic, Message: m}})
	s.mu.Unlock()
}

func (s *memStore) Claim(ctx context.Context, limit int, lease time.Duration) ([]outbox.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	type keyState struct {
		topic   string
		n       int
		blocked bool
	}
	keys := map[string]*keyState{}
	var out []outbox.Record
	for _, r := range s.rows {
		if r.dead {
			continue
		}
		due := !r.availableAt.After(now) && !r.claimedUntil.After(now)
		if k := r.rec.Message.Key; k != "" {
			ks := keys[k]
			if ks == nil {
				ks = &keyState{topic: r.rec.Topic}
				keys[k] = ks
			}
			if ks.blocked || !due || r.rec.Topic != ks.topic || (s.maxRun > 0 && ks.n == s.maxRun) {
				ks.blocked = true
				continue
			}
			ks.n++
		} else if !due {
			continue
		}
		r.claimedUntil = now.Add(lease)
		r.rec.Attempts++
		r.rec.Claim++
		rec := r.rec
		rec.Message = r.rec.Message.Clone()
		out = append(out, rec)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// settle applies fn to the rows of refs whose claim is current and reports
// the others as lost, like the pgx store.
func (s *memStore) settle(ctx context.Context, refs []outbox.Ref, fn func(r *row)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var lost []int64
	for _, ref := range refs {
		i := slices.IndexFunc(s.rows, func(r *row) bool { return r.rec.ID == ref.ID })
		if i < 0 || s.rows[i].rec.Claim != ref.Claim {
			lost = append(lost, ref.ID)
			continue
		}
		fn(s.rows[i])
	}
	s.rows = slices.DeleteFunc(s.rows, func(r *row) bool { return r.deleted })
	if len(lost) > 0 {
		return &outbox.ClaimLostError{IDs: lost}
	}
	return nil
}

func (s *memStore) Sent(ctx context.Context, refs []outbox.Ref) error {
	return s.settle(ctx, refs, func(r *row) { r.deleted = true })
}

func (s *memStore) Failed(ctx context.Context, ref outbox.Ref, cause error, retryAt time.Time) error {
	return s.settle(ctx, []outbox.Ref{ref}, func(r *row) {
		r.availableAt, r.claimedUntil, r.lastErr = retryAt, time.Time{}, cause.Error()
	})
}

func (s *memStore) Release(ctx context.Context, refs []outbox.Ref, cause error, retryAt time.Time) error {
	return s.settle(ctx, refs, func(r *row) {
		r.availableAt, r.claimedUntil = retryAt, time.Time{}
		r.rec.Attempts--
		if cause != nil {
			r.lastErr = cause.Error()
		}
	})
}

func (s *memStore) Dead(ctx context.Context, ref outbox.Ref, cause error) error {
	return s.settle(ctx, []outbox.Ref{ref}, func(r *row) {
		r.dead, r.claimedUntil, r.lastErr = true, time.Time{}, cause.Error()
	})
}

// get returns a copy of the row whose payload is data.
func (s *memStore) get(data string) (row, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if string(r.rec.Message.Data) == data {
			return *r, true
		}
	}
	return row{}, false
}

func (s *memStore) Wake() <-chan struct{} { return s.wake }

func (s *memStore) live() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.rows {
		if !r.dead {
			out = append(out, string(r.rec.Message.Data))
		}
	}
	return out
}

func (s *memStore) deadRows() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.rows {
		if r.dead {
			out = append(out, string(r.rec.Message.Data))
		}
	}
	return out
}

func (s *memStore) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

// flakyPub fails the first publish of message "fail-once" and, like a
// KeyResumer publisher, pauses its key until ResumeKey.
type flakyPub struct {
	mu      sync.Mutex
	got     []string
	failed  bool
	paused  map[string]bool
	resumed []string
}

func (p *flakyPub) Publish(_ context.Context, _ string, msgs ...*pubsub.Message) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]string, len(msgs))
	failed := map[int]error{}
	for i, m := range msgs {
		if p.paused[m.Key] {
			failed[i] = pubsub.ErrKeyPaused
			continue
		}
		if string(m.Data) == "fail-once" && !p.failed {
			p.failed = true
			failed[i] = errors.New("unavailable")
			if m.Key != "" {
				if p.paused == nil {
					p.paused = map[string]bool{}
				}
				p.paused[m.Key] = true
			}
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
	delete(p.paused, key)
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
		outbox.WithLogger(log.Discard()))
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

// failPub fails every message whose data is in fail (with err).
type failPub struct {
	mu   sync.Mutex
	fail map[string]error
	got  []string
}

func (p *failPub) Publish(_ context.Context, _ string, msgs ...*pubsub.Message) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]string, len(msgs))
	failed := map[int]error{}
	for i, m := range msgs {
		if err, ok := p.fail[string(m.Data)]; ok {
			failed[i] = err
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
func (p *failPub) Close(context.Context) error { return nil }

func TestRelayDeadLettersPoisonRecords(t *testing.T) {
	st := &memStore{wake: make(chan struct{}, 1)}
	pub := &failPub{fail: map[string]error{
		"poison":    errors.New("unavailable"),
		"malformed": pubsub.Permanent(errors.New("too large")),
	}}
	st.add("t", &pubsub.Message{Key: "k", Data: []byte("poison")})
	st.add("t", &pubsub.Message{Key: "k", Data: []byte("after-poison")})
	st.add("t", &pubsub.Message{Key: "j", Data: []byte("malformed")})
	st.add("t", &pubsub.Message{Key: "j", Data: []byte("after-malformed")})

	r, err := outbox.NewRelay(st, pub, outbox.WithMaxAttempts(3),
		outbox.WithRetryBackoff(time.Microsecond, time.Microsecond),
		outbox.WithLogger(log.Discard()))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for range 20 {
		if _, err := r.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	if live := st.live(); len(live) != 0 {
		t.Fatalf("live rows left: %v", live)
	}
	if dead := st.deadRows(); !slices.Equal(dead, []string{"poison", "malformed"}) {
		t.Fatalf("dead rows = %v", dead)
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if !slices.Contains(pub.got, "after-poison") || !slices.Contains(pub.got, "after-malformed") {
		t.Fatalf("published %v: keys stayed blocked", pub.got)
	}
}

func TestNewRelayValidation(t *testing.T) {
	if _, err := outbox.NewRelay(nil, &flakyPub{}); err == nil {
		t.Fatal("nil store accepted")
	}
	if _, err := outbox.NewRelay(&memStore{}, &flakyPub{}, outbox.WithBatchSize(0)); err == nil {
		t.Fatal("batch 0 accepted")
	}
	if _, err := outbox.NewRelay(&memStore{}, &flakyPub{}, outbox.WithMaxAttempts(-1)); err == nil {
		t.Fatal("negative max attempts accepted")
	}
}
