package cache_test

// Tests of the consistency, failure and resource guarantees of Cache:
// writes win over loads, Redis failures are bounded, Close waits, hits do not
// allocate.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/cache"
	"github.com/dungnh3/kitchen/log"
)

// hook is a go-redis hook that lets a test observe, delay or fail commands.
type hook struct {
	// before runs before a command or pipeline; after runs after it.
	before func(ctx context.Context, cmds []redis.Cmder) error
	after  func(ctx context.Context, cmds []redis.Cmder)
	single atomic.Int32 // commands sent outside a pipeline
	pipes  atomic.Int32 // pipelines sent
}

func (h *hook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.single.Add(1)
		return h.run(ctx, []redis.Cmder{cmd}, func() error { return next(ctx, cmd) })
	}
}

func (h *hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.pipes.Add(1)
		return h.run(ctx, cmds, func() error { return next(ctx, cmds) })
	}
}

func (h *hook) run(ctx context.Context, cmds []redis.Cmder, next func() error) error {
	if h.before != nil {
		if err := h.before(ctx, cmds); err != nil {
			for _, c := range cmds {
				c.SetErr(err)
			}
			return err
		}
	}
	err := next()
	if h.after != nil {
		h.after(ctx, cmds)
	}
	return err
}

func hasCmd(cmds []redis.Cmder, name string) bool {
	for _, c := range cmds {
		if c.Name() == name {
			return true
		}
	}
	return false
}

func closeCache[V any](t *testing.T, c *cache.Cache[V]) {
	t.Helper()
	if err := c.Close(context.Background()); err != nil {
		t.Error(err)
	}
}

func l1Only(t *testing.T, cfg cache.Config[user]) *cache.Cache[user] {
	t.Helper()
	cfg.Name = "t"
	if cfg.Local == nil {
		cfg.Local = &cache.LocalConfig{MaxSize: 10, TTL: time.Minute}
	}
	c, err := cache.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// A Delete or Set during a load wins over the load in L1.
func TestWriteDuringLoadWinsInL1(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(*cache.Cache[user]) error
		want  string // "" = not found
	}{
		{"delete", func(c *cache.Cache[user]) error { return c.Delete(context.Background(), "1") }, ""},
		{"set", func(c *cache.Cache[user]) error { return c.Set(context.Background(), "1", user{"1", "new"}) }, "new"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := l1Only(t, cache.Config[user]{})
				defer closeCache(t, c)
				release := make(chan struct{})
				go func() {
					_, _ = c.GetOrLoad(context.Background(), "1", func(context.Context, string) (user, error) {
						<-release
						return user{"1", "old"}, nil
					})
				}()
				synctest.Wait() // the load is in flight
				if err := tc.write(c); err != nil {
					t.Fatal(err)
				}
				close(release)
				synctest.Wait()
				u, err := c.Get(context.Background(), "1")
				switch {
				case tc.want == "" && !errors.Is(err, cache.ErrNotFound):
					t.Fatalf("Get = %+v, %v; the old value came back after Delete", u, err)
				case tc.want != "" && u.Name != tc.want:
					t.Fatalf("Get = %+v, %v; want %q", u, err, tc.want)
				}
			})
		})
	}
}

// A Delete or Set on another pod during a load wins over the load in Redis:
// the load holds a lease and writes back only if it is still there.
func TestWriteDuringLoadWinsInRedis(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(*cache.Cache[user]) error
		want  string // raw Redis payload; "" = absent
	}{
		{"delete", func(c *cache.Cache[user]) error { return c.Delete(context.Background(), "1") }, ""},
		{"set", func(c *cache.Cache[user]) error { return c.Set(context.Background(), "1", user{"1", "new"}) }, `v{"id":"1","name":"new"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mr, rdb := newRedis(t)
			a := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute, Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute}})
			b := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute})
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan user)
			go func() {
				u, _ := a.GetOrLoad(context.Background(), "1", func(context.Context, string) (user, error) {
					close(started)
					<-release
					return user{"1", "old"}, nil
				})
				done <- u
			}()
			<-started
			// While loading, the key holds a lease: readers see a miss.
			if _, err := b.Get(context.Background(), "1"); !errors.Is(err, cache.ErrNotFound) {
				t.Fatalf("Get during load = %v, want ErrNotFound", err)
			}
			if err := tc.write(b); err != nil {
				t.Fatal(err)
			}
			close(release)
			if u := <-done; u.Name != "old" {
				t.Fatalf("caller got %+v, want the loaded value", u)
			}
			raw, _ := mr.Get("u:1")
			if raw != tc.want {
				t.Fatalf("Redis holds %q, want %q", raw, tc.want)
			}
			// Nor is the old value kept in a's L1.
			if u, err := a.Get(context.Background(), "1"); err == nil && u.Name == "old" {
				t.Fatal("old value kept in L1")
			}
		})
	}
}

// A batch load does not write back keys deleted meanwhile, nor keep them in
// L1.
func TestDeleteDuringBatchLoad(t *testing.T) {
	mr, rdb := newRedis(t)
	a := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute, Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute}})
	b := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute})
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan map[string]user)
	go func() {
		m, _ := a.GetOrLoadMany(context.Background(), []string{"1", "2"}, func(_ context.Context, keys []string) (map[string]user, error) {
			close(started)
			<-release
			out := map[string]user{}
			for _, k := range keys {
				out[k] = user{k, "old"}
			}
			return out, nil
		})
		done <- m
	}()
	<-started
	if err := b.Delete(context.Background(), "1"); err != nil {
		t.Fatal(err)
	}
	close(release)
	if m := <-done; len(m) != 2 {
		t.Fatalf("caller got %v, want both loaded values", m)
	}
	if mr.Exists("u:1") || !mr.Exists("u:2") {
		t.Fatalf("Redis keys = %v, want only u:2", mr.Keys())
	}
	if _, err := a.Get(context.Background(), "1"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("deleted key kept in L1: %v", err)
	}
}

// A Get that promotes a Redis value to L1 must not undo a Delete that ran
// while it read Redis.
func TestPromotionDoesNotUndoDelete(t *testing.T) {
	_, rdb := newRedis(t)
	gate := make(chan struct{})
	var block atomic.Bool
	h := &hook{after: func(_ context.Context, cmds []redis.Cmder) {
		if hasCmd(cmds, "get") && block.CompareAndSwap(true, false) {
			<-gate // the GET has read the value; hold it before L1 sees it
		}
	}}
	rdb.AddHook(h)
	c := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute, Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute}})
	w := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute})
	if err := w.Set(context.Background(), "1", user{"1", "old"}); err != nil {
		t.Fatal(err)
	}
	block.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.Get(context.Background(), "1")
	}()
	for block.Load() { // wait until the GET is held
		select {
		case <-done:
			t.Fatal("Get did not reach Redis")
		default:
			runtime.Gosched()
		}
	}
	if err := c.Delete(context.Background(), "1"); err != nil {
		t.Fatal(err)
	}
	close(gate)
	<-done
	if _, err := c.Get(context.Background(), "1"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrNotFound (stale value promoted)", err)
	}
}

// A panicking Codec is returned as a *PanicError instead of crashing the
// process from singleflight's goroutine.
func TestCodecPanicIsAnError(t *testing.T) {
	_, rdb := newRedis(t)
	c, err := cache.New(cache.Config[string]{Redis: rdb, Prefix: "p:", TTL: time.Minute, Codec: panicCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer closeCache(t, c)
	_, err = c.GetOrLoad(context.Background(), "k", func(context.Context, string) (string, error) { return "v", nil })
	var pe *cache.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want *PanicError", err)
	}
}

type panicCodec struct{}

func (panicCodec) Marshal(string) ([]byte, error)  { panic("codec bug") }
func (panicCodec) Unmarshal([]byte, *string) error { panic("codec bug") }

// GetMany reports a Redis error on any key, not only the first.
func TestGetManyReportsLaterErrors(t *testing.T) {
	_, rdb := newRedis(t)
	boom := errors.New("node down")
	rdb.AddHook(&hook{after: func(_ context.Context, cmds []redis.Cmder) {
		for i := len(cmds) - 1; i >= 0; i-- {
			if cmds[i].Name() == "get" {
				cmds[i].SetErr(boom) // the last key fails; the first is a clean miss
				return
			}
		}
	}})
	c := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute})
	if _, err := c.GetMany(context.Background(), []string{"a", "b"}); !errors.Is(err, boom) {
		t.Fatalf("GetMany err = %v, want %v", err, boom)
	}
}

// A hung Redis costs a read-through call at most RedisTimeout, and after a
// few failures it is skipped.
func TestHungRedisIsBoundedAndSkipped(t *testing.T) {
	_, rdb := newRedis(t)
	var calls atomic.Int32
	rdb.AddHook(&hook{before: func(ctx context.Context, _ []redis.Cmder) error {
		calls.Add(1)
		<-ctx.Done() // hang until the caller's deadline
		return ctx.Err()
	}})
	c := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute, RedisTimeout: 20 * time.Millisecond, Logger: log.Discard()})
	load := func(_ context.Context, id string) (user, error) { return user{id, "Ann"}, nil }
	for i := range 10 {
		res := make(chan error, 1)
		go func() {
			_, err := c.GetOrLoad(context.Background(), fmt.Sprint(i), load)
			res <- err
		}()
		select {
		case err := <-res:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("GetOrLoad blocked on a hung Redis")
		}
	}
	if n := calls.Load(); n > 6 {
		t.Fatalf("Redis called %d times, want it skipped after 5 failures", n)
	}
}

// Read-through failures are logged at most once per interval, and keys are
// never logged.
func TestWarningsAreThrottledAndKeyless(t *testing.T) {
	mr, rdb := newRedis(t)
	mr.Close()
	var buf syncBuf
	c := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute, Logger: log.New(&buf, log.Options{})})
	for i := range 3 {
		if _, err := c.GetOrLoad(context.Background(), fmt.Sprintf("alice%d@example.com", i), func(_ context.Context, id string) (user, error) {
			return user{id, "Ann"}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	out := buf.String()
	if n := strings.Count(out, "lookup failed"); n != 1 {
		t.Fatalf("lookup warnings = %d, want 1:\n%s", n, out)
	}
	if strings.Contains(out, "example.com") || !strings.Contains(out, "cache.key_hash") {
		t.Fatalf("log must carry a key hash, not the key:\n%s", out)
	}
	if _, err := c.Get(context.Background(), "alice@example.com"); err == nil || strings.Contains(err.Error(), "example.com") {
		t.Fatalf("error must not carry the key: %v", err)
	}
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// Close cancels and waits for detached loads; afterwards loads fail with
// ErrClosed.
func TestCloseCancelsAndWaitsForLoads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := l1Only(t, cache.Config[user]{})
		var exited atomic.Bool
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			_, _ = c.GetOrLoad(ctx, "1", func(lctx context.Context, _ string) (user, error) {
				<-lctx.Done() // a load only Close can stop
				exited.Store(true)
				return user{}, lctx.Err()
			})
		}()
		synctest.Wait()
		cancel() // the caller gives up; the load keeps running detached
		synctest.Wait()
		if err := c.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !exited.Load() {
			t.Fatal("Close returned before the load ended")
		}
		if _, err := c.GetOrLoad(context.Background(), "2", func(context.Context, string) (user, error) { return user{}, nil }); !errors.Is(err, cache.ErrClosed) {
			t.Fatalf("GetOrLoad after Close = %v, want ErrClosed", err)
		}
		if err := c.Close(context.Background()); err != nil {
			t.Fatalf("second Close = %v", err)
		}
	})
}

// Close gives up when its ctx ends.
func TestCloseHonoursCtx(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := l1Only(t, cache.Config[user]{})
		release := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			_, _ = c.GetOrLoad(ctx, "1", func(context.Context, string) (user, error) {
				<-release // ignores its ctx
				return user{}, nil
			})
		}()
		synctest.Wait()
		cancel()
		cctx, ccancel := context.WithTimeout(context.Background(), time.Second)
		defer ccancel()
		if err := c.Close(cctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close = %v, want DeadlineExceeded", err)
		}
		close(release)
		synctest.Wait()
	})
}

// Refresh errors go to Config.Logger, not slog.Default, and a refresh
// is not cut short by the request that started it.
func TestRefreshLogsToConfigLogger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var buf syncBuf
		c := l1Only(t, cache.Config[user]{
			Local:  &cache.LocalConfig{MaxSize: 10, TTL: time.Minute, RefreshAfter: 10 * time.Second},
			Logger: log.New(&buf, log.Options{}),
		})
		defer closeCache(t, c)
		var calls atomic.Int32
		load := func(context.Context, string) (user, error) {
			if calls.Add(1) == 3 {
				return user{}, errors.New("db down")
			}
			time.Sleep(time.Second) // a refresh outlives the request
			return user{"1", fmt.Sprint("v", calls.Load())}, nil
		}
		if _, err := c.GetOrLoad(context.Background(), "1", load); err != nil {
			t.Fatal(err)
		}
		time.Sleep(15 * time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		if u, _ := c.GetOrLoad(ctx, "1", load); u.Name != "v1" {
			t.Fatalf("stale read = %+v", u)
		}
		cancel() // the request ends at once
		synctest.Wait()
		time.Sleep(2 * time.Second)
		if u, _ := c.GetOrLoad(context.Background(), "1", load); u.Name != "v2" {
			t.Fatalf("after refresh = %+v, want v2", u)
		}
		if out := buf.String(); out != "" {
			t.Fatalf("unexpected log: %s", out)
		}
		time.Sleep(15 * time.Second)
		_, _ = c.GetOrLoad(context.Background(), "1", load) // refresh #2 fails
		synctest.Wait()
		if out := buf.String(); !strings.Contains(out, "db down") {
			t.Fatalf("refresh error not in Config.Logger: %q", out)
		}
	})
}

// L1 TTLs are jittered downwards, so entries written together do not
// expire together, and never outlive Local.TTL.
func TestL1TTLJitter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := l1Only(t, cache.Config[user]{Local: &cache.LocalConfig{MaxSize: 1000, TTL: 100 * time.Second}, Jitter: 0.5})
		defer closeCache(t, c)
		for i := range 100 {
			_ = c.Set(context.Background(), fmt.Sprint(i), user{})
		}
		count := func() int {
			n := 0
			for i := range 100 {
				if _, err := c.Get(context.Background(), fmt.Sprint(i)); err == nil {
					n++
				}
			}
			return n
		}
		time.Sleep(75 * time.Second)
		if n := count(); n == 0 || n == 100 {
			t.Fatalf("at 75s %d of 100 entries alive, want some expired and some not", n)
		}
		time.Sleep(25 * time.Second)
		if n := count(); n != 0 {
			t.Fatalf("at Local.TTL %d entries alive, want 0", n)
		}
	})
}

// A value promoted from Redis lives in L1 no longer than its Redis TTL.
func TestPromotionKeepsRedisTTL(t *testing.T) {
	mr, rdb := newRedis(t)
	synctest.Test(t, func(t *testing.T) {
		w := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Hour, Jitter: -1})
		r := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Hour, Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Hour}, Jitter: -1})
		if err := w.Set(context.Background(), "otp", user{"otp", "123456"}, cache.WithTTL(5*time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Get(context.Background(), "otp"); err != nil {
			t.Fatal(err)
		}
		mr.FastForward(6 * time.Second)
		time.Sleep(6 * time.Second)
		if u, err := r.Get(context.Background(), "otp"); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("expired entry served from L1: %+v, %v", u, err)
		}
		closeCache(t, w)
		closeCache(t, r)
	})
}

// Local.MaxWeight bounds L1 by weight.
func TestMaxWeight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := cache.New(cache.Config[string]{
			Name:    "w",
			Local:   &cache.LocalConfig{MaxWeight: 10, TTL: time.Minute},
			Weigher: func(_ string, v string) uint32 { return uint32(len(v)) }, //nolint:gosec // test
		})
		if err != nil {
			t.Fatal(err)
		}
		defer closeCache(t, c)
		ctx := context.Background()
		for i := range 5 {
			_ = c.Set(ctx, fmt.Sprint(i), "12345") // 5 × 5 > 10
		}
		synctest.Wait()
		n := 0
		for i := range 5 {
			if _, err := c.Get(ctx, fmt.Sprint(i)); err == nil {
				n++
			}
		}
		if n > 2 {
			t.Fatalf("%d entries of weight 5 kept, MaxWeight 10", n)
		}
	})
}

func TestConfigValidation(t *testing.T) {
	weigh := func(string, user) uint32 { return 1 }
	for name, cfg := range map[string]cache.Config[user]{
		"no name without prefix": {Local: &cache.LocalConfig{MaxSize: 1, TTL: time.Second}},
		"size and weight":        {Name: "t", Local: &cache.LocalConfig{MaxSize: 1, MaxWeight: 1, TTL: time.Second}, Weigher: weigh},
		"weight without weigher": {Name: "t", Local: &cache.LocalConfig{MaxWeight: 1, TTL: time.Second}},
		"weigher without weight": {Name: "t", Local: &cache.LocalConfig{MaxSize: 1, TTL: time.Second}, Weigher: weigh},
		"negative redis timeout": {Name: "t", Local: &cache.LocalConfig{MaxSize: 1, TTL: time.Second}, RedisTimeout: -1},
	} {
		if c, err := cache.New(cfg); err == nil {
			_ = c.Close(context.Background())
			t.Errorf("%s: want error", name)
		}
	}
}

// GetOrLoadMany reads and writes Redis in pipelines, not one command per
// key.
func TestBatchUsesPipelines(t *testing.T) {
	_, rdb := newRedis(t)
	h := &hook{}
	rdb.AddHook(h)
	c := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute})
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = fmt.Sprint(i)
	}
	loadAll := func(_ context.Context, ks []string) (map[string]user, error) {
		out := map[string]user{}
		for _, k := range ks {
			out[k] = user{ID: k}
		}
		return out, nil
	}
	_, _ = c.GetOrLoadMany(context.Background(), []string{"warm-up"}, loadAll) // connection and script setup
	h.single.Store(0)
	h.pipes.Store(0)
	m, err := c.GetOrLoadMany(context.Background(), keys, loadAll)
	if err != nil || len(m) != 50 {
		t.Fatalf("GetOrLoadMany = %d, %v", len(m), err)
	}
	if s, p := h.single.Load(), h.pipes.Load(); s != 0 || p != 2 {
		t.Fatalf("%d single commands and %d pipelines for 50 keys", s, p)
	}
	if got, err := c.GetMany(context.Background(), keys); err != nil || len(got) != 50 {
		t.Fatalf("GetMany = %d, %v", len(got), err)
	}
}

// An entry the codec cannot read is replaced by the loaded value.
func TestUnreadableEntryIsReplaced(t *testing.T) {
	mr, rdb := newRedis(t)
	c := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute, Logger: log.Discard()})
	_ = mr.Set("u:1", "v{not json")
	if u, err := c.GetOrLoad(context.Background(), "1", func(_ context.Context, id string) (user, error) { return user{id, "Ann"}, nil }); err != nil || u.Name != "Ann" {
		t.Fatalf("GetOrLoad = %+v, %v", u, err)
	}
	if u, err := c.Get(context.Background(), "1"); err != nil || u.Name != "Ann" {
		t.Fatalf("entry not replaced: %+v, %v", u, err)
	}
}

// L1 hits allocate nothing.
func TestHitsDoNotAllocate(t *testing.T) {
	c := l1Only(t, cache.Config[user]{})
	defer closeCache(t, c)
	ctx := context.Background()
	load := func(_ context.Context, id string) (user, error) { return user{ID: id}, nil }
	_, _ = c.GetOrLoad(ctx, "k", load)
	if n := testing.AllocsPerRun(100, func() { _, _ = c.Get(ctx, "k") }); n != 0 {
		t.Errorf("Get hit: %v allocs", n)
	}
	if n := testing.AllocsPerRun(100, func() { _, _ = c.GetOrLoad(ctx, "k", load) }); n != 0 {
		t.Errorf("GetOrLoad hit: %v allocs", n)
	}
}
