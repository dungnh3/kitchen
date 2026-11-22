package cache_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/cache"
	"github.com/dungnh3/kitchen/errs"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type user struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func newRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func newCache(t *testing.T, cfg cache.Config[user]) *cache.Cache[user] {
	t.Helper()
	c, err := cache.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestNewValidation(t *testing.T) {
	_, rdb := newRedis(t)
	bad := []cache.Config[user]{
		{},
		{Redis: rdb, TTL: time.Minute},
		{Redis: rdb, Prefix: "u:"},
		{Local: &cache.LocalConfig{MaxSize: 0, TTL: time.Second}},
		{Local: &cache.LocalConfig{MaxSize: 1, TTL: time.Second}, NegativeTTL: -1},
		{Local: &cache.LocalConfig{MaxSize: 1, TTL: time.Second}, Jitter: 1},
	}
	for i, cfg := range bad {
		if c, err := cache.New(cfg); err == nil {
			c.Close()
			t.Errorf("case %d: want error", i)
		}
	}
}

func TestSetGetDeleteRedis(t *testing.T) {
	mr, rdb := newRedis(t)
	ctx := context.Background()
	c := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute})

	if _, err := c.Get(ctx, "1"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get miss err = %v", err)
	}
	if err := c.Set(ctx, "1", user{"1", "Ann"}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, "1")
	if err != nil || got.Name != "Ann" {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	ttl := mr.TTL("u:1")
	if ttl < 54*time.Second || ttl > 66*time.Second {
		t.Fatalf("TTL with jitter = %v", ttl)
	}
	raw, _ := mr.Get("u:1")
	if raw[0] != 'v' {
		t.Fatalf("raw payload %q", raw)
	}
	if err := c.Delete(ctx, "1", "2"); err != nil {
		t.Fatal(err)
	}
	if mr.Exists("u:1") {
		t.Fatal("key not deleted")
	}
}

func TestGetOrLoadSingleflight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := cache.New(cache.Config[user]{Local: &cache.LocalConfig{MaxSize: 100, TTL: time.Minute}})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		ctx := context.Background()

		var calls atomic.Int32
		release := make(chan struct{})
		load := func(_ context.Context, id string) (user, error) {
			calls.Add(1)
			<-release
			return user{id, "Ann"}, nil
		}
		var wg sync.WaitGroup
		errs := make(chan error, 50)
		for range 50 {
			wg.Go(func() {
				u, err := c.GetOrLoad(ctx, "1", load)
				if err == nil && u.Name != "Ann" {
					err = fmt.Errorf("got %+v", u)
				}
				errs <- err
			})
		}
		synctest.Wait() // every caller is now blocked on the single flight
		close(release)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if n := calls.Load(); n != 1 {
			t.Fatalf("loader calls = %d, want 1", n)
		}
		if _, err := c.GetOrLoad(ctx, "1", load); err != nil || calls.Load() != 1 {
			t.Fatalf("second GetOrLoad: %v, calls %d", err, calls.Load())
		}
		// L1 TTL expiry, on the bubble's fake clock.
		time.Sleep(2 * time.Minute)
		if _, err := c.GetOrLoad(ctx, "1", load); err != nil || calls.Load() != 2 {
			t.Fatalf("after L1 TTL: %v, calls %d, want 2", err, calls.Load())
		}
	})
}

func TestGetOrLoadCallerCancelDoesNotCancelLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := cache.New(cache.Config[user]{Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute}})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		release := make(chan struct{})
		var loadCtxErr error
		load := func(ctx context.Context, id string) (user, error) {
			<-release
			loadCtxErr = ctx.Err()
			return user{id, "Ann"}, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := c.GetOrLoad(ctx, "1", load)
			done <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want Canceled", err)
		}
		close(release)
		synctest.Wait() // detached load finishes and fills the cache
		if loadCtxErr != nil {
			t.Fatalf("loader ctx err = %v", loadCtxErr)
		}
		if u, err := c.Get(context.Background(), "1"); err != nil || u.Name != "Ann" {
			t.Fatalf("Get after detached load = %+v, %v", u, err)
		}
	})
}

func TestNegativeCaching(t *testing.T) {
	mr, rdb := newRedis(t)
	ctx := context.Background()
	c := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute, NegativeTTL: 10 * time.Second, Jitter: -1})
	var calls int
	load := func(context.Context, string) (user, error) {
		calls++
		return user{}, fmt.Errorf("repo: %w", cache.ErrNotFound)
	}
	for range 3 {
		if _, err := c.GetOrLoad(ctx, "x", load); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
	if ttl := mr.TTL("u:x"); ttl != 10*time.Second {
		t.Fatalf("negative TTL = %v", ttl)
	}
	if _, err := c.Get(ctx, "x"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get negative = %v", err)
	}
}

func TestLoaderErrorNotCached(t *testing.T) {
	c := newCache(t, cache.Config[user]{Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute}, NegativeTTL: time.Minute})
	boom := errors.New("db down")
	var calls int
	load := func(context.Context, string) (user, error) { calls++; return user{}, boom }
	for range 2 {
		if _, err := c.GetOrLoad(context.Background(), "x", load); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestFailOpenWhenRedisDown(t *testing.T) {
	mr, rdb := newRedis(t)
	ctx := context.Background()
	c := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute})
	mr.Close()
	u, err := c.GetOrLoad(ctx, "1", func(_ context.Context, id string) (user, error) { return user{id, "Ann"}, nil })
	if err != nil || u.Name != "Ann" {
		t.Fatalf("GetOrLoad with Redis down = %+v, %v", u, err)
	}
	if _, err := c.Get(ctx, "1"); err == nil || errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get with Redis down should surface the error, got %v", err)
	}
	m, err := c.GetOrLoadMany(ctx, []string{"1", "2"}, func(context.Context, []string) (map[string]user, error) {
		return map[string]user{"1": {"1", "Ann"}, "2": {"2", "Bob"}}, nil
	})
	if err != nil || len(m) != 2 {
		t.Fatalf("GetOrLoadMany with Redis down = %v, %v", m, err)
	}
}

func TestL2PromotesToL1(t *testing.T) {
	mr, rdb := newRedis(t)
	ctx := context.Background()
	c := newCache(t, cache.Config[user]{
		Redis: rdb, Prefix: "u:", TTL: time.Minute,
		Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute},
	})
	writer := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Minute})
	if err := writer.Set(ctx, "1", user{"1", "Ann"}); err != nil {
		t.Fatal(err)
	}
	if u, err := c.Get(ctx, "1"); err != nil || u.Name != "Ann" {
		t.Fatalf("Get = %+v, %v", u, err)
	}
	mr.FlushAll()
	if u, err := c.Get(ctx, "1"); err != nil || u.Name != "Ann" {
		t.Fatalf("Get from L1 = %+v, %v", u, err)
	}
}

func TestManyAndPrefix(t *testing.T) {
	mr, rdb := newRedis(t)
	ctx := context.Background()
	c := newCache(t, cache.Config[user]{
		Redis: rdb, Prefix: "u:", TTL: time.Minute, NegativeTTL: time.Minute,
		Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute},
	})
	_ = c.Set(ctx, "a1", user{"a1", "A1"})

	var asked []string
	got, err := c.GetOrLoadMany(ctx, []string{"a1", "a2", "b1", "a2"}, func(_ context.Context, ids []string) (map[string]user, error) {
		asked = ids
		return map[string]user{"a2": {"a2", "A2"}}, nil // b1 omitted → not found
	})
	if err != nil || len(got) != 2 || got["a2"].Name != "A2" {
		t.Fatalf("GetOrLoadMany = %v, %v", got, err)
	}
	if len(asked) != 2 {
		t.Fatalf("loader asked for %v", asked)
	}
	raw, _ := mr.Get("u:b1")
	if raw != "n" {
		t.Fatalf("b1 negative payload = %q", raw)
	}

	m, err := c.GetMany(ctx, []string{"a1", "a2", "b1", "zz"})
	if err != nil || len(m) != 2 {
		t.Fatalf("GetMany = %v, %v", m, err)
	}

	_ = mr.Set("u:a*literal", "v{}") // must not be matched by prefix "a" glob escape test below
	n, err := c.DeleteByPrefix(ctx, "a")
	if err != nil || n != 3 {
		t.Fatalf("DeleteByPrefix = %d, %v (keys %v)", n, err, mr.Keys())
	}
	if !mr.Exists("u:b1") {
		t.Fatal("b1 must survive")
	}
	if _, err := c.Get(ctx, "a1"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("a1 still in L1: %v", err)
	}
	_ = mr.Set("u:x*y", "n")
	_ = mr.Set("u:xzy", "n")
	if n, _ := c.DeleteByPrefix(ctx, "x*"); n != 1 || !mr.Exists("u:xzy") {
		t.Fatalf("glob not escaped: n=%d keys=%v", n, mr.Keys())
	}
}

type upperCodec struct{}

func (upperCodec) Marshal(v string) ([]byte, error)    { return []byte(v), nil }
func (upperCodec) Unmarshal(b []byte, v *string) error { *v = string(b); return nil }

func TestCustomCodecAndBadPayload(t *testing.T) {
	mr, rdb := newRedis(t)
	ctx := context.Background()
	c, err := cache.New(cache.Config[string]{Redis: rdb, Prefix: "s:", TTL: time.Minute, Codec: upperCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.Set(ctx, "k", "hello")
	if raw, _ := mr.Get("s:k"); raw != "vhello" {
		t.Fatalf("raw = %q", raw)
	}
	_ = mr.Set("s:bad", "?garbage")
	if _, err := c.Get(ctx, "bad"); err == nil || errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("bad payload err = %v", err)
	}
}

func ExampleCache_GetOrLoad() {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	users, err := cache.New(cache.Config[user]{
		Redis:  rdb, // nil → in-process only
		Prefix: "user:v1:",
		TTL:    10 * time.Minute,
		Local:  &cache.LocalConfig{MaxSize: 100_000, TTL: 30 * time.Second},
	})
	if err != nil {
		panic(err)
	}
	defer users.Close()

	u, err := users.GetOrLoad(context.Background(), "42", func(_ context.Context, id string) (user, error) {
		return user{ID: id, Name: "Ann"}, nil // e.g. repo.GetUser(ctx, id)
	})
	fmt.Println(u.Name, err)
	// Output: Ann <nil>
}

func TestLoaderPanicIsAnError(t *testing.T) {
	_, rdb := newRedis(t)
	for name, cfg := range map[string]cache.Config[user]{
		"l1": {Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute}},
		"l2": {Redis: rdb, Prefix: "p:", TTL: time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			c := newCache(t, cfg)
			_, err := c.GetOrLoad(context.Background(), "1", func(context.Context, string) (user, error) { panic("boom") })
			var pe *cache.PanicError
			if !errors.As(err, &pe) || pe.Value != "boom" {
				t.Fatalf("err = %v", err)
			}
			_, err = c.GetOrLoadMany(context.Background(), []string{"1"}, func(context.Context, []string) (map[string]user, error) { panic("bulk") })
			if !errors.As(err, &pe) {
				t.Fatalf("bulk err = %v", err)
			}
		})
	}
}

func TestLoadTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := cache.New(cache.Config[user]{Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute}, LoadTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_, err = c.GetOrLoad(context.Background(), "1", func(ctx context.Context, _ string) (user, error) {
			<-ctx.Done() // a loader that hangs until its deadline
			return user{}, ctx.Err()
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestCloneProtectsEntries(t *testing.T) {
	type bag struct{ Items []string }
	c, err := cache.New(cache.Config[*bag]{
		Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute},
		Clone: func(b *bag) *bag { return &bag{Items: append([]string(nil), b.Items...)} },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	in := &bag{Items: []string{"a"}}
	_ = c.Set(ctx, "k", in)
	in.Items[0] = "mutated-after-set"
	got, _ := c.Get(ctx, "k")
	got.Items[0] = "mutated-after-get"
	again, _ := c.Get(ctx, "k")
	if again.Items[0] != "a" {
		t.Fatalf("entry mutated: %v", again.Items)
	}
}

func TestErrsNotFoundIsNegative(t *testing.T) {
	c := newCache(t, cache.Config[user]{Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute}, NegativeTTL: time.Minute})
	var calls atomic.Int32
	domainNotFound := errs.New(codes.NotFound, "USER_NOT_FOUND")
	load := func(context.Context, string) (user, error) { calls.Add(1); return user{}, domainNotFound }
	for range 2 {
		if _, err := c.GetOrLoad(context.Background(), "1", load); !errors.Is(err, cache.ErrNotFound) || errs.Code(err) != codes.NotFound {
			t.Fatalf("err = %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1 (negative entry cached)", calls.Load())
	}
}

func TestSetWithTTL(t *testing.T) {
	mr, rdb := newRedis(t)
	c := newCache(t, cache.Config[user]{Redis: rdb, Prefix: "u:", TTL: time.Hour, Jitter: -1})
	if err := c.Set(context.Background(), "1", user{ID: "1"}, cache.WithTTL(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL("u:1"); ttl != 5*time.Minute {
		t.Fatalf("redis TTL = %v", ttl)
	}
	if err := c.Set(context.Background(), "2", user{}, cache.WithTTL(-time.Second)); err == nil {
		t.Fatal("negative TTL accepted")
	}
}

func TestRefreshAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := cache.New(cache.Config[user]{Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute, RefreshAfter: 10 * time.Second}})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		ctx := context.Background()
		var calls atomic.Int32
		load := func(context.Context, string) (user, error) {
			n := calls.Add(1)
			return user{ID: "1", Name: fmt.Sprint("v", n)}, nil
		}
		if u, _ := c.GetOrLoad(ctx, "1", load); u.Name != "v1" {
			t.Fatalf("first = %v", u)
		}
		time.Sleep(15 * time.Second)
		// Past RefreshAfter: the current value is served and a refresh starts.
		if u, _ := c.GetOrLoad(ctx, "1", load); u.Name != "v1" {
			t.Fatalf("stale read = %v", u)
		}
		synctest.Wait()
		if u, _ := c.GetOrLoad(ctx, "1", load); u.Name != "v2" || calls.Load() != 2 {
			t.Fatalf("after refresh = %v, calls %d", u, calls.Load())
		}
	})
}

func TestGetOrLoadManySharesLoads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := cache.New(cache.Config[user]{Local: &cache.LocalConfig{MaxSize: 100, TTL: time.Minute}})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		var loadedKeys atomic.Int32
		release := make(chan struct{})
		load := func(_ context.Context, keys []string) (map[string]user, error) {
			loadedKeys.Add(int32(len(keys))) //nolint:gosec // tiny test slices
			<-release
			out := map[string]user{}
			for _, k := range keys {
				out[k] = user{ID: k}
			}
			return out, nil
		}
		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				if m, err := c.GetOrLoadMany(context.Background(), []string{"a", "b"}, load); err != nil || len(m) != 2 {
					t.Errorf("m=%v err=%v", m, err)
				}
			})
		}
		synctest.Wait()
		close(release)
		wg.Wait()
		if n := loadedKeys.Load(); n != 2 {
			t.Fatalf("keys loaded = %d, want 2 (each key once)", n)
		}
	})
}
