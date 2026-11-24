package kitchen_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/cache"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
	"github.com/dungnh3/kitchen/server"
	"github.com/redis/go-redis/v9"
)

type lines struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lines) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// The process has one logger. With only the process logger installed in
// main (log.SetDefault, as log.Setup does), the server, pubsub and cache,
// none given a Logger, write every line in the same schema. (auth, a
// separate module, has the same check in its tests.)
func TestOneLogFormat(t *testing.T) {
	var out lines
	log.SetDefault(log.New(&out, log.Options{Schema: log.SchemaGCP, Level: log.LevelDebug}))

	// server: an unknown route is logged by the access log.
	var lc net.ListenConfig
	gl, _ := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	hl, _ := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	srv, err := server.New(server.WithGRPCListener(gl), server.WithHTTPListener(hl))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	for i := 0; ; i++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+hl.Addr().String()+"/nope", nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
			break
		}
		if i > 200 {
			t.Fatal("server did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// pubsub: a failing handler is logged (nack).
	broker := memory.New()
	sub, err := broker.Subscribe("t", "s")
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan struct{}, 1)
	subscriber, err := pubsub.NewSubscriber(sub, func(context.Context, *pubsub.Message) error {
		select {
		case handled <- struct{}{}:
		default:
		}
		return errors.New("boom")
	})
	if err != nil {
		t.Fatal(err)
	}
	sctx, scancel := context.WithCancel(context.Background())
	sdone := make(chan error, 1)
	go func() { sdone <- subscriber.Run(sctx) }()
	if _, err := broker.Publish(context.Background(), "t", &pubsub.Message{Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	<-handled
	time.Sleep(50 * time.Millisecond)
	scancel()
	if err := <-sdone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscriber: %v", err)
	}
	_ = broker.Close(context.Background())

	// cache: an unreachable Redis is logged before falling back to the loader.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	defer func() { _ = rdb.Close() }()
	c, err := cache.New(cache.Config[string]{Redis: rdb, Prefix: "t:", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(context.Background()) }()
	_, _ = c.GetOrLoad(context.Background(), "k", func(context.Context, string) (string, error) { return "v", nil })

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	got := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(got) < 3 {
		t.Fatalf("expected lines from server, pubsub and cache, got:\n%s", out.String())
	}
	seen := map[string]bool{}
	for _, line := range got {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("not kitchen JSON: %q", line)
		}
		for _, k := range []string{"time", "severity", "message"} {
			if _, ok := m[k]; !ok {
				t.Errorf("line without %q (schema not applied): %s", k, line)
			}
		}
		for _, k := range []string{"msg", "level"} {
			if _, ok := m[k]; ok {
				t.Errorf("line with slog's own key %q: %s", k, line)
			}
		}
		msg, _ := m["message"].(string)
		switch {
		case strings.HasPrefix(msg, "http request"):
			seen["server"] = true
		case strings.HasPrefix(msg, "pubsub"):
			seen["pubsub"] = true
		case strings.HasPrefix(msg, "cache"):
			seen["cache"] = true
		}
	}
	for _, p := range []string{"server", "pubsub", "cache"} {
		if !seen[p] {
			t.Errorf("no line from %s:\n%s", p, out.String())
		}
	}
}
