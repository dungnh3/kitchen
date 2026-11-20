//go:build loadtest

// Load test against a real Redis. Not run in CI. Example (30K viewers):
//
//	SSE_LOADTEST_REDIS=localhost:6379 SSE_LOADTEST_STREAMS=30000 SSE_LOADTEST_TOKENS=200 \
//	  go test -tags loadtest -run TestLoad -timeout 30m -v ./redisbus/
//
// Raise the file-descriptor limit first (ulimit -n 200000). It starts one
// HTTP server (Hub + sse.Server), one SSE client connection per stream, and
// writers appending tokens at SSE_LOADTEST_RATE per second per stream. It
// reports end-to-end latency percentiles (append → browser read) and the
// number of Redis client connections the hub used.
package redisbus_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/redisbus"
)

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func TestLoad(t *testing.T) {
	addr := os.Getenv("SSE_LOADTEST_REDIS")
	if addr == "" {
		t.Skip("set SSE_LOADTEST_REDIS")
	}
	streams := envInt("SSE_LOADTEST_STREAMS", 1000)
	tokens := envInt("SSE_LOADTEST_TOKENS", 100)
	rate := envInt("SSE_LOADTEST_RATE", 20)

	readRDB := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 256})
	writeRDB := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 512})
	defer func() { _ = readRDB.Close(); _ = writeRDB.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prefix := fmt.Sprintf("ssetest:%d:", time.Now().UnixNano())
	ks := redisbus.Keyspace{Prefix: prefix}
	prod, err := redisbus.NewProducer(writeRDB, redisbus.ProducerConfig{Keyspace: ks, DoneTTL: time.Minute, ActiveTTL: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	hub, err := redisbus.NewHub(readRDB, redisbus.HubConfig{Keyspace: ks})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	srv, _ := sse.NewServer(hub, sse.ServerConfig{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /s/{id}", func(w http.ResponseWriter, r *http.Request) {
		_ = srv.Serve(w, r, r.PathValue("id"), sse.ServeOptions{})
	})
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = hs.Serve(ln) }()
	defer func() { _ = hs.Close() }()
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: streams, MaxConnsPerHost: 0}}

	var (
		mu        sync.Mutex
		latencies []time.Duration
		received  atomic.Int64
		connected sync.WaitGroup
		finished  sync.WaitGroup
	)
	connected.Add(streams)
	for i := range streams {
		finished.Go(func() {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/s/load-%d", ln.Addr(), i), nil)
			resp, err := client.Do(req)
			connected.Done()
			if err != nil {
				t.Errorf("connect: %v", err)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			local := make([]time.Duration, 0, tokens)
			sc := bufio.NewScanner(resp.Body)
			for sc.Scan() {
				line := sc.Text()
				if strings.HasPrefix(line, "event: "+sse.TypeDone) {
					break
				}
				if ns, ok := strings.CutPrefix(line, "data: "); ok && ns != "" {
					if v, err := strconv.ParseInt(ns, 10, 64); err == nil {
						local = append(local, time.Since(time.Unix(0, v)))
						received.Add(1)
					}
				}
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		})
	}
	connected.Wait()
	time.Sleep(2 * time.Second) // let every subscription register

	start := time.Now()
	var writers sync.WaitGroup
	for i := range streams {
		writers.Go(func() {
			id := fmt.Sprintf("load-%d", i)
			tick := time.NewTicker(time.Second / time.Duration(rate))
			defer tick.Stop()
			for range tokens {
				<-tick.C
				data := strconv.AppendInt(nil, time.Now().UnixNano(), 10)
				if _, err := prod.Append(ctx, id, sse.Message{Type: sse.TypeToken, Data: data}); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
			_, _ = prod.End(ctx, id)
		})
	}
	writers.Wait()
	finished.Wait()
	elapsed := time.Since(start)

	slices.Sort(latencies)
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*p)]
	}
	want := int64(streams * tokens)
	t.Logf("streams=%d tokens/stream=%d rate=%d/s elapsed=%s", streams, tokens, rate, elapsed)
	t.Logf("received=%d/%d p50=%s p90=%s p99=%s max=%s", received.Load(), want, pct(.5), pct(.9), pct(.99), pct(1))
	t.Logf("hub redis pool: total=%d idle=%d", readRDB.PoolStats().TotalConns, readRDB.PoolStats().IdleConns)
	if received.Load() != want {
		t.Fatalf("lost messages: %d of %d", want-received.Load(), want)
	}
}
