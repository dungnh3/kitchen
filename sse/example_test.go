package sse_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/redisx"
	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/redisbus"
)

func ExampleAppendEvent() {
	b, _ := sse.AppendEvent(nil, sse.Event{ID: "1732089600000-0", Type: sse.TypeToken, Data: []byte("Hello\nworld")})
	_, _ = os.Stdout.Write(b)
	// Output:
	// id: 1732089600000-0
	// event: stream.token
	// data: Hello
	// data: world
}

func ExampleVisibleTypes() {
	visible := sse.VisibleTypes("run.", "approval.required")
	fmt.Println(visible(sse.TypeToken), visible("run.started"), visible("approval.granted"), visible("internal.audit"))
	// Output: true true false false
}

// printStream prints the event and data lines of an SSE response (ids are
// Redis entry IDs, which change on every run).
func printStream(url, lastEventID string) {
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if line := sc.Text(); strings.HasPrefix(line, "event:") || strings.HasPrefix(line, "data:") {
			fmt.Println(strings.TrimSpace(line))
		}
	}
}

// A writer appends to a stream with a Producer; a gateway serves it to
// browsers with a Hub and a Server.
func ExampleServer() {
	mr, _ := miniredis.Run()
	defer mr.Close()
	ctx := context.Background()
	logger := log.Default() // main calls log.Setup once

	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = rdb.Close() }()

	// Writer side: append, then End.
	prod, _ := redisbus.NewProducer(rdb, redisbus.ProducerConfig{})
	_, _ = prod.Append(ctx, "run_1", sse.Message{Type: sse.TypeToken, Data: []byte("Hello")})
	_, _ = prod.End(ctx, "run_1")

	// Reader side: one Hub and one Server per process.
	hub, _ := redisbus.NewHub(rdb, redisbus.HubConfig{Logger: logger})
	defer hub.Close()
	srv, _ := sse.NewServer(hub, sse.ServerConfig{
		MaxConnections:          10_000,
		MaxConnectionsPerStream: 20,
		Logger:                  logger,
	})
	defer func() { _ = srv.Shutdown(ctx) }()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /runs/{id}/stream", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		ticket, err := srv.Admit(r.Context(), id)
		if err != nil {
			errs.WriteHTTP(w, r, err) // 503 or 429, before anything is written
			return
		}
		err = srv.Serve(w, r, id, sse.ServeOptions{Ticket: ticket})
		switch {
		case err == nil, errors.Is(err, sse.ErrServerClosed), errors.Is(err, sse.ErrReplayGap):
		case errors.Is(err, sse.ErrInvalidCursor), errors.Is(err, sse.ErrStreamingUnsupported):
			errs.WriteHTTP(w, r, err) // the response is not committed yet
		default:
			logger.WarnContext(r.Context(), "stream ended", log.Err(err))
		}
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()
	printStream(ts.URL+"/runs/run_1/stream", "")
	// Output:
	// event: stream.token
	// data: Hello
	// event: stream.done
	// data:
}

// A reconnecting browser sends Last-Event-ID and receives only what follows.
func ExampleServer_resume() {
	mr, _ := miniredis.Run()
	defer mr.Close()
	ctx := context.Background()
	rdb, _ := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	defer func() { _ = rdb.Close() }()

	prod, _ := redisbus.NewProducer(rdb, redisbus.ProducerConfig{})
	first, _ := prod.Append(ctx, "run_1", sse.Message{Type: sse.TypeToken, Data: []byte("Hel")})
	_, _ = prod.Append(ctx, "run_1", sse.Message{Type: sse.TypeToken, Data: []byte("lo")})
	_, _ = prod.End(ctx, "run_1")

	hub, _ := redisbus.NewHub(rdb, redisbus.HubConfig{})
	defer hub.Close()
	srv, _ := sse.NewServer(hub, sse.ServerConfig{})
	defer func() { _ = srv.Shutdown(ctx) }()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Serve reads the Last-Event-ID header; ServeOptions.LastEventID
		// overrides it (for a cursor carried in a query parameter).
		err := srv.Serve(w, r, "run_1", sse.ServeOptions{LastEventID: r.URL.Query().Get("cursor")})
		if errors.Is(err, sse.ErrInvalidCursor) {
			errs.WriteHTTP(w, r, err) // 400 INVALID_LAST_EVENT_ID
		}
	}))
	defer ts.Close()
	printStream(ts.URL, first)
	// Output:
	// event: stream.token
	// data: lo
	// event: stream.done
	// data:
}

// ServeOptions tune one connection: which types the browser sees, an idle
// limit, and write batching.
func ExampleServeOptions() {
	opts := sse.ServeOptions{
		Visible:       sse.VisibleTypes("run.", "step.", "approval.required"),
		MaxIdle:       2 * time.Minute,       // ends with stream.error {"type":"idle_timeout"}
		FlushInterval: 50 * time.Millisecond, // batch small token writes
	}
	fmt.Println(opts.Visible("step.started"), opts.Visible("internal.audit"))
	// Output: true false
}

// A TransformFactory builds one Transform per connection. This one rewrites
// moderated messages and ends the connection on a safety block.
func ExampleTransformFactory() {
	var moderate sse.TransformFactory = func(context.Context) sse.Transform {
		return func(_ context.Context, m *sse.Message) (sse.Action, error) {
			switch m.Type {
			case "safety.blocked":
				return sse.ActionStop, nil // written, then the connection ends
			case "internal.audit":
				return sse.ActionSkip, nil // this connection only
			}
			return sse.ActionPass, nil
		}
	}
	t := moderate(context.Background())
	for _, typ := range []string{sse.TypeToken, "internal.audit", "safety.blocked"} {
		act, _ := t(context.Background(), &sse.Message{Type: typ})
		fmt.Println(typ, act)
	}
	// Output:
	// stream.token 0
	// internal.audit 1
	// safety.blocked 2
}
