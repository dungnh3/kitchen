package sse_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/redisx"
	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/redisbus"
)

// A well-formed cursor newer than the stream was never issued. Serve must
// refuse it before committing the response, so the client gets a 400 instead
// of an empty stream it would reconnect to forever.
func TestServeRejectsFutureCursorBeforeCommit(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}}, redisx.WithoutTracing())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rdb.Close() }()
	prod, _ := redisbus.NewProducer(rdb, redisbus.ProducerConfig{})
	if _, err := prod.Append(ctx, "run_1", sse.Message{Type: sse.TypeToken, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	hub, _ := redisbus.NewHub(rdb, redisbus.HubConfig{Logger: log.Discard()})
	defer hub.Close()
	srv, _ := sse.NewServer(hub, sse.ServerConfig{Logger: log.Discard()})
	defer func() { _ = srv.Shutdown(ctx) }()

	for _, cursor := range []string{"99999999999999-0", "not-a-cursor"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequestWithContext(ctx, http.MethodGet, "/runs/run_1/stream", nil)
		r.Header.Set("Last-Event-ID", cursor)
		err := srv.Serve(w, r, "run_1", sse.ServeOptions{})
		if !errors.Is(err, sse.ErrInvalidCursor) {
			t.Fatalf("cursor %q: Serve = %v, want ErrInvalidCursor", cursor, err)
		}
		if w.Flushed || w.Body.Len() != 0 {
			t.Fatalf("cursor %q: response committed before the error (%q)", cursor, w.Body.String())
		}
		errs.WriteHTTP(w, r, err)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("cursor %q: status %d, want 400", cursor, w.Code)
		}
	}
}
