package sse_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dungnh3/kitchen/sse"
)

// Hidden messages keep the stream alive but write nothing, so they must not
// postpone the heartbeat: proxies close connections that stay silent.
func TestServeHeartbeatDespiteHiddenMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake()
		s := newServer(t, f, sse.ServerConfig{Heartbeat: 10 * time.Second})
		w := newRecorder()
		done := make(chan error, 1)
		go func() { done <- s.Serve(w, request(t.Context(), ""), "r", sse.ServeOptions{}) }()
		for i := range 8 {
			time.Sleep(5 * time.Second)
			f.send("r", sse.Message{ID: "1-" + string(rune('0'+i)), Type: "internal.hidden", Data: []byte("x")})
		}
		synctest.Wait()
		if n := strings.Count(w.String(), ": ping\n"); n < 3 {
			t.Fatalf("pings after 40s of hidden-only traffic = %d, want >= 3", n)
		}
		close(f.ch("r"))
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

// ActionStop ends the stream with a terminal stream.cancelled, so the browser
// closes instead of reconnecting and resuming after the stopping message.
func TestServeStopWritesTerminalEvent(t *testing.T) {
	f := newFake()
	s := newServer(t, f, sse.ServerConfig{})
	f.send("r",
		sse.Message{ID: "1-0", Type: sse.TypeToken, Data: []byte("bad")},
		sse.Message{ID: "2-0", Type: sse.TypeToken, Data: []byte("after")},
	)
	stop := func(context.Context) sse.Transform {
		return func(context.Context, *sse.Message) (sse.Action, error) { return sse.ActionStop, nil }
	}
	w := newRecorder()
	if err := s.Serve(w, request(t.Context(), ""), "r", sse.ServeOptions{Transforms: []sse.TransformFactory{stop}}); err != nil {
		t.Fatal(err)
	}
	body := w.String()
	if !strings.HasSuffix(body, "event: stream.cancelled\ndata: {\"reason\":\"stopped\"}\n\n") || strings.Contains(body, "after") {
		t.Fatalf("body:\n%s", body)
	}
}

// slowCheck delays CheckCursor, widening the window between Serve's
// closed check and its registration with Shutdown.
type slowCheck struct{ *fakeSource }

func (s slowCheck) CheckCursor(ctx context.Context, id, c string) error {
	time.Sleep(time.Millisecond) //nolint:forbidigo // widens a real race window
	return s.fakeSource.CheckCursor(ctx, id, c)
}

// A Serve that starts while Shutdown runs either refuses before writing
// anything or is waited for: nothing is written after Shutdown returns.
func TestShutdownWaitsForEveryServe(t *testing.T) {
	for range 30 {
		s := newServer(t, slowCheck{newFake()}, sse.ServerConfig{})
		ws := make([]*recorder, 8)
		var wg sync.WaitGroup
		for i := range ws {
			ws[i] = newRecorder()
			wg.Go(func() { _ = s.Serve(ws[i], request(t.Context(), ""), "r", sse.ServeOptions{}) })
		}
		time.Sleep(500 * time.Microsecond) //nolint:forbidigo // let some Serve calls pass the closed check
		if err := s.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		at := make([]string, len(ws))
		for i, w := range ws {
			at[i] = w.String()
		}
		wg.Wait()
		for i, w := range ws {
			if w.String() != at[i] {
				t.Fatalf("Serve %d wrote after Shutdown returned: %q", i, w.String())
			}
		}
	}
}
