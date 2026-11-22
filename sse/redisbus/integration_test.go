package redisbus_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/aisdk"
	"github.com/dungnh3/kitchen/sse/envelope"
	"github.com/dungnh3/kitchen/sse/redisbus"
)

type stack struct {
	rdb  *redis.Client
	prod *redisbus.Producer
	hub  *redisbus.Hub
	srv  *sse.Server
	http *httptest.Server
}

func newStack(t *testing.T, enc sse.EncoderFactory) *stack {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	prod, err := redisbus.NewProducer(rdb, redisbus.ProducerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	hub, err := redisbus.NewHub(rdb, redisbus.HubConfig{Block: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := sse.NewServer(hub, sse.ServerConfig{MaxConnectionsPerStream: 5})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /runs/{id}/stream", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		tk, err := srv.Admit(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		err = srv.Serve(w, r, id, sse.ServeOptions{Ticket: tk, Encoder: enc})
		if errors.Is(err, sse.ErrInvalidCursor) {
			http.Error(w, "bad Last-Event-ID", http.StatusBadRequest)
		}
	})
	ts := httptest.NewServer(mux)
	s := &stack{rdb: rdb, prod: prod, hub: hub, srv: srv, http: ts}
	t.Cleanup(func() {
		ts.Close()
		_ = srv.Shutdown(context.Background())
		hub.Close()
		_ = rdb.Close()
	})
	return s
}

type wireEvent struct{ id, typ, data string }

// read parses SSE frames until the body ends.
func read(t *testing.T, s *stack, run, lastID string) (int, []wireEvent) {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, s.http.URL+"/runs/"+run+"/stream", nil)
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	resp, err := s.http.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil
	}
	var out []wireEvent
	var cur wireEvent
	var data []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if cur.typ != "" || data != nil || cur.id != "" {
				cur.data = strings.Join(data, "\n")
				out = append(out, cur)
			}
			cur, data = wireEvent{}, nil
		case strings.HasPrefix(line, "id: "):
			cur.id = line[4:]
		case strings.HasPrefix(line, "event: "):
			cur.typ = line[7:]
		case strings.HasPrefix(line, "data: "):
			data = append(data, line[6:])
		}
	}
	return resp.StatusCode, out
}

func TestEndToEndEnvelopeWire(t *testing.T) {
	s := newStack(t, envelope.NewEncoder)
	ctx := t.Context()
	b := envelope.NewBuilder("run1", "app", "u1")
	for _, tok := range []string{"Hel", "lo"} {
		e, _ := b.BuildText(sse.TypeToken, tok)
		m, _ := e.Message()
		if _, err := s.prod.Append(ctx, "run1", m); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.prod.End(ctx, "run1"); err != nil {
		t.Fatal(err)
	}
	code, evs := read(t, s, "run1", "")
	if code != http.StatusOK || len(evs) != 3 {
		t.Fatalf("code %d events %+v", code, evs)
	}
	if evs[0].data != "Hel" || evs[1].data != "lo" || evs[2].typ != sse.TypeDone || evs[0].id == "" {
		t.Fatalf("events = %+v", evs)
	}

	// Resume from the first token: only the second comes back, then done.
	_, resumed := read(t, s, "run1", evs[0].id)
	if len(resumed) != 2 || resumed[0].data != "lo" || resumed[1].typ != sse.TypeDone {
		t.Fatalf("resumed = %+v", resumed)
	}

	if code, _ := read(t, s, "run1", "not-an-id"); code != http.StatusBadRequest {
		t.Fatalf("invalid cursor status = %d", code)
	}
}

func TestEndToEndLiveAISDK(t *testing.T) {
	s := newStack(t, aisdk.NewEncoder)
	ctx := t.Context()
	done := make(chan []wireEvent, 1)
	go func() { _, evs := read(t, s, "run2", ""); done <- evs }()
	time.Sleep(100 * time.Millisecond) // let the viewer subscribe first
	for i := range 3 {
		if _, err := s.prod.Append(ctx, "run2", sse.Message{Type: sse.TypeToken, Data: fmt.Appendf(nil, "t%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.prod.End(ctx, "run2"); err != nil {
		t.Fatal(err)
	}
	var evs []wireEvent
	select {
	case evs = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
	var joined []string
	for _, e := range evs {
		joined = append(joined, e.data)
	}
	got := strings.Join(joined, "|")
	for _, want := range []string{`"type":"start"`, `"type":"text-start"`, `"delta":"t0"`, `"delta":"t2"`, `"type":"text-end"`, `"type":"finish"`, `[DONE]`} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %s in %s", want, got)
		}
	}
}
