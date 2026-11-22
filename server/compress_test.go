package server

import (
	"bufio"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// bodySvc serves a large JSON body, a tiny one and a slow SSE stream.
type bodySvc struct{}

func (bodySvc) RegisterGRPC(grpc.ServiceRegistrar) {}

func (bodySvc) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("GET /big", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"items":"`+strings.Repeat("abc", 2000)+`"}`)
	})
	mux.HandleFunc("GET /small", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentTypeSSE)
		_, _ = io.WriteString(w, "data: first\n\n")
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done() // never ends on its own
	})
}

// fetch requests path with Accept-Encoding: gzip (when gz) and does not let
// the transport decompress. The body is closed when the test ends.
func fetch(t *testing.T, base, path string, gz bool) (http.Header, io.Reader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if gz {
		req.Header.Set("Accept-Encoding", "gzip")
	}
	c := &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DisableCompression: true}}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp.Header, resp.Body
}

func TestCompression(t *testing.T) {
	r := start(t, WithServices(bodySvc{}), WithCompression())

	h, body := fetch(t, r.httpURL, "/big", true)
	if h.Get("Content-Encoding") != "gzip" || !strings.Contains(h.Get("Vary"), "Accept-Encoding") {
		t.Fatalf("big: headers %v", h)
	}
	zr, err := gzip.NewReader(body)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := io.ReadAll(zr); len(b) != len(`{"items":""}`)+6000 {
		t.Fatalf("big: %d bytes after gunzip", len(b))
	}

	// Below 1 KiB, or a client without gzip: sent as is.
	if h, _ := fetch(t, r.httpURL, "/small", true); h.Get("Content-Encoding") != "" {
		t.Fatalf("small compressed: %v", h)
	}
	if h, _ := fetch(t, r.httpURL, "/big", false); h.Get("Content-Encoding") != "" {
		t.Fatalf("no Accept-Encoding compressed: %v", h)
	}

	// SSE is not compressed and the first event is not held back.
	h, body = fetch(t, r.httpURL, "/events", true)
	if h.Get("Content-Encoding") != "" {
		t.Fatalf("sse compressed: %v", h)
	}
	line, err := bufio.NewReader(body).ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatalf("sse first line %q %v", line, err)
	}
}

func TestCompressionOffByDefault(t *testing.T) {
	r := start(t, WithServices(bodySvc{}))
	if h, _ := fetch(t, r.httpURL, "/big", true); h.Get("Content-Encoding") != "" {
		t.Fatalf("compressed without WithCompression: %v", h)
	}
	_, body := fetch(t, r.httpURL, "/events", true)
	if line, err := bufio.NewReader(body).ReadString('\n'); err != nil || line != "data: first\n" {
		t.Fatalf("sse %q %v", line, err)
	}
}

func TestCompressible(t *testing.T) {
	for ct, want := range map[string]bool{
		"application/json":                 true,
		"application/json; charset=utf-8":  true,
		"text/event-stream":                false,
		"text/event-stream; charset=utf-8": false,
		"application/x-ndjson":             false,
		"image/png":                        false,
		"application/zip":                  false,
		"image/svg+xml":                    true,
		"font/woff2":                       false,
	} {
		if got := compressible(ct); got != want {
			t.Errorf("compressible(%q) = %v", ct, got)
		}
	}
}
