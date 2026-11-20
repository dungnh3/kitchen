package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type item struct {
	Name string `json:"name"`
}

func newClient(t *testing.T, srv *httptest.Server, opts ...Option) *Client {
	t.Helper()
	c, err := New(srv.URL+"/api", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.hc.CloseIdleConnections() })
	return c
}

func TestJSONRoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/items" || r.URL.Query().Get("q") != "a b" || r.URL.Query().Get("x") != "1" {
			t.Errorf("url %s", r.URL)
		}
		if r.Method == http.MethodPost && r.Header.Get("Authorization") != "Bearer tok" || r.Header.Get("X-Default") != "d" {
			t.Errorf("headers %v", r.Header)
		}
		var in item
		_ = json.NewDecoder(r.Body).Decode(&in)
		_ = json.NewEncoder(w).Encode(item{Name: in.Name + "!"})
	}))
	defer srv.Close()
	c := newClient(t, srv, WithHeader("X-Default", "d"))
	var out item
	if err := c.Post("v1/items?x=1").Query("q", "a b").Bearer("tok").JSON(item{"n"}).Do(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Name != "n!" {
		t.Fatalf("out = %+v", out)
	}
	var raw []byte
	if err := c.Get("/v1/items?x=1&q=a+b").Do(context.Background(), &raw); err != nil || !strings.Contains(string(raw), `"name"`) {
		t.Fatalf("raw = %q %v", raw, err)
	}
}

func TestStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"code":3,"message":"bad"}`+strings.Repeat(" ", 100))
	}))
	defer srv.Close()
	c := newClient(t, srv, WithMaxErrorBody(30))
	err := c.Get("/x?token=secret").Do(context.Background(), nil)
	se, ok := errors.AsType[*StatusError](err)
	if !ok || se.StatusCode != 400 || len(se.Body) != 30 || StatusCode(err) != 400 {
		t.Fatalf("err = %v", err)
	}
	var body struct{ Message string }
	if err := se.Decode(&body); err != nil || body.Message != "bad" {
		t.Fatalf("decode %v %+v", err, body)
	}
	if StatusCode(errors.New("x")) != 0 {
		t.Error("StatusCode of plain error")
	}
}

func TestRetry(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		b, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodGet && string(b) != `{"name":"n"}` {
			t.Errorf("body on attempt %d = %q", n, b)
		}
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"name":"ok"}`)
	}))
	defer srv.Close()
	c := newClient(t, srv, WithRetry(RetryPolicy{MinBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}))
	ctx := context.Background()

	var out item
	if err := c.Get("/r").Do(ctx, &out); err != nil || out.Name != "ok" || calls.Load() != 3 {
		t.Fatalf("GET: %v %+v calls=%d", err, out, calls.Load())
	}

	calls.Store(0)
	if err := c.Post("/r").JSON(item{"n"}).Do(ctx, nil); StatusCode(err) != 503 || calls.Load() != 1 {
		t.Fatalf("POST without key retried: %v calls=%d", err, calls.Load())
	}

	calls.Store(0)
	if err := c.Post("/r").IdempotencyKey("k").JSON(item{"n"}).Do(ctx, nil); err != nil || calls.Load() != 3 {
		t.Fatalf("POST with key: %v calls=%d", err, calls.Load())
	}

	calls.Store(0)
	if err := c.Put("/r").Body(io.NopCloser(strings.NewReader(`{"name":"n"}`)), "application/json").Do(ctx, nil); StatusCode(err) != 503 || calls.Load() != 1 {
		t.Fatalf("non-rewindable body retried: %v calls=%d", err, calls.Load())
	}
}

func TestRetryAfterCap(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c := newClient(t, srv, WithRetry(RetryPolicy{}))
	if err := c.Get("/").Do(context.Background(), nil); StatusCode(err) != 429 || calls.Load() != 1 {
		t.Fatalf("%v calls=%d", err, calls.Load())
	}
	if d, ok := retryAfter("2"); !ok || d != 2*time.Second {
		t.Error("retry-after seconds")
	}
	if _, ok := retryAfter(time.Now().Add(time.Second).UTC().Format(http.TimeFormat)); !ok {
		t.Error("retry-after date")
	}
	if _, ok := retryAfter("soon"); ok {
		t.Error("retry-after junk")
	}
}

func TestRetryStopsAtDeadline(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	// The 5s wait does not fit in the 200ms left, so no retry is made.
	c := newClient(t, srv, WithRetry(RetryPolicy{}))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.Get("/").Do(ctx, nil); StatusCode(err) != 502 || calls.Load() != 1 {
		t.Fatalf("%v calls=%d", err, calls.Load())
	}
}

func TestMultipart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, h, err := r.FormFile("file")
		if err != nil {
			t.Error(err)
			return
		}
		b, _ := io.ReadAll(f)
		if h.Filename != "a.txt" || string(b) != "hello" || r.FormValue("k") != "v" {
			t.Errorf("multipart %q %q %q", h.Filename, b, r.FormValue("k"))
		}
	}))
	defer srv.Close()
	c := newClient(t, srv)
	err := c.Post("/up").Multipart(func(w *multipart.Writer) error {
		if err := w.WriteField("k", "v"); err != nil {
			return err
		}
		fw, err := w.CreateFormFile("file", "a.txt")
		if err != nil {
			return err
		}
		_, err = io.WriteString(fw, "hello")
		return err
	}).Do(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentUse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"name":"`+r.URL.Query().Get("i")+`"}`)
	}))
	defer srv.Close()
	c := newClient(t, srv)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			var out item
			want := strings.Repeat("x", i)
			if err := c.Get("/c").Query("i", want).Do(context.Background(), &out); err != nil || out.Name != want {
				t.Errorf("%d: %v %q", i, err, out.Name)
			}
		})
	}
	wg.Wait()
}

func TestNewErrors(t *testing.T) {
	for _, u := range []string{"", "/rel", "ftp://x", "http://%zz"} {
		if _, err := New(u); err == nil {
			t.Errorf("New(%q) succeeded", u)
		}
	}
	c, _ := New("http://x")
	if err := c.Post("/").JSON(func() {}).Do(context.Background(), nil); err == nil {
		t.Error("unencodable body accepted")
	}
}

func TestSendReturnsNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	defer srv.Close()
	c := newClient(t, srv, WithTimeout(0))
	resp, err := c.Get("/").Send(context.Background())
	if err != nil || resp.StatusCode != http.StatusTeapot {
		t.Fatal(err)
	}
	resp.Body.Close()
}
