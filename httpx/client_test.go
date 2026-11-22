package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func newClient(t *testing.T, opts ...Option) *Client {
	t.Helper()
	c, err := New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	return c
}

type item struct {
	Name string `json:"name"`
}

func TestJSONAndPathParams(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		post := r.Method == http.MethodPost
		if post && (r.URL.EscapedPath() != "/api/items/a%2Fb%3Fx=1" || r.URL.Query().Get("q") != "a b") ||
			r.Header.Get("User-Agent") != "kitchen-httpx" ||
			r.Header.Get("Authorization") != "Bearer tok" || r.Header.Get("X-Default") != "d" {
			t.Errorf("path %s query %s headers %v", r.URL.EscapedPath(), r.URL.RawQuery, r.Header)
		}
		var in item
		_ = json.NewDecoder(r.Body).Decode(&in)
		// No content type: Do decodes JSON anyway.
		_, _ = w.Write([]byte(`{"name":"` + in.Name + `!"}`))
	}))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL+"/api"), WithBearer("tok"), WithHeader("X-Default", "d"))
	var out item
	err := c.Post("/items/{id}").PathParam("id", "a/b?x=1").Query("q", "a b").JSON(item{"n"}).Do(t.Context(), &out)
	if err != nil || out.Name != "n!" {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	var raw []byte
	if err := c.Get("/items/x").Do(t.Context(), &raw); err != nil || !strings.Contains(string(raw), `"name"`) {
		t.Fatalf("raw=%q err=%v", raw, err)
	}
}

func TestEmptyBodyAndNilOut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/204" {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL))
	var out item
	for _, p := range []string{"/204", "/empty"} {
		if err := c.Get(p).Do(t.Context(), &out); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
	}
	if err := c.Delete("/x").Do(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestFormAndMultipart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/form":
			_ = r.ParseForm()
			_ = json.NewEncoder(w).Encode(item{Name: r.PostForm.Get("token")})
		case "/upload":
			f, h, err := r.FormFile("file")
			if err != nil {
				t.Error(err)
				return
			}
			b, _ := io.ReadAll(f)
			_ = json.NewEncoder(w).Encode(item{Name: r.FormValue("purpose") + ":" + h.Filename + ":" + string(b)})
		}
	}))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL))
	var out item
	if err := c.Post("/form").Form(url.Values{"token": {"t1"}}).Do(t.Context(), &out); err != nil || out.Name != "t1" {
		t.Fatalf("form: %+v %v", out, err)
	}
	err := c.Post("/upload").FormField("purpose", "fine-tune").File("file", "a.txt", strings.NewReader("hello")).Do(t.Context(), &out)
	if err != nil || out.Name != "fine-tune:a.txt:hello" {
		t.Fatalf("multipart: %+v %v", out, err)
	}
}

func TestSendStreams(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bad" {
			http.Error(w, "nope", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: 1\n\ndata: 2\n\n"))
	}))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL))
	res, err := c.Get("/events").Send(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body())
	_ = res.Body().Close()
	if res.StatusCode() != 200 || res.Header().Get("Content-Type") != "text/event-stream" || string(b) != "data: 1\n\ndata: 2\n\n" {
		t.Fatalf("stream: %d %q", res.StatusCode(), b)
	}
	if res, err := c.Get("/bad").Send(t.Context()); res != nil || StatusCode(err) != 400 {
		t.Fatalf("bad: %v %v", res, err)
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		status int
		header string
		want   codes.Code
		retry  time.Duration
	}{
		{404, "", codes.NotFound, 0},
		{401, "", codes.Internal, 0},
		{403, "", codes.Internal, 0},
		{400, "", codes.Internal, 0},
		{500, "", codes.Internal, 0},
		{504, "", codes.DeadlineExceeded, 0},
		{503, "7", codes.Unavailable, 7 * time.Second},
		{429, "", codes.Unavailable, 0},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.header != "" {
					w.Header().Set("Retry-After", tt.header)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(`{"message":"boom"}`))
			}))
			defer srv.Close()
			c := newClient(t, WithBaseURL(srv.URL))
			var apiErr struct{ Message string }
			var out item
			err := c.Get("/x?key=SECRET").ErrorInto(&apiErr).Do(t.Context(), &out)
			if got := errs.Code(err); got != tt.want {
				t.Fatalf("code = %v, want %v (%v)", got, tt.want, err)
			}
			se, ok := errors.AsType[*StatusError](err)
			if !ok || se.StatusCode != tt.status || string(se.Body) != `{"message":"boom"}` || StatusCode(err) != tt.status {
				t.Fatalf("status error = %+v", se)
			}
			if strings.Contains(err.Error(), "SECRET") || !strings.Contains(se.URL, "key=REDACTED") {
				t.Fatalf("secret leaked: %v / %s", err, se.URL)
			}
			if apiErr.Message != "boom" {
				t.Fatalf("ErrorInto not decoded: %+v", apiErr)
			}
			if tt.retry > 0 {
				e, _ := errors.AsType[*errs.Error](err)
				var got time.Duration
				for _, d := range e.Details() {
					if ri, ok := d.(*errdetails.RetryInfo); ok {
						got = ri.GetRetryDelay().AsDuration()
					}
				}
				if got != tt.retry {
					t.Fatalf("retry info = %v", got)
				}
			}
		})
	}
}

func TestErrorMapperOverride(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL), WithErrorMapper(nil))
	err := c.Get("/").Do(t.Context(), nil)
	//nolint:errorlint // the mapper is off, so the error must be the bare *StatusError
	if se, ok := err.(*StatusError); !ok || se.StatusCode != http.StatusConflict {
		t.Fatalf("err = %#v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHooksAndMiddleware(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(item{Name: r.Header.Get("X-Sig") + "," + r.Header.Get("X-Mw")})
	}))
	defer srv.Close()
	var mu sync.Mutex
	var seen []int
	c := newClient(t, WithBaseURL(srv.URL),
		WithRequestHook(func(r *Request) error {
			if r.HeaderValue("X-Block") != "" {
				return errors.New("blocked by hook")
			}
			r.Header("X-Sig", r.Method()+" "+r.Path())
			return nil
		}),
		WithResponseHook(func(res *Response) error {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, res.StatusCode())
			return nil
		}),
		WithTransportMiddleware(func(next http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(r *http.Request) (*http.Response, error) {
				r.Header.Set("X-Mw", "1")
				return next.RoundTrip(r)
			})
		}),
	)
	var out item
	if err := c.Get("/a").Do(t.Context(), &out); err != nil || out.Name != "GET /a,1" {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if err := c.Get("/a").Header("X-Block", "1").Do(t.Context(), &out); err == nil || err.Error() != "blocked by hook" {
		t.Fatalf("hook did not block: %v", err)
	}
	if len(seen) != 1 || seen[0] != 200 {
		t.Fatalf("response hook saw %v", seen)
	}
}

func TestOriginGuard(t *testing.T) {
	var hits atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer other.Close()
	base := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer base.Close()

	c := newClient(t, WithBaseURL(base.URL+"/v1"), WithBearer("secret"))
	err := c.Get(other.URL+"/steal").Do(t.Context(), nil)
	if !errors.Is(err, ErrDestinationNotAllowed) || errs.Code(err) != codes.InvalidArgument || hits.Load() != 0 {
		t.Fatalf("cross-origin absolute URL: err=%v hits=%d", err, hits.Load())
	}
	if err := c.Get(base.URL+"/v1/ok").Do(t.Context(), nil); err != nil {
		t.Fatalf("same origin: %v", err)
	}
	open := newClient(t) // no base URL: any URL
	if err := open.Get(other.URL).Do(t.Context(), nil); err != nil || hits.Load() != 1 {
		t.Fatalf("no base: err=%v hits=%d", err, hits.Load())
	}
	loose := newClient(t, WithBaseURL(base.URL), WithoutOriginGuard())
	if err := loose.Get(other.URL).Do(t.Context(), nil); err != nil || hits.Load() != 2 {
		t.Fatalf("WithoutOriginGuard: err=%v", err)
	}
}

func TestSafeRedirect(t *testing.T) {
	var got http.Header
	other := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = r.Header.Clone() }))
	defer other.Close()
	var same http.Header
	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/same":
			http.Redirect(w, r, srvURL+"/final", http.StatusFound)
		case "/final":
			same = r.Header.Clone()
		default:
			// localhost vs 127.0.0.1: another origin.
			http.Redirect(w, r, strings.Replace(other.URL, "127.0.0.1", "localhost", 1), http.StatusFound)
		}
	}))
	defer srv.Close()
	srvURL = srv.URL

	c := newClient(t, WithBaseURL(srv.URL), WithBearer("tok"), WithHeader("X-Custom-Key", "k"), WithHeader("X-Trace-Me", "1"))
	if err := c.Get("/cross").Do(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{"Authorization", "X-Custom-Key", "X-Trace-Me"} {
		if got.Get(h) != "" {
			t.Errorf("%s leaked on cross-origin redirect", h)
		}
	}
	if err := c.Get("/same").Do(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if same.Get("Authorization") != "Bearer tok" || same.Get("X-Custom-Key") != "k" {
		t.Errorf("same-origin redirect lost headers: %v", same)
	}
	never := newClient(t, WithBaseURL(srv.URL), WithRedirectPolicy(NoRedirects))
	res, err := never.Get("/same").Send(t.Context())
	if err != nil || res.StatusCode() != http.StatusFound {
		t.Fatalf("NoRedirects: %v %v", res, err)
	}
	_ = res.Body().Close()
}

func TestRedirectDowngradeRefused(t *testing.T) {
	plain := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer plain.Close()
	tlsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL, http.StatusFound)
	}))
	defer tlsSrv.Close()
	c := newClient(t, WithBaseURL(tlsSrv.URL), WithTransport(tlsSrv.Client().Transport))
	if err := c.Get("/").Do(t.Context(), nil); !errors.Is(err, ErrDestinationNotAllowed) {
		t.Fatalf("err = %v", err)
	}
}

func TestSSRFGuard(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer srv.Close()
	_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
	var p int
	_, _ = fmt.Sscan(port, &p)

	c := newClient(t, WithSSRFGuard(SSRFConfig{Ports: []int{p}}), WithRetry(RetryPolicy{MinBackoff: time.Millisecond}))
	err := c.Get(srv.URL).Do(t.Context(), nil)
	if !errors.Is(err, ErrDestinationNotAllowed) || errs.Code(err) != codes.InvalidArgument || hits.Load() != 0 {
		t.Fatalf("loopback: err=%v code=%v hits=%d", err, errs.Code(err), hits.Load())
	}
	allowed := newClient(t, WithSSRFGuard(SSRFConfig{Ports: []int{p}, Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}}))
	if err := allowed.Get(srv.URL).Do(t.Context(), nil); err != nil || hits.Load() != 1 {
		t.Fatalf("allowlisted: err=%v", err)
	}
	wrongPort := newClient(t, WithSSRFGuard(SSRFConfig{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}}))
	if err := wrongPort.Get(srv.URL).Do(t.Context(), nil); !errors.Is(err, ErrDestinationNotAllowed) {
		t.Fatalf("port: err=%v", err)
	}
	if _, err := New(WithSSRFGuard(SSRFConfig{}), WithTransport(http.DefaultTransport)); err == nil {
		t.Fatal("SSRF guard with custom transport must fail")
	}
}

func TestPublicAddr(t *testing.T) {
	for addr, want := range map[string]bool{
		"8.8.8.8": true, "2606:4700::1111": true,
		"127.0.0.1": false, "10.1.2.3": false, "172.16.0.1": false, "192.168.1.1": false,
		"169.254.169.254": false, "100.100.100.200": false, "0.0.0.0": false, "::1": false,
		"fd00::1": false, "fe80::1": false, "::ffff:127.0.0.1": false, "64:ff9b::a9fe:a9fe": false,
		"224.0.0.1": false, "255.255.255.255": false,
	} {
		if got := PublicAddr(netip.MustParseAddr(addr)); got != want {
			t.Errorf("PublicAddr(%s) = %v", addr, got)
		}
	}
}

func TestRetry(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL), WithRetry(RetryPolicy{MinBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}))

	if err := c.Get("/").Do(t.Context(), nil); err != nil || n.Load() != 3 {
		t.Fatalf("GET: err=%v attempts=%d", err, n.Load())
	}
	n.Store(0)
	if err := c.Post("/").JSON(1).Do(t.Context(), nil); errs.Code(err) != codes.Unavailable || n.Load() != 1 {
		t.Fatalf("POST without key: err=%v attempts=%d", err, n.Load())
	}
	n.Store(0)
	if err := c.Post("/").IdempotencyKey("k1").JSON(1).Do(t.Context(), nil); err != nil || n.Load() != 3 {
		t.Fatalf("POST with key: err=%v attempts=%d", err, n.Load())
	}
	n.Store(0)
	// A one-shot reader cannot be replayed, so it is sent once.
	if err := c.Put("/").Body(io.NopCloser(strings.NewReader("x")), "text/plain").Do(t.Context(), nil); errs.Code(err) != codes.Unavailable || n.Load() != 1 {
		t.Fatalf("PUT one-shot body: err=%v attempts=%d", err, n.Load())
	}
}

func TestRetryAfterCap(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL), WithRetry(RetryPolicy{}))
	start := time.Now()
	err := c.Get("/").Do(t.Context(), nil)
	if StatusCode(err) != http.StatusTooManyRequests || n.Load() != 1 || time.Since(start) > 5*time.Second {
		t.Fatalf("err=%v attempts=%d", err, n.Load())
	}
}

func TestRetryNetworkAndAttemptTimeout(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
		}
	}))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL), WithTimeout(100*time.Millisecond),
		WithRetry(RetryPolicy{MinBackoff: time.Millisecond}))
	if err := c.Get("/").Do(t.Context(), nil); err != nil || n.Load() != 2 {
		t.Fatalf("attempt timeout not retried: err=%v attempts=%d", err, n.Load())
	}

	ln, _ := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens: connection refused
	var tries atomic.Int32
	down := newClient(t, WithBaseURL("http://"+addr), WithRetry(RetryPolicy{
		MinBackoff: time.Millisecond,
		Retryable: func(res *Response, err error) bool {
			tries.Add(1)
			return DefaultRetryable(res, err)
		},
	}))
	err := down.Get("/?key=SECRET").Do(t.Context(), nil)
	if errs.Code(err) != codes.Unavailable || tries.Load() != 2 || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("refused: err=%v code=%v tries=%d", err, errs.Code(err), tries.Load())
	}
}

func TestCancelNotMapped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL), WithRetry(RetryPolicy{}))
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(50*time.Millisecond, cancel)
	err := c.Get("/").Do(ctx, nil)
	if !errors.Is(err, context.Canceled) || errs.Code(err) != codes.Canceled {
		t.Fatalf("err=%v code=%v", err, errs.Code(err))
	}
}

func TestCircuitBreaker(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	var mu sync.Mutex
	var states []string
	c := newClient(t, WithBaseURL(srv.URL), WithCircuitBreaker(BreakerConfig{
		FailureThreshold: 2, ResetTimeout: time.Minute,
		OnStateChange: func(_, to string) { mu.Lock(); states = append(states, to); mu.Unlock() },
	}))
	for range 2 {
		_ = c.Get("/").Do(t.Context(), nil)
	}
	err := c.Get("/").Do(t.Context(), nil)
	if !errors.Is(err, ErrCircuitOpen) || errs.Code(err) != codes.Unavailable || n.Load() != 2 {
		t.Fatalf("breaker: err=%v hits=%d", err, n.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(states) == 0 || states[len(states)-1] != "open" {
		t.Fatalf("states = %v", states)
	}
}

func TestResponseBodyLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(item{Name: strings.Repeat("a", 4096)})
	}))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL), WithMaxResponseBody(1024))
	var out item
	if err := c.Get("/").Do(t.Context(), &out); err == nil {
		t.Fatal("oversized body accepted")
	}
	if err := c.Get("/").MaxResponseBody(0).Do(t.Context(), &out); err != nil || len(out.Name) != 4096 {
		t.Fatalf("per-request override: %v", err)
	}
}

func TestNoCookieJar(t *testing.T) {
	var cookies atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.Cookies()) > 0 {
			cookies.Add(1)
		}
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "user-a", Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode})
	}))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL))
	for range 2 {
		if err := c.Get("/").Do(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
	}
	if cookies.Load() != 0 {
		t.Fatal("cookie from one call was sent on the next")
	}
}

func TestConcurrentUse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(item{Name: r.URL.Query().Get("n")})
	}))
	defer srv.Close()
	c := newClient(t, WithBaseURL(srv.URL))
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			var out item
			err := c.Get("/").Query("n", fmt.Sprint(i)).Do(t.Context(), &out)
			if err != nil || out.Name != fmt.Sprint(i) {
				t.Errorf("%d: %v %+v", i, err, out)
			}
		})
	}
	wg.Wait()
}

func TestNewErrors(t *testing.T) {
	for _, u := range []string{"api.example.com", "ftp://x", "://"} {
		if _, err := New(WithBaseURL(u)); err == nil {
			t.Errorf("New(%q) succeeded", u)
		}
	}
	c := newClient(t)
	if err := c.Post("/").JSON(func() {}).Do(t.Context(), nil); err == nil {
		t.Error("unencodable JSON accepted")
	}
}
