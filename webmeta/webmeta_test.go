package webmeta_test

import (
	"context"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/httpx"
	"github.com/dungnh3/kitchen/webmeta"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const page = `<!doctype html><html><head>
<base href="/sub/">
<title>  Plain
 Title </title>
<meta name="description" content="plain desc">
<meta property="og:title" content="OG Title">
<meta property="og:site_name" content="Example">
<meta name="twitter:image" content="https://cdn.example.com/tw.png">
<meta property="og:image" content="img/og.png">
<link rel="icon" href="/favicon-32.png">
<link rel="apple-touch-icon" href="/apple.png">
<link rel="canonical" href="https://example.com/canonical">
</head><body><meta property="og:description" content="ignored after body"></body></html>`

func TestParse(t *testing.T) {
	base, _ := url.Parse("https://example.com/a/page")
	m, err := webmeta.Parse(base, strings.NewReader(page))
	if err != nil {
		t.Fatal(err)
	}
	want := webmeta.Metadata{
		URL:          "https://example.com/sub/",
		CanonicalURL: "https://example.com/canonical",
		Title:        "OG Title",
		Description:  "plain desc",
		SiteName:     "Example",
		Image:        "https://example.com/sub/img/og.png",
		Icon:         "https://example.com/apple.png",
	}
	if *m != want {
		t.Fatalf("got  %+v\nwant %+v", *m, want)
	}
}

func TestParseFallbacks(t *testing.T) {
	base, _ := url.Parse("https://example.com/x")
	m, err := webmeta.Parse(base, strings.NewReader(`<html><head><title>T</title>
<meta property="og:image" content="javascript:alert(1)"></head></html>`))
	if err != nil {
		t.Fatal(err)
	}
	if m.Title != "T" || m.Image != "" || m.Icon != "https://example.com/favicon.ico" {
		t.Fatalf("got %+v", m)
	}
}

func TestParseParagraphFallback(t *testing.T) {
	base, _ := url.Parse("https://example.com/x")
	long := strings.Repeat("word ", 100)
	m, err := webmeta.Parse(base, strings.NewReader(`<html><head><title>T</title></head><body>
<nav><p>Home</p></nav>
<script>var p = "<p>not a paragraph, it is script content</p>";</script>
<p>This first real paragraph is <b>long enough</b> to describe the page.</p>
<p>Second paragraph is ignored.</p>`))
	if err != nil {
		t.Fatal(err)
	}
	if m.Description != "This first real paragraph is long enough to describe the page." {
		t.Fatalf("description = %q", m.Description)
	}
	m, _ = webmeta.Parse(base, strings.NewReader("<body><p>"+long+"</p>"))
	if n := utf8.RuneCountInString(m.Description); n > 301 || !strings.HasSuffix(m.Description, "…") {
		t.Fatalf("not truncated: %d runes", n)
	}
	// A head description wins and the body is not read.
	m, _ = webmeta.Parse(base, strings.NewReader(`<head><meta name="description" content="meta"></head><body><p>`+long+`</p>`))
	if m.Description != "meta" {
		t.Fatalf("description = %q", m.Description)
	}
}

func serve(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func TestFetch(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/latin1", http.StatusFound)
		case "/latin1":
			w.Header().Set("Content-Type", "text/html; charset=iso-8859-1")
			_, _ = w.Write([]byte("<html><head><title>Caf\xe9</title></head></html>"))
		case "/pdf":
			w.Header().Set("Content-Type", "application/pdf")
		case "/missing":
			http.NotFound(w, r)
		case "/big":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<html><head><title>Big</title>" + strings.Repeat("<!-- pad -->", 10000)))
		}
	})
	f := newLocalFetcher(t, srv, 4096)
	ctx := context.Background()

	m, err := f.Fetch(ctx, srv.URL+"/redirect")
	if err != nil || m.Title != "Café" || m.URL != srv.URL+"/latin1" {
		t.Fatalf("Fetch redirect = %+v, %v", m, err)
	}
	if _, err := f.Fetch(ctx, srv.URL+"/pdf"); !errors.Is(err, webmeta.ErrNotHTML) {
		t.Fatalf("pdf err = %v", err)
	}
	if _, err := f.Fetch(ctx, srv.URL+"/missing"); !errors.Is(err, errs.ErrNotFound) || httpx.StatusCode(err) != 404 {
		t.Fatalf("404 err = %v", err)
	}
	if m, err := f.Fetch(ctx, srv.URL+"/big"); err != nil || m.Title != "Big" {
		t.Fatalf("big = %+v, %v", m, err)
	}
	for _, bad := range []string{"ftp://x/y", "file:///etc/passwd", "http://", "http://u:p@example.com/"} {
		if _, err := f.Fetch(ctx, bad); !errors.Is(err, webmeta.ErrInvalidURL) {
			t.Errorf("Fetch(%q) = %v, want ErrInvalidURL", bad, err)
		}
	}
}

func TestFetchAll(t *testing.T) {
	var inFlight, peak atomic.Int32
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		if r.URL.Path == "/missing" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprintf(w, "<title>%s</title>", html.EscapeString(strings.TrimPrefix(r.URL.Path, "/")))
	})
	f := newLocalFetcher(t, srv, 0)
	urls := []string{srv.URL + "/a", srv.URL + "/missing", "ftp://bad", srv.URL + "/b", srv.URL + "/c"}
	res := f.FetchAll(context.Background(), urls, 2)
	if len(res) != len(urls) {
		t.Fatalf("len = %d", len(res))
	}
	for i, want := range []string{"a", "", "", "b", "c"} {
		r := res[i]
		if r.URL != urls[i] {
			t.Errorf("%d: URL %q, want %q (order)", i, r.URL, urls[i])
		}
		if want == "" {
			if r.Err == nil || r.Metadata != nil {
				t.Errorf("%d: want error, got %+v", i, r)
			}
			continue
		}
		if r.Err != nil || r.Metadata.Title != want {
			t.Errorf("%d: got %+v", i, r)
		}
	}
	if p := peak.Load(); p > 2 {
		t.Errorf("peak concurrency %d > 2", p)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, r := range f.FetchAll(ctx, urls[:2], 0) {
		if !errors.Is(r.Err, context.Canceled) {
			t.Errorf("cancelled: %+v", r)
		}
	}
}

func TestFetchBlocksPrivateByDefault(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<title>secret</title>"))
	})
	f, err := webmeta.New(webmeta.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, u := range []string{srv.URL, strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)} {
		if _, err := f.Fetch(context.Background(), u); !errors.Is(err, httpx.ErrDestinationNotAllowed) {
			t.Errorf("Fetch(%s) err = %v, want ErrDestinationNotAllowed", u, err)
		}
	}
	// Allowing the address is not enough: the port must be allowed too.
	f2, err := webmeta.New(webmeta.Config{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}})
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	if _, err := f2.Fetch(context.Background(), srv.URL); !errors.Is(err, httpx.ErrDestinationNotAllowed) {
		t.Errorf("non-80/443 port err = %v, want ErrDestinationNotAllowed", err)
	}
}

// newLocalFetcher allows the httptest server's loopback address and port.
func newLocalFetcher(t *testing.T, srv *httptest.Server, maxBytes int64) *webmeta.Fetcher {
	t.Helper()
	ap := netip.MustParseAddrPort(srv.Listener.Addr().String())
	f, err := webmeta.New(webmeta.Config{
		MaxBytes: maxBytes,
		Allow:    []netip.Prefix{netip.PrefixFrom(ap.Addr(), ap.Addr().BitLen())},
		Ports:    []int{int(ap.Port())},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.Close)
	return f
}

func ExampleParse() {
	base, _ := url.Parse("https://example.com/post/1")
	m, _ := webmeta.Parse(base, strings.NewReader(`<head>
<meta property="og:title" content="Hello">
<meta property="og:image" content="/cover.png"></head>`))
	fmt.Println(m.Title, m.Image, m.Icon)
	// Output: Hello https://example.com/cover.png https://example.com/favicon.ico
}

func ExampleFetcher_Fetch() {
	f, err := webmeta.New(webmeta.Config{}) // SSRF guard on by default
	if err != nil {
		return
	}
	defer f.Close()
	m, err := f.Fetch(context.Background(), "http://169.254.169.254/latest/meta-data/")
	fmt.Println(m, errors.Is(err, httpx.ErrDestinationNotAllowed))
	// Output: <nil> true
}
