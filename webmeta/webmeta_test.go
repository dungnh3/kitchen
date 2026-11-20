package webmeta_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"go.uber.org/goleak"

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
	f := webmeta.New(webmeta.Config{AllowPrivate: true, MaxBytes: 4096})
	defer f.Close()
	ctx := context.Background()

	m, err := f.Fetch(ctx, srv.URL+"/redirect")
	if err != nil || m.Title != "Café" || m.URL != srv.URL+"/latin1" {
		t.Fatalf("Fetch redirect = %+v, %v", m, err)
	}
	if _, err := f.Fetch(ctx, srv.URL+"/pdf"); !errors.Is(err, webmeta.ErrNotHTML) {
		t.Fatalf("pdf err = %v", err)
	}
	var se *webmeta.StatusError
	if _, err := f.Fetch(ctx, srv.URL+"/missing"); !errors.As(err, &se) || se.Code != 404 {
		t.Fatalf("404 err = %v", err)
	}
	if m, err := f.Fetch(ctx, srv.URL+"/big"); err != nil || m.Title != "Big" {
		t.Fatalf("big = %+v, %v", m, err)
	}
	for _, bad := range []string{"ftp://x/y", "file:///etc/passwd", "http://", "http://u:p@example.com/"} {
		if _, err := f.Fetch(ctx, bad); err == nil {
			t.Errorf("Fetch(%q): want error", bad)
		}
	}
}

func TestFetchBlocksPrivateByDefault(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<title>secret</title>"))
	})
	f := webmeta.New(webmeta.Config{})
	defer f.Close()
	for _, u := range []string{srv.URL, strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)} {
		if _, err := f.Fetch(context.Background(), u); !errors.Is(err, webmeta.ErrForbiddenAddress) {
			t.Errorf("Fetch(%s) err = %v, want ErrForbiddenAddress", u, err)
		}
	}
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
	f := webmeta.New(webmeta.Config{}) // SSRF guard on by default
	defer f.Close()
	m, err := f.Fetch(context.Background(), "http://169.254.169.254/latest/meta-data/")
	fmt.Println(m, errors.Is(err, webmeta.ErrForbiddenAddress))
	// Output: <nil> true
}
