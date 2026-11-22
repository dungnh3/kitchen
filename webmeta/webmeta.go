// Package webmeta fetches a web page and extracts its title, description,
// preview image and icon (Open Graph, Twitter cards and HTML fallbacks).
//
// The fetcher is safe to point at untrusted URLs (for example URLs produced by
// an LLM). It fetches through kitchen's httpx client with the SSRF guard on:
// only public addresses on ports 80 and 443 are dialled, checked after DNS
// resolution and on every redirect, proxies are ignored, https is never
// downgraded to http on a redirect, the body is capped and the request times
// out. Errors are errs errors: httpx.ErrDestinationNotAllowed for a refused
// address, the httpx status mapping (errs.ErrNotFound, ...) for non-2xx
// responses, [ErrInvalidURL] and [ErrNotHTML].
package webmeta

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html/charset"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/httpx"
)

// ErrNotHTML is returned when the response is not an HTML document.
var ErrNotHTML = errs.New(codes.InvalidArgument, "NOT_HTML", errs.Message("the URL does not point to an HTML page"))

// ErrInvalidURL is returned for a URL that is not an absolute http(s) URL
// without credentials.
var ErrInvalidURL = errs.New(codes.InvalidArgument, "INVALID_URL", errs.Message("the URL must be an absolute http or https URL"))

// Config configures a [Fetcher]. The zero value is a safe default.
type Config struct {
	// Timeout bounds one fetch, redirects included. Default 10s.
	Timeout time.Duration
	// MaxBytes caps how much of the body is read. Default 2 MiB.
	MaxBytes int64
	// UserAgent sent with requests. Default "kitchen-webmeta/1 (+https://github.com/dungnh3/kitchen)".
	UserAgent string
	// MaxRedirects followed. Default 5.
	MaxRedirects int
	// Ports allowed as destination. Default 80 and 443.
	Ports []int
	// Allow lists non-public prefixes that may still be fetched (tests, a
	// known internal site). Default none: only public addresses.
	Allow []netip.Prefix
	// ClientOptions are extra httpx options (logger, tracer/meter
	// providers, retry). httpx.WithTransport is rejected: it would bypass
	// the SSRF guard.
	ClientOptions []httpx.Option
}

// Fetcher fetches pages. It is safe for concurrent use.
type Fetcher struct {
	cfg    Config
	client *httpx.Client
}

// New returns a Fetcher.
func New(cfg Config) (*Fetcher, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 2 << 20
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "kitchen-webmeta/1 (+https://github.com/dungnh3/kitchen)"
	}
	if cfg.MaxRedirects <= 0 {
		cfg.MaxRedirects = 5
	}
	opts := append([]httpx.Option{}, cfg.ClientOptions...)
	// Ours last, so ClientOptions cannot loosen them.
	opts = append(opts,
		httpx.WithSSRFGuard(httpx.SSRFConfig{Ports: cfg.Ports, Allow: cfg.Allow}),
		httpx.WithTimeout(cfg.Timeout),
		httpx.WithMaxResponseBody(cfg.MaxBytes),
		httpx.WithRedirectPolicy(httpx.SafeRedirectPolicy(cfg.MaxRedirects)),
		httpx.WithHeader("User-Agent", cfg.UserAgent),
		httpx.WithHeader("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1"),
	)
	c, err := httpx.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("webmeta: %w", err)
	}
	return &Fetcher{cfg: cfg, client: c}, nil
}

// Close releases idle connections.
func (f *Fetcher) Close() { f.client.CloseIdleConnections() }

// Fetch downloads rawURL and parses its metadata. Metadata.URL is the final
// URL after redirects, with query values redacted.
func (f *Fetcher) Fetch(ctx context.Context, rawURL string) (*Metadata, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return nil, errs.Wrap(fmt.Errorf("webmeta: bad URL %q", httpx.RedactURL(orEmpty(u))), ErrInvalidURL)
	}
	resp, err := f.client.Get(u.String()).Send(ctx)
	if err != nil {
		return nil, fmt.Errorf("webmeta: fetch %s: %w", u.Host, err)
	}
	body := resp.Body()
	defer func() { _ = body.Close() }()
	ct := resp.Header().Get("Content-Type")
	if mt, _, err := mime.ParseMediaType(ct); ct != "" && (err != nil || (mt != "text/html" && mt != "application/xhtml+xml")) {
		return nil, errs.Wrap(fmt.Errorf("webmeta: %s is %s", u.Host, ct), ErrNotHTML)
	}
	r, err := charset.NewReader(io.LimitReader(body, f.cfg.MaxBytes), ct)
	if err != nil {
		return nil, fmt.Errorf("webmeta: charset: %w", err)
	}
	final, err := url.Parse(resp.URL())
	if err != nil || final.Host == "" {
		final = u
	}
	return Parse(final, r)
}

func orEmpty(u *url.URL) *url.URL {
	if u == nil {
		return &url.URL{}
	}
	return u
}

// Result is the outcome of one URL in [Fetcher.FetchAll].
type Result struct {
	URL      string
	Metadata *Metadata // nil when Err is set
	Err      error
}

// FetchAll fetches urls with at most concurrency fetches in flight (4 when
// concurrency <= 0) and returns one Result per URL, in input order. A failed
// URL does not stop the others (best effort, e.g. citation previews). Once
// ctx is done no new fetch starts and the remaining Results carry ctx's
// error. FetchAll returns after every started fetch has finished, so no
// goroutine outlives it.
func (f *Fetcher) FetchAll(ctx context.Context, urls []string, concurrency int) []Result {
	if concurrency <= 0 {
		concurrency = 4
	}
	out := make([]Result, len(urls))
	var g errgroup.Group
	g.SetLimit(concurrency)
	for i, u := range urls {
		out[i].URL = u
		if err := ctx.Err(); err != nil {
			out[i].Err = err
			continue
		}
		g.Go(func() error {
			out[i].Metadata, out[i].Err = f.Fetch(ctx, u)
			return nil // best effort: never cancel the others
		})
	}
	_ = g.Wait() // the goroutines never return an error
	return out
}
