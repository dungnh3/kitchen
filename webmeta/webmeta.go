// Package webmeta fetches a web page and extracts its title, description,
// preview image and icon (Open Graph, Twitter cards and HTML fallbacks).
//
// The fetcher is safe to point at untrusted URLs (for example URLs produced by
// an LLM): by default it only dials publicly routable addresses, checked after
// DNS resolution and on every redirect, caps the body size and times out.
package webmeta

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html/charset"
)

// ErrNotHTML is returned when the response is not an HTML document.
var ErrNotHTML = errors.New("webmeta: response is not HTML")

// StatusError is returned for non-2xx responses.
type StatusError struct{ Code int }

func (e *StatusError) Error() string { return fmt.Sprintf("webmeta: HTTP status %d", e.Code) }

// Config configures a [Fetcher]. Zero values are replaced by defaults.
type Config struct {
	// Timeout bounds the whole fetch (default 10s).
	Timeout time.Duration
	// MaxBytes caps how much of the body is read (default 2 MiB). Longer
	// documents are parsed up to the cap.
	MaxBytes int64
	// UserAgent default "kitchen-webmeta/1 (+https://github.com/dungnh3/kitchen)".
	UserAgent string
	// MaxRedirects default 5.
	MaxRedirects int
	// AllowPrivate disables the address guard. Only for tests and trusted
	// internal targets.
	AllowPrivate bool
	// Transport overrides the HTTP transport. When set, the address guard is
	// NOT applied; the caller is responsible for it.
	Transport http.RoundTripper
}

// Fetcher fetches page metadata. It is safe for concurrent use.
type Fetcher struct {
	cfg    Config
	client *http.Client
	tr     *http.Transport // owned transport, nil when Config.Transport set
}

// New returns a Fetcher.
func New(cfg Config) *Fetcher {
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
	f := &Fetcher{cfg: cfg}
	rt := cfg.Transport
	if rt == nil {
		d := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
		if !cfg.AllowPrivate {
			d.Control = dialControl
		}
		f.tr = &http.Transport{
			Proxy:                 nil, // a proxy would bypass the address guard
			DialContext:           d.DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: cfg.Timeout,
		}
		rt = f.tr
	}
	f.client = &http.Client{
		Transport: rt,
		Timeout:   cfg.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > cfg.MaxRedirects {
				return fmt.Errorf("webmeta: stopped after %d redirects", cfg.MaxRedirects)
			}
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return fmt.Errorf("webmeta: redirect to unsupported scheme %q", req.URL.Scheme)
			}
			return nil
		},
	}
	return f
}

// Close releases idle connections held by the Fetcher's own transport.
func (f *Fetcher) Close() {
	if f.tr != nil {
		f.tr.CloseIdleConnections()
	}
}

// Fetch downloads rawURL and parses its metadata.
func (f *Fetcher) Fetch(ctx context.Context, rawURL string) (*Metadata, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("webmeta: parse URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("webmeta: unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("webmeta: URL has no host")
	}
	if u.User != nil {
		return nil, errors.New("webmeta: URLs with credentials are not allowed")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("webmeta: %w", err)
	}
	req.Header.Set("User-Agent", f.cfg.UserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1")
	resp, err := f.client.Do(req)
	if err != nil {
		if errors.Is(err, ErrForbiddenAddress) {
			return nil, fmt.Errorf("webmeta: fetch %s: %w", u.Host, ErrForbiddenAddress)
		}
		return nil, fmt.Errorf("webmeta: fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, &StatusError{Code: resp.StatusCode}
	}
	ct := resp.Header.Get("Content-Type")
	if mt, _, err := mime.ParseMediaType(ct); ct != "" && (err != nil || (mt != "text/html" && mt != "application/xhtml+xml")) {
		return nil, fmt.Errorf("%w: %s", ErrNotHTML, ct)
	}
	body, err := charset.NewReader(io.LimitReader(resp.Body, f.cfg.MaxBytes), ct)
	if err != nil {
		return nil, fmt.Errorf("webmeta: charset: %w", err)
	}
	return Parse(resp.Request.URL, body)
}
