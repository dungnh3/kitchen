package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dungnh3/kitchen/httpx"
	"github.com/lestrrat-go/httprc/v3"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
)

// JWKSConfig configures NewJWKS.
type JWKSConfig struct {
	Issuer   string // required; must match the token's iss
	Audience string // when set, must be in the token's aud
	URL      string // required; the JWKS endpoint (https in production)
	// Algs is the algorithm allow-list. Default: RS256, ES256, EdDSA.
	// Symmetric (HS*) algorithms are rejected: a JWKS holds public keys.
	Algs []jwa.SignatureAlgorithm
	// Client fetches the JWKS (kitchen/httpx: tracing, metrics, retries,
	// redirect and SSRF policy). Default: httpx.New(httpx.WithBaseURL(URL),
	// httpx.WithTimeout(10*time.Second)). The body is capped at 1 MiB.
	Client *httpx.Client
	// MinRefresh and MaxRefresh bound the background refresh interval
	// (Cache-Control is honored in between). Defaults 5m and 1h.
	MinRefresh, MaxRefresh time.Duration
	// MissRefresh is the minimum gap between forced refreshes triggered by
	// an unknown key ID (key rotation). Default 30s.
	MissRefresh time.Duration
	Leeway      time.Duration
	// Types, when set, lists the accepted JWT "typ" header values (RFC 8725
	// explicit typing), for example []string{"at+jwt"} for RFC 9068 access
	// tokens, so an ID token of the same issuer cannot be used as an access
	// token. Matching ignores case and an "application/" prefix. Empty: typ is
	// not checked (many IdPs send "JWT").
	Types []string
	Now   func() time.Time
}

// JWKS verifies JWTs with public keys from a JWKS URL. It refreshes the key
// set in the background; call Close to stop.
type JWKS struct {
	check       jwtCheck
	url         string
	cache       *jwk.Cache
	cancel      context.CancelFunc
	missRefresh time.Duration
	lastForced  atomic.Int64 // unix nanos
}

// NewJWKS fetches the key set once (bounded by ctx) and returns a verifier
// that keeps it fresh until Close.
func NewJWKS(ctx context.Context, cfg JWKSConfig) (*JWKS, error) {
	if cfg.Issuer == "" || cfg.URL == "" {
		return nil, errors.New("auth: jwks: issuer and URL are required")
	}
	algs := cfg.Algs
	if len(algs) == 0 {
		algs = []jwa.SignatureAlgorithm{jwa.RS256(), jwa.ES256(), jwa.EdDSA()}
	}
	for _, a := range algs {
		if strings.HasPrefix(a.String(), "HS") || a.String() == jwa.NoSignature().String() {
			return nil, fmt.Errorf("auth: jwks: algorithm %s is not allowed", a)
		}
	}
	minR, maxR := cfg.MinRefresh, cfg.MaxRefresh
	if minR <= 0 {
		minR = 5 * time.Minute
	}
	if maxR <= 0 {
		maxR = time.Hour
	}
	maxR = max(maxR, minR)

	wl := httprc.NewMapWhitelist().Add(cfg.URL)
	// The cache refreshes in the background until Close, past ctx. It starts
	// from Background, not ctx: a startup span in ctx must not parent every
	// later refresh.
	life, cancel := context.WithCancel(context.Background()) //nolint:contextcheck // see above
	cache, err := jwk.NewCache(life, httprc.NewClient(httprc.WithWhitelist(wl)))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("auth: jwks: %w", err)
	}
	client := cfg.Client
	if client == nil {
		if client, err = httpx.New(httpx.WithBaseURL(cfg.URL), httpx.WithTimeout(10*time.Second)); err != nil {
			_ = cache.Shutdown(context.WithoutCancel(ctx))
			cancel()
			return nil, fmt.Errorf("auth: jwks: %w", err)
		}
	}
	opts := []jwk.RegisterOption{
		jwk.WithMinInterval(minR), jwk.WithMaxInterval(maxR), jwk.WithHTTPClient(httpxDoer{client}),
	}
	if err := cache.Register(ctx, cfg.URL, opts...); err != nil {
		_ = cache.Shutdown(context.WithoutCancel(ctx))
		cancel()
		return nil, fmt.Errorf("auth: jwks: initial fetch of %s: %w", cfg.URL, err)
	}
	missR := cfg.MissRefresh
	if missR <= 0 {
		missR = 30 * time.Second
	}
	return &JWKS{
		check: jwtCheck{
			issuer: cfg.Issuer, audience: cfg.Audience, algs: algs, types: normTypes(cfg.Types),
			leeway: leewayOr(cfg.Leeway), now: nowOr(cfg.Now),
		},
		url: cfg.URL, cache: cache, cancel: cancel, missRefresh: missR,
	}, nil
}

// Issuer implements Verifier.
func (j *JWKS) Issuer() string { return j.check.issuer }

// Verify implements Verifier.
func (j *JWKS) Verify(ctx context.Context, token string) (*Principal, error) {
	return j.check.verify(ctx, token, j)
}

// Close stops background refreshing and waits for it (bounded by ctx).
func (j *JWKS) Close(ctx context.Context) error {
	err := j.cache.Shutdown(ctx)
	j.cancel()
	return err
}

func (j *JWKS) key(ctx context.Context, kid string, alg jwa.SignatureAlgorithm) (any, error) {
	set, err := j.cache.Lookup(ctx, j.url)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errKeySourceUnavailable, err)
	}
	k, ok := findKey(set, kid)
	if !ok && j.mayForceRefresh() {
		if set, err = j.cache.Refresh(ctx, j.url); err == nil {
			k, ok = findKey(set, kid)
		}
	}
	if !ok {
		return nil, fmt.Errorf("auth: unknown key id %q", kid)
	}
	if ka, has := k.Algorithm(); has && ka.String() != alg.String() {
		return nil, fmt.Errorf("auth: key %q is for %s, token uses %s", kid, ka, alg)
	}
	if use, has := k.KeyUsage(); has && use != "sig" {
		return nil, fmt.Errorf("auth: key %q is not a signing key", kid)
	}
	return k, nil
}

func findKey(set jwk.Set, kid string) (jwk.Key, bool) {
	if kid != "" {
		return set.LookupKeyID(kid)
	}
	if set.Len() == 1 {
		return set.Key(0)
	}
	return nil, false
}

// mayForceRefresh rate-limits refreshes caused by unknown key IDs, so junk
// tokens cannot hammer the identity provider.
func (j *JWKS) mayForceRefresh() bool {
	now := time.Now().UnixNano()
	last := j.lastForced.Load()
	if now-last < j.missRefresh.Nanoseconds() {
		return false
	}
	return j.lastForced.CompareAndSwap(last, now)
}

// maxJWKSBody caps a JWKS response; real key sets are a few KiB.
const maxJWKSBody = 1 << 20

// httpxDoer fetches the JWKS through kitchen/httpx for jwx (httprc.HTTPClient).
type httpxDoer struct{ c *httpx.Client }

func (d httpxDoer) Do(req *http.Request) (*http.Response, error) {
	r := d.c.NewRequest(req.Method, req.URL.String())
	for k, vs := range req.Header {
		for _, v := range vs {
			r.Header(k, v)
		}
	}
	res, err := r.Send(req.Context())
	if err != nil {
		return nil, err
	}
	body := res.Body()
	return &http.Response{
		Status:     http.StatusText(res.StatusCode()),
		StatusCode: res.StatusCode(),
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     res.Header(),
		Body: struct {
			io.Reader
			io.Closer
		}{io.LimitReader(body, maxJWKSBody), body},
		ContentLength: -1,
		Request:       req,
	}, nil
}
