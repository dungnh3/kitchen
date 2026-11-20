package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

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
	// HTTPClient fetches the JWKS. Default: jwx's client with timeouts.
	HTTPClient *http.Client
	// MinRefresh and MaxRefresh bound the background refresh interval
	// (Cache-Control is honored in between). Defaults 5m and 1h.
	MinRefresh, MaxRefresh time.Duration
	// MissRefresh is the minimum gap between forced refreshes triggered by
	// an unknown key ID (key rotation). Default 30s.
	MissRefresh time.Duration
	Leeway      time.Duration
	Now         func() time.Time
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
	life, cancel := context.WithCancel(context.Background())
	cache, err := jwk.NewCache(life, httprc.NewClient(httprc.WithWhitelist(wl)))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("auth: jwks: %w", err)
	}
	opts := []jwk.RegisterOption{jwk.WithMinInterval(minR), jwk.WithMaxInterval(maxR)}
	if cfg.HTTPClient != nil {
		opts = append(opts, jwk.WithHTTPClient(cfg.HTTPClient))
	}
	if err := cache.Register(ctx, cfg.URL, opts...); err != nil {
		_ = cache.Shutdown(context.Background())
		cancel()
		return nil, fmt.Errorf("auth: jwks: initial fetch of %s: %w", cfg.URL, err)
	}
	missR := cfg.MissRefresh
	if missR <= 0 {
		missR = 30 * time.Second
	}
	return &JWKS{
		check: jwtCheck{
			issuer: cfg.Issuer, audience: cfg.Audience, algs: algs,
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
