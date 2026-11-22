package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dungnh3/kitchen/log"

	"github.com/dungnh3/kitchen/cache"
	"github.com/dungnh3/kitchen/errs"
	"github.com/redis/go-redis/v9"
)

// IntrospectFunc asks an authority about a token. It returns the principal
// for an active token, an error wrapping ErrTokenInvalid for an inactive or
// unknown token, and any other error when the authority could not answer.
type IntrospectFunc func(ctx context.Context, token string) (*Principal, error)

// IntrospectionConfig configures NewIntrospection.
type IntrospectionConfig struct {
	// Issuer routes JWTs with this iss here. Leave empty for a verifier of
	// opaque tokens (Config.Opaque).
	Issuer     string
	Introspect IntrospectFunc // required
	// TTL caps how long an active result is reused (never past the token's
	// expiry). Default 30s.
	TTL time.Duration
	// NegativeTTL is how long an inactive result is reused. Default 5s.
	NegativeTTL time.Duration
	// MaxEntries bounds the in-process cache. Default 100_000.
	MaxEntries int
	// Redis, when set, shares results across instances through package
	// kitchen/cache (keys are SHA-256 hashes of tokens, under Prefix), so a
	// fleet asks the authority once per token and TTL. Use redisx.Open.
	// CacheKey is then required.
	Redis redis.UniversalClient
	// CacheKey (at least 32 random bytes, shared by the fleet, kept secret)
	// authenticates every entry written to Redis with HMAC-SHA256 and binds
	// it to its token hash. Whoever can write to Redis still cannot forge a
	// principal or move one entry to another token: an entry that fails the
	// check is ignored and the authority is asked again. Required with Redis.
	CacheKey []byte
	// Prefix of the Redis keys, required with Redis. Give every verifier its
	// own prefix (for example "orders:introspection:idp-a:"): verifiers with
	// different rules (issuer, audience, client credentials) must never share
	// cached answers.
	Prefix string
	// Timeout bounds one introspection call, which is shared by all
	// concurrent requests for the same token. Default 2s.
	Timeout time.Duration
	Now     func() time.Time
	Logger  *log.Logger
}

// Introspection verifies tokens through IntrospectFunc, cached with package
// kitchen/cache (in-process, plus Redis when configured). Concurrent lookups
// of one token share a single call. An authority failure is never cached and
// yields ErrAuthUnavailable (fail closed). Call Close when done.
type Introspection struct {
	cfg   IntrospectionConfig
	cache *cache.Cache[introResult]
}

// introResult is one cached answer. Until is when it stops being valid,
// measured with IntrospectionConfig.Now (min of TTL and the token expiry).
type introResult struct {
	K       string     `json:"k"`           // hash of the token it answers for
	P       *Principal `json:"p,omitempty"` // nil: inactive or expired
	Expired bool       `json:"expired,omitempty"`
	Until   time.Time  `json:"until"`
}

// macCodec encodes cache entries for Redis as HMAC-SHA256(key, json) || json
// and rejects entries whose MAC does not match. With an empty key it is
// plain JSON (no Redis configured, the codec is unused).
type macCodec struct{ key []byte }

func (c macCodec) Marshal(v introResult) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil || len(c.key) == 0 {
		return data, err
	}
	m := hmac.New(sha256.New, c.key)
	m.Write(data)
	return append(m.Sum(nil), data...), nil
}

func (c macCodec) Unmarshal(b []byte, v *introResult) error {
	if len(c.key) > 0 {
		if len(b) < sha256.Size {
			return errors.New("auth: introspection cache entry too short")
		}
		m := hmac.New(sha256.New, c.key)
		m.Write(b[sha256.Size:])
		if !hmac.Equal(m.Sum(nil), b[:sha256.Size]) {
			return errors.New("auth: introspection cache entry failed authentication")
		}
		b = b[sha256.Size:]
	}
	return json.Unmarshal(b, v)
}

// NewIntrospection returns an Introspection verifier.
func NewIntrospection(cfg IntrospectionConfig) (*Introspection, error) {
	if cfg.Introspect == nil {
		return nil, errors.New("auth: introspection: Introspect is required")
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 30 * time.Second
	}
	if cfg.NegativeTTL <= 0 {
		cfg.NegativeTTL = 5 * time.Second
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 100_000
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.Redis != nil && cfg.Prefix == "" {
		return nil, errors.New("auth: introspection: Prefix is required with Redis")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "auth:introspection:" // in-process only: never shared
	}
	if cfg.Redis != nil && len(cfg.CacheKey) < 32 {
		return nil, errors.New("auth: introspection: CacheKey of at least 32 bytes is required with Redis")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	cfg.Now = nowOr(cfg.Now)
	ttl := max(cfg.TTL, cfg.NegativeTTL)
	c, err := cache.New(cache.Config[introResult]{
		Local:  &cache.LocalConfig{MaxSize: cfg.MaxEntries, TTL: ttl},
		Redis:  cfg.Redis,
		Prefix: cfg.Prefix,
		TTL:    ttl,
		Jitter: -1, // Until decides validity
		Codec:  macCodec{key: cfg.CacheKey},
		Logger: cfg.Logger,
		Name:   "auth.introspection",
	})
	if err != nil {
		return nil, fmt.Errorf("auth: introspection: %w", err)
	}
	return &Introspection{cfg: cfg, cache: c}, nil
}

// Close releases the cache (not the Redis client).
func (in *Introspection) Close() { in.cache.Close() }

// Issuer implements Verifier.
func (in *Introspection) Issuer() string { return in.cfg.Issuer }

// Verify implements Verifier.
func (in *Introspection) Verify(ctx context.Context, token string) (*Principal, error) {
	sum := sha256.Sum256([]byte(token))
	key := hex.EncodeToString(sum[:])
	load := func(lctx context.Context, _ string) (introResult, error) { return in.load(lctx, token, key) }
	var r introResult
	fresh := false
	for range 2 {
		var err error
		r, err = in.cache.GetOrLoad(ctx, key, load)
		switch {
		case errors.Is(err, ErrAuthUnavailable):
			// First: the authority failed. Its cause may itself carry a
			// NotFound code (an IdP 404), which must not look like a
			// tampered cache entry.
			return nil, err
		case errors.Is(err, cache.ErrNotFound):
			// cache.ErrNotFound is errs.ErrNotFound: any NotFound-coded error.
			// Never written by this package (no negative caching): tampered.
			in.cfg.Logger.WarnContext(ctx, "auth: unexpected negative introspection cache entry, ignoring it")
			_ = in.cache.Delete(ctx, key)
			continue
		case err != nil:
			return nil, errs.Wrap(err, ErrAuthUnavailable) // the caller gave up, or the cache failed
		}
		if r.K != key {
			in.cfg.Logger.WarnContext(ctx, "auth: introspection cache entry belongs to another token, ignoring it")
			_ = in.cache.Delete(ctx, key)
			continue
		}
		if in.cfg.Now().Before(r.Until) {
			fresh = true
			break
		}
		_ = in.cache.Delete(ctx, key) // stale (TTL or token expiry): ask again once
	}
	if !fresh {
		// The cache did not give a usable answer twice: ask the authority
		// directly rather than trust it.
		var err error
		if r, err = in.load(ctx, token, key); err != nil {
			return nil, err
		}
	}
	switch {
	case r.Expired:
		return nil, ErrTokenExpired
	case r.P == nil:
		return nil, ErrTokenInvalid
	}
	return r.P.clone(), nil
}

// load asks the authority. Inactive and expired answers are cached for
// NegativeTTL; failures are returned and not cached.
func (in *Introspection) load(ctx context.Context, token, key string) (introResult, error) {
	// ctx is detached from the first caller by package cache; bound it.
	cctx, cancel := context.WithTimeout(ctx, in.cfg.Timeout)
	defer cancel()
	p, err := in.cfg.Introspect(cctx, token)
	now := in.cfg.Now()
	negative := introResult{K: key, Until: now.Add(in.cfg.NegativeTTL)}
	switch {
	case err == nil && p != nil:
		until := now.Add(in.cfg.TTL)
		if !p.ExpiresAt.IsZero() && p.ExpiresAt.Before(until) {
			until = p.ExpiresAt
		}
		if !now.Before(until) {
			negative.Expired = true
			return negative, nil
		}
		return introResult{K: key, P: p, Until: until}, nil
	case err == nil, errors.Is(err, ErrTokenInvalid):
		return negative, nil
	case errors.Is(err, ErrTokenExpired):
		negative.Expired = true
		return negative, nil
	default:
		return introResult{}, errs.Wrap(err, ErrAuthUnavailable)
	}
}

// All returns a Verifier that requires every verifier to accept the token,
// for example a JWKS signature check followed by an introspection
// revocation check. It reports the first verifier's issuer and principal.
func All(vs ...Verifier) Verifier { return allVerifier(vs) }

type allVerifier []Verifier

func (a allVerifier) Issuer() string {
	if len(a) == 0 {
		return ""
	}
	return a[0].Issuer()
}

func (a allVerifier) Verify(ctx context.Context, token string) (*Principal, error) {
	if len(a) == 0 {
		return nil, ErrTokenInvalid
	}
	var first *Principal
	for i, v := range a {
		p, err := v.Verify(ctx, token)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			first = p
		}
	}
	return first, nil
}
