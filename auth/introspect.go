package auth

import (
	"container/list"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"golang.org/x/sync/singleflight"
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
	// MaxEntries bounds the cache (LRU). Default 100_000.
	MaxEntries int
	// Timeout bounds one introspection call, which is shared by all
	// concurrent requests for the same token. Default 2s.
	Timeout time.Duration
	Now     func() time.Time
}

// Introspection verifies tokens through IntrospectFunc with a TTL cache.
// Concurrent lookups of one token share a single call. An authority failure
// is never cached and yields ErrAuthUnavailable (fail closed).
type Introspection struct {
	cfg   IntrospectionConfig
	group singleflight.Group

	mu    sync.Mutex
	items map[[32]byte]*list.Element
	lru   list.List
}

type introEntry struct {
	key     [32]byte
	p       *Principal // nil: inactive
	expires time.Time
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
	cfg.Now = nowOr(cfg.Now)
	return &Introspection{cfg: cfg, items: make(map[[32]byte]*list.Element)}, nil
}

// Issuer implements Verifier.
func (in *Introspection) Issuer() string { return in.cfg.Issuer }

// Verify implements Verifier.
func (in *Introspection) Verify(ctx context.Context, token string) (*Principal, error) {
	key := sha256.Sum256([]byte(token))
	if p, hit, active := in.lookup(key); hit {
		if !active {
			return nil, ErrTokenInvalid
		}
		return p, nil
	}

	ch := in.group.DoChan(string(key[:]), func() (any, error) {
		// Detached from the first caller so its cancellation does not fail
		// the others sharing this call; bounded by Timeout instead.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), in.cfg.Timeout)
		defer cancel()
		p, err := in.cfg.Introspect(cctx, token)
		switch {
		case err == nil && p != nil:
			in.store(key, p)
			return p, nil
		case err == nil:
			in.store(key, nil)
			return nil, ErrTokenInvalid
		case errors.Is(err, ErrTokenInvalid) || errors.Is(err, ErrTokenExpired):
			in.store(key, nil)
			return nil, err
		default:
			return nil, errs.Wrap(err, ErrAuthUnavailable)
		}
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(*Principal), nil
	case <-ctx.Done():
		return nil, errs.Wrap(ctx.Err(), ErrAuthUnavailable)
	}
}

func (in *Introspection) lookup(key [32]byte) (p *Principal, hit, active bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	el, ok := in.items[key]
	if !ok {
		return nil, false, false
	}
	e := el.Value.(*introEntry)
	if !in.cfg.Now().Before(e.expires) {
		in.lru.Remove(el)
		delete(in.items, key)
		return nil, false, false
	}
	in.lru.MoveToFront(el)
	return e.p, true, e.p != nil
}

func (in *Introspection) store(key [32]byte, p *Principal) {
	now := in.cfg.Now()
	ttl := in.cfg.NegativeTTL
	if p != nil {
		ttl = in.cfg.TTL
		if !p.ExpiresAt.IsZero() {
			if left := p.ExpiresAt.Sub(now); left < ttl {
				ttl = left
			}
		}
		if ttl <= 0 {
			return // already expired: do not cache
		}
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if el, ok := in.items[key]; ok {
		in.lru.Remove(el)
		delete(in.items, key)
	}
	for len(in.items) >= in.cfg.MaxEntries {
		oldest := in.lru.Back()
		in.lru.Remove(oldest)
		delete(in.items, oldest.Value.(*introEntry).key)
	}
	in.items[key] = in.lru.PushFront(&introEntry{key: key, p: p, expires: now.Add(ttl)})
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
