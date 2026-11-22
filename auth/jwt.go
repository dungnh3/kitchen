package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/dungnh3/kitchen/errs"
)

const defaultLeeway = 30 * time.Second

// errKeySourceUnavailable marks key lookups that failed for infrastructure
// reasons (JWKS not reachable), as opposed to an unknown key.
var errKeySourceUnavailable = errors.New("auth: key source unavailable")

// keySource finds the verification key for a token header.
type keySource interface {
	// key returns the key for kid and alg. alg is already in the allow-list.
	key(ctx context.Context, kid string, alg jwa.SignatureAlgorithm) (any, error)
}

// jwtCheck holds the claim checks shared by JWT verifiers.
type jwtCheck struct {
	issuer   string
	audience string
	leeway   time.Duration
	algs     []jwa.SignatureAlgorithm
	types    []string // normalized; empty: not checked
	now      func() time.Time
}

// normType lower-cases a media type and drops the optional "application/"
// prefix (RFC 7515 section 4.1.9).
func normType(t string) string {
	return strings.TrimPrefix(strings.ToLower(strings.TrimSpace(t)), "application/")
}

func normTypes(ts []string) []string {
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		if t = normType(t); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func (c jwtCheck) allowed(alg jwa.SignatureAlgorithm) bool {
	return slices.ContainsFunc(c.algs, func(a jwa.SignatureAlgorithm) bool { return a.String() == alg.String() })
}

func (c jwtCheck) verify(ctx context.Context, token string, src keySource) (*Principal, error) {
	kp := jws.KeyProviderFunc(func(_ context.Context, sink jws.KeySink, sig *jws.Signature, _ *jws.Message) error {
		h := sig.ProtectedHeaders()
		alg, ok := h.Algorithm()
		if !ok || !c.allowed(alg) {
			return fmt.Errorf("auth: algorithm %q not allowed", alg)
		}
		if len(c.types) > 0 {
			typ, _ := h.Type()
			if !slices.Contains(c.types, normType(typ)) {
				return fmt.Errorf("auth: token type %q not accepted", typ)
			}
		}
		kid, _ := h.KeyID()
		key, err := src.key(ctx, kid, alg)
		if err != nil {
			return err
		}
		sink.Key(alg, key)
		return nil
	})
	opts := []jwt.ParseOption{
		jwt.WithKeyProvider(kp),
		jwt.WithValidate(true),
		jwt.WithIssuer(c.issuer),
		jwt.WithRequiredClaim(jwt.ExpirationKey),
		jwt.WithAcceptableSkew(c.leeway),
		jwt.WithClock(jwt.ClockFunc(c.now)),
	}
	if c.audience != "" {
		opts = append(opts, jwt.WithAudience(c.audience))
	}
	tok, err := jwt.ParseString(token, opts...)
	if err != nil {
		switch {
		case errors.Is(err, errKeySourceUnavailable):
			return nil, errs.Wrap(err, ErrAuthUnavailable)
		case errors.Is(err, jwt.TokenExpiredError()):
			return nil, errs.Wrap(err, ErrTokenExpired)
		default:
			return nil, errs.Wrap(err, ErrTokenInvalid)
		}
	}
	return principalFromToken(tok), nil
}

func principalFromToken(tok jwt.Token) *Principal {
	p := &Principal{Claims: make(map[string]any)}
	p.Subject, _ = tok.Subject()
	p.Issuer, _ = tok.Issuer()
	p.Audience, _ = tok.Audience()
	p.TokenID, _ = tok.JwtID()
	p.ExpiresAt, _ = tok.Expiration()
	p.IssuedAt, _ = tok.IssuedAt()
	for _, k := range tok.Keys() {
		var v any
		if tok.Get(k, &v) == nil {
			p.Claims[k] = v
		}
	}
	p.Scopes = scopesFrom(p.Claims)
	return p
}

// scopesFrom reads "scope" (space separated, RFC 8693) or "scp" (string or
// array).
func scopesFrom(claims map[string]any) []string {
	for _, name := range []string{"scope", "scp"} {
		switch v := claims[name].(type) {
		case string:
			return strings.Fields(v)
		case []any:
			out := make([]string, 0, len(v))
			for _, s := range v {
				if str, ok := s.(string); ok {
					out = append(out, str)
				}
			}
			return out
		case []string:
			return slices.Clone(v)
		}
	}
	return nil
}

// Key is one static verification key.
type Key struct {
	// ID is matched against the token's "kid" header. It may be empty when
	// there is exactly one key.
	ID string
	// Alg is the only algorithm accepted with this key, for example
	// jwa.HS256() or jwa.RS256().
	Alg jwa.SignatureAlgorithm
	// Material is []byte for HMAC, or a public key (*rsa.PublicKey,
	// *ecdsa.PublicKey, ed25519.PublicKey).
	Material any
}

// StaticConfig configures NewStatic.
type StaticConfig struct {
	Issuer   string // required; must match the token's iss
	Audience string // when set, must be in the token's aud
	Keys     []Key  // at least one
	Leeway   time.Duration
	// Types, when set, lists the accepted JWT "typ" header values (RFC 8725
	// explicit typing), for example []string{"at+jwt"} for RFC 9068 access
	// tokens, so an ID token of the same issuer cannot be used as an access
	// token. Matching ignores case and an "application/" prefix. Empty: typ is
	// not checked (many IdPs send "JWT").
	Types []string
	// Now overrides the clock (tests).
	Now func() time.Time
}

// Static verifies JWTs with fixed keys.
type Static struct {
	check jwtCheck
	keys  map[string]Key
}

// NewStatic returns a Static verifier.
func NewStatic(cfg StaticConfig) (*Static, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("auth: static: issuer is required")
	}
	if len(cfg.Keys) == 0 {
		return nil, errors.New("auth: static: at least one key is required")
	}
	s := &Static{keys: make(map[string]Key, len(cfg.Keys))}
	var algs []jwa.SignatureAlgorithm
	for _, k := range cfg.Keys {
		if _, dup := s.keys[k.ID]; dup {
			return nil, fmt.Errorf("auth: static: duplicate key id %q", k.ID)
		}
		if err := checkKeyMaterial(k); err != nil {
			return nil, fmt.Errorf("auth: static: key %q: %w", k.ID, err)
		}
		s.keys[k.ID] = k
		algs = append(algs, k.Alg)
	}
	s.check = jwtCheck{
		issuer: cfg.Issuer, audience: cfg.Audience, algs: algs, types: normTypes(cfg.Types),
		leeway: leewayOr(cfg.Leeway), now: nowOr(cfg.Now),
	}
	return s, nil
}

// checkKeyMaterial makes sure the algorithm family matches the key type, so
// an HMAC algorithm can never be paired with a public key (alg confusion).
func checkKeyMaterial(k Key) error {
	name := k.Alg.String()
	var ok bool
	switch {
	case strings.HasPrefix(name, "HS"):
		var b []byte
		if b, ok = k.Material.([]byte); ok && len(b) < 32 {
			return errors.New("HMAC secret must be at least 32 bytes")
		}
	case strings.HasPrefix(name, "RS"), strings.HasPrefix(name, "PS"):
		_, ok = k.Material.(*rsa.PublicKey)
	case strings.HasPrefix(name, "ES"):
		_, ok = k.Material.(*ecdsa.PublicKey)
	case name == "EdDSA" || name == "Ed25519":
		_, ok = k.Material.(ed25519.PublicKey)
	default:
		return fmt.Errorf("unsupported algorithm %q", name)
	}
	if !ok {
		return fmt.Errorf("material %T does not fit algorithm %s", k.Material, name)
	}
	return nil
}

// Issuer implements Verifier.
func (s *Static) Issuer() string { return s.check.issuer }

// Verify implements Verifier.
func (s *Static) Verify(ctx context.Context, token string) (*Principal, error) {
	return s.check.verify(ctx, token, s)
}

func (s *Static) key(_ context.Context, kid string, alg jwa.SignatureAlgorithm) (any, error) {
	k, ok := s.keys[kid]
	if !ok && kid == "" && len(s.keys) == 1 {
		for _, only := range s.keys {
			k, ok = only, true
		}
	}
	if !ok {
		return nil, fmt.Errorf("auth: unknown key id %q", kid)
	}
	if k.Alg.String() != alg.String() {
		return nil, fmt.Errorf("auth: key %q does not accept %s", kid, alg)
	}
	return k.Material, nil
}

func leewayOr(d time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return defaultLeeway
}

func nowOr(f func() time.Time) func() time.Time {
	if f != nil {
		return f
	}
	return time.Now
}
