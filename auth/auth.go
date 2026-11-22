// Package auth authenticates gRPC and HTTP requests with bearer tokens and
// fails closed: every token is verified (signature, exp, nbf, iss, aud and an
// algorithm allow-list) by the Verifier registered for its issuer, and
// anything that cannot be verified is rejected with codes.Unauthenticated.
// There is no "trust upstream" mode and no pass-through for unconfigured
// issuers.
//
// Built-in verifiers:
//
//   - NewStatic: fixed HMAC secrets or public keys, several key IDs for rotation.
//   - NewJWKS: public keys from a JWKS URL, refreshed automatically.
//   - NewIntrospection: opaque tokens or revocation checks through an
//     introspection call (RFC7662 provides an RFC 7662 client), cached.
//
// Methods that need no authentication must be listed in Config.Public;
// everything else requires a valid token (deny by default).
package auth

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"google.golang.org/grpc/codes"
)

// Client-visible errors: the errs catalog sentinels (localized by package
// i18n, domain filled by the errs.Handler) plus AUTH_UNAVAILABLE. Why a
// token was rejected is logged at Debug level, never returned.
var (
	ErrTokenMissing      = errs.ErrTokenMissing      // 401, no credentials
	ErrTokenInvalid      = errs.ErrTokenInvalid      // 401
	ErrTokenExpired      = errs.ErrTokenExpired      // 401
	ErrForbidden         = errs.ErrForbidden         // 403, denied by an Authorizer
	ErrInsufficientScope = errs.ErrInsufficientScope // 403, metadata missing_scope
	// ErrAuthUnavailable is returned when introspection or the JWKS endpoint fails (503).
	ErrAuthUnavailable = errs.New(codes.Unavailable, "AUTH_UNAVAILABLE", errs.Message("authentication temporarily unavailable"))
)

// Principal is an authenticated caller.
type Principal struct {
	Subject   string
	Issuer    string
	Audience  []string
	Scopes    []string
	TokenID   string
	ExpiresAt time.Time
	IssuedAt  time.Time
	// Claims holds every claim of a JWT, or the introspection response
	// fields. Treat it as read-only.
	Claims map[string]any
}

// clone copies p so a handler that changes its principal does not change
// the cached one shared by other requests.
func (p *Principal) clone() *Principal {
	c := *p
	c.Audience = slices.Clone(p.Audience)
	c.Scopes = slices.Clone(p.Scopes)
	c.Claims = maps.Clone(p.Claims)
	return &c
}

// HasScope reports whether p was granted scope s.
func (p *Principal) HasScope(s string) bool { return slices.Contains(p.Scopes, s) }

// Verifier verifies one kind of token.
type Verifier interface {
	// Issuer is the `iss` value this verifier handles. Authenticator routes
	// JWTs by it. Verifiers for opaque tokens may return "".
	Issuer() string
	// Verify returns the principal for a valid token. Errors should be (or
	// wrap) ErrTokenInvalid, ErrTokenExpired or ErrAuthUnavailable.
	Verify(ctx context.Context, token string) (*Principal, error)
}

type principalKey struct{}

// NewContext returns ctx carrying p.
func NewContext(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext returns the principal stored by the interceptors or
// middleware.
func FromContext(ctx context.Context) (*Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(*Principal)
	return p, ok && p != nil
}

// SubjectFromContext returns the authenticated subject. Its signature
// matches ratelimit.Config.Subject.
func SubjectFromContext(ctx context.Context) (string, bool) {
	p, ok := FromContext(ctx)
	if !ok || p.Subject == "" {
		return "", false
	}
	return p.Subject, true
}

// Authorizer decides whether an authenticated principal may call method
// (a gRPC full method or an HTTP route pattern). Return ErrForbidden or
// ErrInsufficientScope (or a copy made with With) to deny; any other error
// is also a denial, reported as FORBIDDEN.
type Authorizer interface {
	Authorize(ctx context.Context, method string, p *Principal) error
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(ctx context.Context, method string, p *Principal) error

// Authorize implements Authorizer.
func (f AuthorizerFunc) Authorize(ctx context.Context, method string, p *Principal) error {
	return f(ctx, method, p)
}

// RequireScopes returns an Authorizer that requires every listed scope for
// the given methods. Methods that are not listed only need authentication. A
// missing scope is ErrInsufficientScope with metadata missing_scope (HTTP 403
// with WWW-Authenticate: Bearer error="insufficient_scope", RFC 6750).
func RequireScopes(m map[string][]string) Authorizer {
	required := make(map[string][]string, len(m))
	for k, v := range m {
		required[k] = slices.Clone(v)
	}
	return AuthorizerFunc(func(_ context.Context, method string, p *Principal) error {
		for _, s := range required[method] {
			if !p.HasScope(s) {
				return ErrInsufficientScope.With(errs.Meta("missing_scope", s))
			}
		}
		return nil
	})
}
