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
	"slices"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"google.golang.org/grpc/codes"
)

// Domain is the ErrorInfo domain of this package's errors.
const Domain = "auth.kitchen"

// Client-visible errors. Why a token was rejected is logged at Debug level,
// never returned.
var (
	ErrTokenMissing = errs.New(codes.Unauthenticated, "TOKEN_MISSING",
		errs.Domain(Domain), errs.Message("missing bearer token"))
	ErrTokenInvalid = errs.New(codes.Unauthenticated, "TOKEN_INVALID",
		errs.Domain(Domain), errs.Message("invalid token"))
	ErrTokenExpired = errs.New(codes.Unauthenticated, "TOKEN_EXPIRED",
		errs.Domain(Domain), errs.Message("token expired"))
	ErrForbidden = errs.New(codes.PermissionDenied, "FORBIDDEN",
		errs.Domain(Domain), errs.Message("permission denied"))
	ErrAuthUnavailable = errs.New(codes.Unavailable, "AUTH_UNAVAILABLE",
		errs.Domain(Domain), errs.Message("authentication temporarily unavailable"))
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
// (a gRPC full method or an HTTP route pattern). Return ErrForbidden (or an
// error wrapping it) to deny.
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
// the given methods. Methods that are not listed only need authentication.
func RequireScopes(m map[string][]string) Authorizer {
	required := make(map[string][]string, len(m))
	for k, v := range m {
		required[k] = slices.Clone(v)
	}
	return AuthorizerFunc(func(_ context.Context, method string, p *Principal) error {
		for _, s := range required[method] {
			if !p.HasScope(s) {
				return ErrForbidden.With(errs.Meta("missing_scope", s))
			}
		}
		return nil
	})
}
