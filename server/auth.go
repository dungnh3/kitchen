package server

import (
	"context"
	"net/http"

	"github.com/dungnh3/kitchen/ratelimit"
	"google.golang.org/grpc"
)

// Authenticator authenticates calls on both ports.
// *auth.Authenticator (module github.com/dungnh3/kitchen/auth) implements
// it; the interface keeps the JWT dependencies out of this module.
type Authenticator interface {
	UnaryServerInterceptor() grpc.UnaryServerInterceptor
	StreamServerInterceptor() grpc.StreamServerInterceptor
	// AuthenticateHTTP authenticates r as the route pattern and returns its
	// context with the principal, or a client-safe error.
	AuthenticateHTTP(r *http.Request, pattern string) (context.Context, error)
}

// WithAuth authenticates every call, fail-closed:
//
//   - gRPC and the REST gateway: a's interceptors run after the errs
//     interceptor (so failures are localized AIP-193 errors) and before the
//     user interceptors. The gRPC health service is always public.
//   - Plain HTTP routes (HTTPRegistrar): authenticated by their matched
//     route pattern ("GET /files/{id}"), which is what a's public list and
//     authorizer see. /livez and /readyz are not affected.
//
// The authenticated subject is stored with xcontext.WithUserID by the
// authenticator, so WithUserIDFrom is not needed.
func WithAuth(a Authenticator) Option { return func(c *config) { c.auth = a } }

// WithRateLimit limits every call with l, after authentication (so a
// signed-in caller is limited per user, others per client IP resolved with
// WithTrustedProxies):
//
//   - gRPC and the REST gateway: l's interceptors, by full method. The gRPC
//     health service is never limited. The RateLimit / X-RateLimit-* headers
//     reach REST clients unprefixed.
//   - Plain HTTP routes (HTTPRegistrar): by matched route pattern.
//
// Denials are RATE_LIMITED (429, Retry-After) rendered by the server's
// errs.Handler.
func WithRateLimit(l *ratelimit.Limiter) Option { return func(c *config) { c.rateLimit = l } }

func skipHealthUnary(next grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if isHealthMethod(info.FullMethod) {
			return handler(ctx, req)
		}
		return next(ctx, req, info, handler)
	}
}

func skipHealthStream(next grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if isHealthMethod(info.FullMethod) {
			return handler(srv, ss)
		}
		return next(srv, ss, info, handler)
	}
}

// guardHTTP authenticates and rate-limits the plain HTTP routes of mux by
// their matched pattern. Requests routed to the REST gateway ("/") are
// handled by the gRPC interceptors instead, so they are passed through, as
// are requests that match no route.
func (s *Server) guardHTTP(mux *http.ServeMux) http.Handler {
	if s.cfg.auth == nil && s.cfg.rateLimit == nil {
		return mux
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		if pattern == "" || pattern == "/" {
			mux.ServeHTTP(w, r)
			return
		}
		if s.cfg.auth != nil {
			ctx, err := s.cfg.auth.AuthenticateHTTP(r, pattern)
			if err != nil {
				s.errs.WriteHTTP(w, r, err)
				return
			}
			r = r.WithContext(ctx) //nolint:contextcheck // ctx derives from r.Context() in AuthenticateHTTP
		}
		if s.cfg.rateLimit != nil {
			if err := s.cfg.rateLimit.LimitHTTP(w, r, pattern); err != nil {
				s.errs.WriteHTTP(w, r, err)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}
