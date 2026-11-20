package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/dungnh3/kitchen/errs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Config configures an Authenticator.
type Config struct {
	// Verifiers handle JWTs, routed by the token's iss. Each issuer may
	// appear once. A JWT whose iss has no verifier is rejected.
	Verifiers []Verifier
	// Opaque handles tokens that are not JWTs. nil: such tokens are rejected.
	Opaque Verifier
	// Audience, when set, must be in every principal's Audience, whatever
	// verifier produced it. Leave it empty only if each verifier checks
	// its own audience.
	Audience string
	// Public lists methods that skip authentication: an exact gRPC full
	// method ("/pkg.Svc/Method") or HTTP route pattern, or a service prefix
	// ending in "/" ("/grpc.health.v1.Health/"). Everything else requires a
	// valid token.
	Public []string
	// Authorizer runs after authentication. Optional.
	Authorizer Authorizer

	Logger        *slog.Logger
	MeterProvider metric.MeterProvider
}

// Authenticator authenticates requests. It is safe for concurrent use.
type Authenticator struct {
	byIssuer map[string]Verifier
	opaque   Verifier
	audience string
	exact    map[string]struct{}
	prefixes []string
	authz    Authorizer
	log      *slog.Logger
	results  metric.Int64Counter
}

// New validates cfg and returns an Authenticator.
func New(cfg Config) (*Authenticator, error) {
	if len(cfg.Verifiers) == 0 && cfg.Opaque == nil {
		return nil, errors.New("auth: at least one verifier is required")
	}
	a := &Authenticator{
		byIssuer: make(map[string]Verifier, len(cfg.Verifiers)),
		opaque:   cfg.Opaque,
		audience: cfg.Audience,
		exact:    make(map[string]struct{}),
		authz:    cfg.Authorizer,
		log:      cfg.Logger,
	}
	for _, v := range cfg.Verifiers {
		if v == nil {
			return nil, errors.New("auth: nil verifier")
		}
		iss := v.Issuer()
		if iss == "" {
			return nil, errors.New("auth: a JWT verifier must have an issuer (use Config.Opaque for opaque tokens)")
		}
		if _, dup := a.byIssuer[iss]; dup {
			return nil, fmt.Errorf("auth: duplicate verifier for issuer %q", iss)
		}
		a.byIssuer[iss] = v
	}
	for _, m := range cfg.Public {
		switch {
		case m == "" || m == "/":
			return nil, fmt.Errorf("auth: public entry %q would disable authentication", m)
		case strings.HasSuffix(m, "/"):
			a.prefixes = append(a.prefixes, m)
		default:
			a.exact[m] = struct{}{}
		}
	}
	if a.log == nil {
		a.log = slog.Default()
	}
	mp := cfg.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	var err error
	a.results, err = mp.Meter("github.com/dungnh3/kitchen/auth").Int64Counter("kitchen.auth.results",
		metric.WithDescription("Authentication outcomes."), metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("auth: metric: %w", err)
	}
	return a, nil
}

// IsPublic reports whether method skips authentication.
func (a *Authenticator) IsPublic(method string) bool {
	if _, ok := a.exact[method]; ok {
		return true
	}
	for _, p := range a.prefixes {
		if strings.HasPrefix(method, p) {
			return true
		}
	}
	return false
}

// Authenticate verifies a raw token (without the "Bearer " prefix).
func (a *Authenticator) Authenticate(ctx context.Context, token string) (*Principal, error) {
	if token == "" {
		return nil, ErrTokenMissing
	}
	v, err := a.route(token)
	if err != nil {
		return nil, err
	}
	p, err := v.Verify(ctx, token)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, ErrTokenInvalid
	}
	if a.audience != "" && !slices.Contains(p.Audience, a.audience) {
		return nil, errs.Wrap(fmt.Errorf("audience %v lacks %q", p.Audience, a.audience), ErrTokenInvalid)
	}
	return p, nil
}

// route picks the verifier. The unverified iss is only a routing key: the
// chosen verifier checks the signature and the iss again.
func (a *Authenticator) route(token string) (Verifier, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		if a.opaque == nil {
			return nil, errs.Wrap(errors.New("not a JWT and no opaque verifier"), ErrTokenInvalid)
		}
		return a.opaque, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errs.Wrap(err, ErrTokenInvalid)
	}
	var claims struct {
		Iss string `json:"iss"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, errs.Wrap(err, ErrTokenInvalid)
	}
	v, ok := a.byIssuer[claims.Iss]
	if !ok {
		return nil, errs.Wrap(fmt.Errorf("no verifier for issuer %q", claims.Iss), ErrTokenInvalid)
	}
	return v, nil
}

// check authenticates and authorizes one request for method.
func (a *Authenticator) check(ctx context.Context, method, header string, headers int) (context.Context, error) {
	if a.IsPublic(method) {
		return ctx, nil
	}
	token, err := bearer(header, headers)
	if err == nil {
		var p *Principal
		if p, err = a.Authenticate(ctx, token); err == nil {
			if a.authz != nil {
				err = a.authz.Authorize(ctx, method, p)
			}
			if err == nil {
				a.record(ctx, "ok")
				return NewContext(ctx, p), nil
			}
		}
	}
	return ctx, a.reject(ctx, method, err)
}

func (a *Authenticator) reject(ctx context.Context, method string, err error) error {
	var out *errs.Error
	switch {
	case errors.Is(err, ErrTokenMissing):
		out = ErrTokenMissing
	case errors.Is(err, ErrTokenExpired):
		out = ErrTokenExpired
	case errors.Is(err, ErrForbidden):
		var e *errs.Error
		if errors.As(err, &e) && errors.Is(e, ErrForbidden) {
			out = e.With() // keep client-safe metadata such as missing_scope
		} else {
			out = ErrForbidden
		}
	case errors.Is(err, ErrAuthUnavailable):
		out = ErrAuthUnavailable
	default:
		out = ErrTokenInvalid
	}
	a.record(ctx, strings.ToLower(out.Reason()))
	if out == ErrAuthUnavailable {
		a.log.WarnContext(ctx, "authentication unavailable", "rpc.method", method, "error", err)
	} else {
		a.log.DebugContext(ctx, "request rejected", "rpc.method", method, "reason", out.Reason(), "error", err)
	}
	// Return a fresh copy: the cause stays in the log, not on the wire.
	return out.With()
}

func (a *Authenticator) record(ctx context.Context, result string) {
	a.results.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

// bearer extracts the token from exactly one Authorization value of the
// form "Bearer <token>" (scheme is case-insensitive).
func bearer(header string, count int) (string, error) {
	switch {
	case count == 0 || header == "":
		return "", ErrTokenMissing
	case count > 1:
		return "", errs.Wrap(errors.New("multiple authorization values"), ErrTokenInvalid)
	}
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "bearer") {
		return "", errs.Wrap(errors.New("authorization scheme is not bearer"), ErrTokenInvalid)
	}
	token = strings.TrimSpace(token)
	if token == "" || strings.ContainsAny(token, " \t") {
		return "", errs.Wrap(errors.New("malformed bearer token"), ErrTokenInvalid)
	}
	return token, nil
}

func grpcAuthorization(ctx context.Context) (string, int) {
	md, _ := metadata.FromIncomingContext(ctx)
	vs := md.Get("authorization")
	if len(vs) == 0 {
		return "", 0
	}
	return vs[0], len(vs)
}

// UnaryServerInterceptor authenticates unary calls and stores the
// principal in the handler's context (see FromContext).
func (a *Authenticator) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		h, n := grpcAuthorization(ctx)
		ctx, err := a.check(ctx, info.FullMethod, h, n)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor authenticates streams once, at creation.
func (a *Authenticator) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		h, n := grpcAuthorization(ss.Context())
		ctx, err := a.check(ss.Context(), info.FullMethod, h, n)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// Middleware authenticates HTTP requests. The method passed to Public and
// the Authorizer is r.Pattern (set when the handler is registered on an
// http.ServeMux). Failures are written with errs.WriteHTTP.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vs := r.Header.Values("Authorization")
		var h string
		if len(vs) > 0 {
			h = vs[0]
		}
		ctx, err := a.check(r.Context(), r.Pattern, h, len(vs))
		if err != nil {
			errs.WriteHTTP(w, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
