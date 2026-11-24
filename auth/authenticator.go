package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/dungnh3/kitchen/log"

	xcontext "github.com/dungnh3/kitchen/context"
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
	// Audience must be in every principal's Audience, whatever verifier
	// produced it (RFC 8725 section 3.9): a token minted for another API of
	// the same issuer is rejected. Required unless AnyAudience is set.
	Audience string
	// AnyAudience accepts principals whatever their audience. Set it only
	// when every verifier checks its own audience (StaticConfig.Audience,
	// JWKSConfig.Audience, RFC7662Config.Audience) or the issuer mints
	// tokens for this service alone. It may not be combined with Audience.
	AnyAudience bool
	// Public lists methods that skip authentication: an exact gRPC full
	// method ("/pkg.Svc/Method") or HTTP route pattern, or a service prefix
	// ending in "/" ("/grpc.health.v1.Health/"). Everything else requires a
	// valid token.
	Public []string
	// Optional lists methods (same syntax as Public) where a token is
	// optional: without one the request runs anonymously (no principal, the
	// Authorizer is not called); with one, the token must be valid, else the
	// request is rejected as for any other method (fail closed: a bad token
	// never degrades to anonymous). A method may not be both Public and
	// Optional.
	Optional []string
	// MaxTokenBytes rejects longer tokens before any decoding. Default 8192.
	MaxTokenBytes int
	// StreamOutlivesToken keeps a stream running after its token expires.
	// By default the stream's context is cancelled at the principal's
	// ExpiresAt and the stream ends with TOKEN_EXPIRED, so a client must
	// reconnect with a fresh token.
	StreamOutlivesToken bool
	// Authorizer runs after authentication. Optional.
	Authorizer Authorizer
	// WriteError writes HTTP failures in Middleware. Default: errs.WriteHTTP
	// (AIP-193 envelope, RFC 6750 WWW-Authenticate); pass the WriteHTTP
	// method of the service's errs.Handler to localize and set the domain.
	WriteError func(http.ResponseWriter, *http.Request, error)

	// Logger receives the authenticator logs. nil means log.Default().
	Logger        *log.Logger
	MeterProvider metric.MeterProvider
}

// Authenticator authenticates requests. It is safe for concurrent use.
type Authenticator struct {
	byIssuer map[string]Verifier
	opaque   Verifier
	audience string
	public   methodSet
	optional methodSet
	maxToken int
	streamOK bool
	authz    Authorizer
	writeErr func(http.ResponseWriter, *http.Request, error)
	log      *log.Logger
	results  metric.Int64Counter
}

// New validates cfg and returns an Authenticator.
func New(cfg Config) (*Authenticator, error) {
	if len(cfg.Verifiers) == 0 && cfg.Opaque == nil {
		return nil, errors.New("auth: at least one verifier is required")
	}
	switch {
	case cfg.Audience == "" && !cfg.AnyAudience:
		return nil, errors.New("auth: Audience is required (set AnyAudience to accept every audience)")
	case cfg.Audience != "" && cfg.AnyAudience:
		return nil, errors.New("auth: Audience and AnyAudience are mutually exclusive")
	}
	a := &Authenticator{
		byIssuer: make(map[string]Verifier, len(cfg.Verifiers)),
		opaque:   cfg.Opaque,
		audience: cfg.Audience,
		maxToken: cfg.MaxTokenBytes,
		streamOK: cfg.StreamOutlivesToken,
		authz:    cfg.Authorizer,
		writeErr: cfg.WriteError,
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
	var err error
	if a.public, err = newMethodSet("public", cfg.Public); err != nil {
		return nil, err
	}
	if a.optional, err = newMethodSet("optional", cfg.Optional); err != nil {
		return nil, err
	}
	for _, m := range cfg.Optional {
		if a.public.has(m) {
			return nil, fmt.Errorf("auth: %q is both public and optional", m)
		}
	}
	if a.maxToken <= 0 {
		a.maxToken = 8192
	}
	if a.log == nil {
		a.log = log.Default()
	}
	if a.writeErr == nil {
		a.writeErr = errs.WriteHTTP
	}
	mp := cfg.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	a.results, err = mp.Meter("github.com/dungnh3/kitchen/auth").Int64Counter("kitchen.auth.results",
		metric.WithDescription("Authentication outcomes."), metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("auth: metric: %w", err)
	}
	return a, nil
}

// methodSet matches exact methods and "/"-terminated prefixes.
type methodSet struct {
	exact    map[string]struct{}
	prefixes []string
}

func newMethodSet(kind string, ms []string) (methodSet, error) {
	s := methodSet{exact: make(map[string]struct{})}
	for _, m := range ms {
		switch {
		case m == "" || m == "/":
			return s, fmt.Errorf("auth: %s entry %q would cover every method", kind, m)
		case strings.HasSuffix(m, "/"):
			s.prefixes = append(s.prefixes, m)
		default:
			s.exact[m] = struct{}{}
		}
	}
	return s, nil
}

func (s methodSet) has(method string) bool {
	if _, ok := s.exact[method]; ok {
		return true
	}
	for _, p := range s.prefixes {
		if strings.HasPrefix(method, p) {
			return true
		}
	}
	return false
}

// IsPublic reports whether method skips authentication.
func (a *Authenticator) IsPublic(method string) bool { return a.public.has(method) }

// IsOptional reports whether method accepts requests without a token.
func (a *Authenticator) IsOptional(method string) bool { return a.optional.has(method) }

// Authenticate verifies a raw token (without the "Bearer " prefix).
func (a *Authenticator) Authenticate(ctx context.Context, token string) (*Principal, error) {
	if token == "" {
		return nil, ErrTokenMissing
	}
	if len(token) > a.maxToken {
		return nil, errs.Wrap(fmt.Errorf("token of %d bytes exceeds %d", len(token), a.maxToken), ErrTokenInvalid)
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
	if errors.Is(err, ErrTokenMissing) && a.IsOptional(method) {
		a.record(ctx, "anonymous")
		return ctx, nil
	}
	if err == nil {
		var p *Principal
		if p, err = a.Authenticate(ctx, token); err == nil {
			if a.authz != nil {
				err = authzError(a.authz.Authorize(ctx, method, p))
			}
			if err == nil {
				a.record(ctx, "ok")
				ctx = NewContext(ctx, p)
				if p.Subject != "" {
					ctx = xcontext.WithUserID(ctx, p.Subject)
				}
				return ctx, nil
			}
		}
	}
	return ctx, a.reject(ctx, method, err)
}

// authzError maps an Authorizer error to a denial: errors other than the
// 403 sentinels and ErrAuthUnavailable become ErrForbidden (never a 401).
func authzError(err error) error {
	if err == nil || errors.Is(err, ErrForbidden) || errors.Is(err, ErrInsufficientScope) || errors.Is(err, ErrAuthUnavailable) {
		return err
	}
	return errs.Wrap(err, ErrForbidden)
}

func (a *Authenticator) reject(ctx context.Context, method string, err error) error {
	var out *errs.Error
	switch {
	case errors.Is(err, ErrAuthUnavailable):
		// First: an authority failure stays a 503 whatever reason its
		// cause carries.
		out = ErrAuthUnavailable
	case errors.Is(err, ErrTokenMissing):
		out = ErrTokenMissing
	case errors.Is(err, ErrTokenExpired):
		out = ErrTokenExpired
	case errors.Is(err, ErrInsufficientScope), errors.Is(err, ErrForbidden):
		// Keep the client-safe metadata (missing_scope), drop the cause.
		out = errs.FromStatus(errs.FromError(err).GRPCStatus())
	default:
		out = ErrTokenInvalid
	}
	a.record(ctx, strings.ToLower(out.Reason()))
	if out.Reason() == ErrAuthUnavailable.Reason() {
		a.log.WarnContext(ctx, "authentication unavailable", log.String("rpc.method", method), log.Err(err))
	} else {
		a.log.DebugContext(ctx, "request rejected", log.String("rpc.method", method),
			log.String("reason", out.Reason()), log.Err(err))
	}
	// The cause stays in the log, not on the wire.
	return out
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
		if p, ok := FromContext(ctx); ok && !a.streamOK && !p.ExpiresAt.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadlineCause(ctx, p.ExpiresAt, ErrTokenExpired)
			defer cancel()
		}
		ws := &wrappedStream{ServerStream: ss, ctx: ctx}
		err = handler(srv, ws)
		if expired := ws.expired(); expired != nil {
			// The stream outlived its token: tell the client to re-authenticate.
			return expired
		}
		return err
	}
}

// wrappedStream carries the authenticated context. Once the token expired
// (the context's cause is ErrTokenExpired) it refuses to receive or send,
// so a handler that never looks at its context still stops serving the
// stream.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

func (w *wrappedStream) expired() error {
	if w.ctx.Err() != nil && errors.Is(context.Cause(w.ctx), ErrTokenExpired) {
		return ErrTokenExpired
	}
	return nil
}

func (w *wrappedStream) RecvMsg(m any) error {
	if err := w.expired(); err != nil {
		return err
	}
	if err := w.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	// The token may have expired while waiting for the message.
	return w.expired()
}

func (w *wrappedStream) SendMsg(m any) error {
	if err := w.expired(); err != nil {
		return err
	}
	return w.ServerStream.SendMsg(m)
}

func (w *wrappedStream) SendHeader(md metadata.MD) error {
	if err := w.expired(); err != nil {
		return err
	}
	return w.ServerStream.SendHeader(md)
}

// AuthenticateHTTP authenticates and authorizes r as the route pattern
// (the method passed to Public and the Authorizer). It returns r's context
// with the principal and the user id, or a client-safe error for the caller
// to write. package server uses it to render failures with its own
// errs.Handler.
func (a *Authenticator) AuthenticateHTTP(r *http.Request, pattern string) (context.Context, error) {
	vs := r.Header.Values("Authorization")
	var h string
	if len(vs) > 0 {
		h = vs[0]
	}
	return a.check(r.Context(), pattern, h, len(vs))
}

// Middleware authenticates HTTP requests. The method passed to Public and
// the Authorizer is r.Pattern, so wrap the handler registered on the
// http.ServeMux (not the mux itself, where r.Pattern is still empty).
// Failures are written with Config.WriteError. With package server, use
// server.WithAuth instead.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := a.AuthenticateHTTP(r, r.Pattern)
		if err != nil {
			a.writeErr(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx)) //nolint:contextcheck // ctx derives from r.Context() in AuthenticateHTTP
	})
}
