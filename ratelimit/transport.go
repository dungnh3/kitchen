package ratelimit

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/dunglas/httpsfv"
	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Response header names. RateLimit-Policy and RateLimit follow the IETF
// httpapi draft "RateLimit header fields for HTTP"; the X-RateLimit-* names
// are the de facto form most clients read.
const (
	HeaderPolicy    = "RateLimit-Policy"
	HeaderRateLimit = "RateLimit"
	HeaderLimit     = "X-RateLimit-Limit"
	HeaderRemaining = "X-RateLimit-Remaining"
	HeaderReset     = "X-RateLimit-Reset"
)

// ResponseHeaders lists the headers the limiter sets, lower-cased (as gRPC
// metadata keys). package server passes them through the REST gateway.
func ResponseHeaders() []string {
	return []string{"ratelimit-policy", "ratelimit", "x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset", "retry-after"}
}

// Key returns the ByCaller bucket key of a request: "user:<id>",
// "anon:<id>", "device:<hash>" or "ip:<client IP>" (IPv6 collapsed to its
// /64). The client IP is the one package server resolved with its trusted
// proxies (xcontext.Info.ClientIP); without it, remote (the transport peer
// address) is the client. ok is false when nothing identifies the caller.
func (l *Limiter) Key(ctx context.Context, remote string) (string, bool) {
	return l.identify(ctx, remote).key(ByCaller)
}

func clientIP(ctx context.Context, remote string) (netip.Addr, bool) {
	if info, ok := xcontext.FromContext(ctx); ok && info.ClientIP.IsValid() {
		return info.ClientIP.Unmap(), true
	}
	s := strings.TrimSpace(remote)
	if s == "" {
		return netip.Addr{}, false
	}
	if host, _, err := net.SplitHostPort(s); err == nil {
		s = host
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Addr{}, false
	}
	return a.Unmap().WithZone(""), true
}

func bucketIP(a netip.Addr) string {
	if a.Is6() {
		p, _ := a.Prefix(64)
		return p.String()
	}
	return a.String()
}

// deny builds the client error for a denied decision.
func deny(name string, d Decision) error {
	if d.Unavailable {
		return ErrUnavailable.With(errs.Meta("policy", name))
	}
	return ErrRateLimited.With(errs.Meta("policy", name),
		errs.Details(&errdetails.RetryInfo{RetryDelay: durationpb.New(d.RetryAfter)}))
}

// headers returns the response headers for a decision, keyed by canonical
// HTTP name, or nil when no limit applied. RateLimit-Policy and RateLimit
// (RFC 9651 structured-field lists, encoded with httpsfv) list every limit
// that applied; X-RateLimit-* describe the deciding one.
func headers(p *policy, d Decision, parts []part) map[string]string {
	if len(parts) == 0 || d.Limit <= 0 {
		return nil
	}
	pol := make(httpsfv.List, 0, len(parts))
	rl := make(httpsfv.List, 0, len(parts))
	for _, pt := range parts {
		id := p.name
		if p.visible > 1 || pt.lim.By == byCallerDeviceIP {
			id += "#" + pt.lim.label
		}
		pi := httpsfv.NewItem(id)
		pi.Params.Add("q", int64(pt.rule.Rate))
		pi.Params.Add("w", int64(max(1, ceilSeconds(pt.rule.Per))))
		pol = append(pol, pi)
		ri := httpsfv.NewItem(id)
		ri.Params.Add("r", int64(pt.d.Remaining))
		ri.Params.Add("t", int64(ceilSeconds(pt.d.ResetAfter)))
		rl = append(rl, ri)
	}
	h := map[string]string{
		HeaderLimit:     strconv.Itoa(d.Limit),
		HeaderRemaining: strconv.Itoa(d.Remaining),
		HeaderReset:     strconv.Itoa(ceilSeconds(d.ResetAfter)),
	}
	// Marshal fails only for policy names outside printable ASCII; the
	// X-RateLimit-* headers are still sent then.
	if v, err := httpsfv.Marshal(pol); err == nil {
		h[HeaderPolicy] = v
	}
	if v, err := httpsfv.Marshal(rl); err == nil {
		h[HeaderRateLimit] = v
	}
	return h
}

func (l *Limiter) grpcCheck(ctx context.Context, method string, setHeader func(metadata.MD) error) error {
	p, ok := l.policyFor(method)
	if !ok {
		return nil
	}
	var remote string
	if pr, ok := peer.FromContext(ctx); ok && pr.Addr != nil {
		remote = pr.Addr.String()
	}
	d, parts, err := l.checkIdentity(ctx, method, remote)
	if err != nil {
		l.log.WarnContext(ctx, "rate-limit store failed", "rpc.method", method, "error", err)
	}
	md := metadata.MD{}
	for k, v := range headers(p, d, parts) {
		md.Set(k, v)
	}
	if !d.Allowed && d.RetryAfter > 0 {
		md.Set("retry-after", strconv.Itoa(ceilSeconds(d.RetryAfter)))
	}
	if md.Len() > 0 {
		_ = setHeader(md) // fails only after headers were sent
	}
	if !d.Allowed {
		return deny(p.name, d)
	}
	return nil
}

// UnaryServerInterceptor limits unary calls by full method and sends the
// rate-limit headers as response metadata. Install it after authentication
// so the subject is known (package server does).
func (l *Limiter) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		set := func(md metadata.MD) error { return grpc.SetHeader(ctx, md) }
		if err := l.grpcCheck(ctx, info.FullMethod, set); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor limits stream creation (not messages) by full
// method.
func (l *Limiter) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := l.grpcCheck(ss.Context(), info.FullMethod, ss.SetHeader); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// LimitHTTP checks r against the policy for the route pattern, sets the
// rate-limit headers on w and returns the client error to write when r is
// denied. package server uses it to render denials with its errs.Handler.
func (l *Limiter) LimitHTTP(w http.ResponseWriter, r *http.Request, pattern string) error {
	p, ok := l.policyFor(pattern)
	if !ok {
		return nil
	}
	d, parts, err := l.checkIdentity(r.Context(), pattern, r.RemoteAddr)
	if err != nil {
		l.log.WarnContext(r.Context(), "rate-limit store failed", "http.route", pattern, "error", err)
	}
	for k, v := range headers(p, d, parts) {
		w.Header().Set(k, v)
	}
	if !d.Allowed {
		return deny(p.name, d) // Retry-After comes from the RetryInfo detail
	}
	return nil
}

// Middleware limits HTTP requests. The policy name is r.Pattern, which
// http.ServeMux sets on handlers it routes to, so wrap handlers registered on
// a mux (mux.Handle("POST /v1/chat", l.Middleware(h))). Without a pattern
// only the Default policy applies. With package server, use
// server.WithRateLimit instead.
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := l.LimitHTTP(w, r, r.Pattern); err != nil {
			if l.errs != nil {
				l.errs.WriteHTTP(w, r, err)
			} else {
				errs.WriteHTTP(w, r, err)
			}
			return
		}
		next.ServeHTTP(w, r)
	})
}

func ceilSeconds(d time.Duration) int {
	return int((d + time.Second - 1) / time.Second)
}
