package ratelimit

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Key returns the bucket key for a request: "sub:<subject>" when
// Config.Subject reports one, else "ip:<client IP>" (IPv6 collapsed to its
// /64). remote is the transport peer address; xff are X-Forwarded-For values,
// believed only when the peer is a trusted proxy. ok is false when neither is
// known.
func (l *Limiter) Key(ctx context.Context, remote string, xff []string) (string, bool) {
	if l.subject != nil {
		if s, ok := l.subject(ctx); ok && s != "" {
			return "sub:" + s, true
		}
	}
	ip, ok := clientIP(remote, xff, l.trusted)
	if !ok {
		return "", false
	}
	return "ip:" + bucketIP(ip), true
}

func parseAddr(s string) (netip.Addr, bool) {
	s = strings.TrimSpace(s)
	if host, _, err := net.SplitHostPort(s); err == nil {
		s = host
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Addr{}, false
	}
	return a.Unmap().WithZone(""), true
}

func trusted(a netip.Addr, ps []netip.Prefix) bool {
	for _, p := range ps {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// clientIP walks X-Forwarded-For from the right, skipping trusted proxies;
// the first untrusted hop is the client. Without a trusted peer, the peer
// itself is the client.
func clientIP(remote string, xff []string, ps []netip.Prefix) (netip.Addr, bool) {
	peerIP, ok := parseAddr(remote)
	if !ok {
		return netip.Addr{}, false
	}
	if !trusted(peerIP, ps) {
		return peerIP, true
	}
	var hops []string
	for _, v := range xff {
		hops = append(hops, strings.Split(v, ",")...)
	}
	client := peerIP
	for i := len(hops) - 1; i >= 0; i-- {
		a, ok := parseAddr(hops[i])
		if !ok {
			break // garbage: stop at the last good hop
		}
		client = a
		if !trusted(a, ps) {
			break
		}
	}
	return client, true
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

func (l *Limiter) grpcCheck(ctx context.Context, method string) error {
	if !l.Limited(method) {
		return nil
	}
	var remote string
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		remote = p.Addr.String()
	}
	md, _ := metadata.FromIncomingContext(ctx)
	key, ok := l.Key(ctx, remote, md.Get("x-forwarded-for"))
	if !ok {
		return nil // nothing to key on (e.g. in-process transport)
	}
	d, err := l.Allow(ctx, method, key)
	if err != nil {
		l.log.WarnContext(ctx, "rate-limit store failed", "rpc.method", method, "error", err)
	}
	if !d.Allowed {
		return deny(method, d)
	}
	return nil
}

// UnaryServerInterceptor limits unary calls by full method. Install it after
// authentication so Config.Subject can see the principal.
func (l *Limiter) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := l.grpcCheck(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor limits stream creation (not messages) by full
// method.
func (l *Limiter) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := l.grpcCheck(ss.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// Middleware limits HTTP requests. The policy name is r.Pattern, which
// http.ServeMux sets on handlers it routes to, so wrap handlers registered on
// a mux (mux.Handle("POST /v1/chat", l.Middleware(h))). Without a pattern
// only the Default policy applies. It sets X-RateLimit-Limit,
// X-RateLimit-Remaining, X-RateLimit-Reset and, on 429, Retry-After.
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.Pattern
		if !l.Limited(name) {
			next.ServeHTTP(w, r)
			return
		}
		key, ok := l.Key(r.Context(), r.RemoteAddr, r.Header.Values("X-Forwarded-For"))
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		d, err := l.Allow(r.Context(), name, key)
		if err != nil {
			l.log.WarnContext(r.Context(), "rate-limit store failed", "http.route", name, "error", err)
		}
		if d.Limit > 0 {
			h := w.Header()
			h.Set("X-RateLimit-Limit", strconv.Itoa(d.Limit))
			h.Set("X-RateLimit-Remaining", strconv.Itoa(d.Remaining))
			h.Set("X-RateLimit-Reset", strconv.Itoa(ceilSeconds(d.ResetAfter)))
		}
		if !d.Allowed {
			errs.WriteHTTP(w, deny(name, d)) // sets Retry-After from RetryInfo
			return
		}
		next.ServeHTTP(w, r)
	})
}

func ceilSeconds(d time.Duration) int {
	return int((d + time.Second - 1) / time.Second)
}
