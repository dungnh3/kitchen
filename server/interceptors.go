package server

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"time"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/errs/validation"
	"github.com/dungnh3/kitchen/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const healthMethodPrefix = "/grpc.health.v1.Health/"

func isHealthMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, healthMethodPrefix)
}

// chain builds the interceptor chains, outermost first:
//
//	recovery → request info → drain → logging → errors → auth → rate limit
//	  → user interceptors → user id → validation
//
// Logging sits outside the error conversion so it records the final code;
// the errs.Handler logs the cause of server faults itself.
func (s *Server) chain() ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	unary := []grpc.UnaryServerInterceptor{
		s.recoverUnary, s.requestInfoUnary, drainUnary(s.drain), s.logUnary, s.errs.UnaryServerInterceptor(),
	}
	if s.cfg.auth != nil {
		unary = append(unary, skipHealthUnary(s.cfg.auth.UnaryServerInterceptor()))
	}
	if s.cfg.rateLimit != nil {
		unary = append(unary, skipHealthUnary(s.cfg.rateLimit.UnaryServerInterceptor()))
	}
	unary = append(unary, s.cfg.unary...)
	stream := []grpc.StreamServerInterceptor{
		s.recoverStream, s.requestInfoStream, drainStream(s.drain), s.logStream, s.errs.StreamServerInterceptor(),
	}
	if s.cfg.auth != nil {
		stream = append(stream, skipHealthStream(s.cfg.auth.StreamServerInterceptor()))
	}
	if s.cfg.rateLimit != nil {
		stream = append(stream, skipHealthStream(s.cfg.rateLimit.StreamServerInterceptor()))
	}
	stream = append(stream, s.cfg.stream...)
	if s.cfg.userID != nil {
		unary = append(unary, s.userIDUnary)
		stream = append(stream, s.userIDStream)
	}
	if s.validator != nil {
		unary = append(unary, s.validateUnary)
		stream = append(stream, s.validateStream)
	}
	return unary, stream
}

// --- recovery ---

// recovered logs a panic and returns the error for the client, not yet
// converted. The stack trace is left to the logger: by default kitchen/log
// adds one to ERROR records, at the panic site since the log call runs in
// the deferred recover.
func (s *Server) recovered(ctx context.Context, operation string, p any) error {
	s.log.LogAttrs(ctx, log.LevelError, "panic recovered",
		log.String("operation", operation),
		log.Any("panic", p))
	if s.cfg.recovery != nil {
		if err := s.cfg.recovery(ctx, p); err != nil {
			return err
		}
	}
	return errs.ErrInternal
}

func (s *Server) recoverUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if p := recover(); p != nil {
			// Recovery is outside the error interceptor: convert here.
			resp, err = nil, s.errs.Status(ctx, s.recovered(ctx, info.FullMethod, p)).Err()
		}
	}()
	return h(ctx, req)
}

func (s *Server) recoverStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = s.errs.Status(ss.Context(), s.recovered(ss.Context(), info.FullMethod, p)).Err()
		}
	}()
	return h(srv, ss)
}

// --- user id from authentication ---

func (s *Server) withUserID(ctx context.Context) context.Context {
	if id, ok := s.cfg.userID(ctx); ok && id != "" {
		return xcontext.WithUserID(ctx, id)
	}
	return ctx
}

func (s *Server) userIDUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	return h(s.withUserID(ctx), req)
}

func (s *Server) userIDStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	return h(srv, &ctxStream{ServerStream: ss, ctx: s.withUserID(ss.Context())})
}

// --- request info and identity sanitising ---

func peerAddr(ctx context.Context) (netip.Addr, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return netip.Addr{}, false
	}
	switch a := p.Addr.(type) {
	case inprocAddr:
		return netip.Addr{}, true
	case *net.TCPAddr: // no String/parse round trip on the hot path
		return a.AddrPort().Addr().Unmap(), false
	}
	if p.Addr.Network() == inprocNetwork {
		return netip.Addr{}, true
	}
	return xcontext.AddrFromHostPort(p.Addr.String()), false
}

// identityKeys are xcontext.IdentityHeaders with their gRPC metadata keys.
var identityKeys = func() []struct{ name, md string } {
	hs := xcontext.IdentityHeaders()
	ks := make([]struct{ name, md string }, len(hs))
	for i, h := range hs {
		ks[i].name, ks[i].md = h, strings.ToLower(h)
	}
	return ks
}()

var (
	mdClientIP = strings.ToLower(xcontext.HeaderClientIP)
	mdUserID   = strings.ToLower(xcontext.HeaderUserID)
)

// requestContext strips untrusted identity metadata and stores the request
// Info (and, from trusted peers, the propagated user id) in ctx.
func (s *Server) requestContext(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	addr, inproc := peerAddr(ctx)
	trusted := inproc || s.cfg.trustAllIdentity || s.cfg.trustedIdentityPeers.Contains(addr)
	if !trusted {
		var stripped metadata.MD
		for _, k := range identityKeys {
			if len(md[k.md]) > 0 {
				s.warnStrippedIdentity(ctx, "grpc", addr, k.name)
				if stripped == nil {
					stripped = md.Copy()
				}
				stripped.Delete(k.md)
			}
		}
		if stripped != nil {
			md = stripped
			ctx = metadata.NewIncomingContext(ctx, md)
		}
	}

	info := xcontext.FromMetadata(md, s.infoOptions())
	var ip netip.Addr
	if trusted {
		if v := firstMD(md, mdClientIP); v != "" {
			ip = xcontext.AddrFromHostPort(v)
		}
	}
	if ip.IsValid() {
		info.ClientIP = ip
	} else if !inproc {
		info.ClientIP = s.cfg.trustedProxies.ClientIP(addr, md.Get(s.cfg.trustedProxies.Header()))
	}
	if info.RequestID == "" {
		info.RequestID = xcontext.NewRequestID()
	}
	ctx = xcontext.NewContext(ctx, info)
	if trusted {
		if uid := firstMD(md, mdUserID); uid != "" {
			ctx = xcontext.WithUserID(ctx, uid)
		}
	}
	return ctx
}

// firstMD returns the first value of the lower-case key k.
func firstMD(md metadata.MD, k string) string {
	if v := md[k]; len(v) > 0 {
		return strings.TrimSpace(v[0])
	}
	return ""
}

func (s *Server) requestInfoUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	return h(s.requestContext(ctx), req)
}

func (s *Server) requestInfoStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	return h(srv, &ctxStream{ServerStream: ss, ctx: s.requestContext(ss.Context())})
}

// --- logging ---

// codeLevel: success at debug (high volume), client errors at info,
// conditions worth a look at warn, server faults at error.
func codeLevel(c codes.Code) log.Level {
	switch c {
	case codes.OK:
		return log.LevelDebug
	case codes.Canceled, codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.Unauthenticated:
		return log.LevelInfo
	case codes.DeadlineExceeded, codes.PermissionDenied, codes.ResourceExhausted, codes.FailedPrecondition,
		codes.Aborted, codes.OutOfRange, codes.Unavailable:
		return log.LevelWarn
	default:
		return log.LevelError
	}
}

func (s *Server) logRPC(ctx context.Context, method string, start time.Time, err error, req any) {
	if isHealthMethod(method) {
		return
	}
	code := errs.Code(err)
	lvl := codeLevel(code)
	if !s.log.Enabled(ctx, lvl) {
		return
	}
	attrs := []log.Attr{
		log.String("rpc.method", method),
		log.String("rpc.grpc.status_code", code.String()),
		log.Duration("duration", time.Since(start)),
	}
	if err != nil {
		attrs = append(attrs, log.String("error", err.Error()))
	}
	if s.cfg.payloadLogging && req != nil && s.log.Enabled(ctx, log.LevelDebug) {
		if m, ok := req.(proto.Message); ok {
			attrs = append(attrs, log.String("request", protojson.Format(m)))
		}
	}
	s.log.LogAttrs(ctx, lvl, "grpc call", attrs...)
}

func (s *Server) logUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := h(ctx, req)
	s.logRPC(ctx, info.FullMethod, start, err, req)
	return resp, err
}

func (s *Server) logStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	start := time.Now()
	err := h(srv, ss)
	s.logRPC(ss.Context(), info.FullMethod, start, err, nil)
	return err
}

// --- validation ---

// pgvMultiError and pgvFieldError match protoc-gen-validate generated types.
type (
	pgvAll        interface{ ValidateAll() error }
	pgvOne        interface{ Validate() error }
	pgvMultiError interface{ AllErrors() []error }
	pgvFieldError interface {
		Field() string
		Reason() string
	}
)

func (s *Server) validate(m any) error {
	msg, ok := m.(proto.Message)
	if !ok {
		return nil
	}
	if err := s.validator.Validate(msg); err != nil {
		if e, ok := validation.FromError(err); ok {
			return e
		}
		// A compilation or runtime error of the rules is a server bug; the
		// errs.Handler logs the cause.
		return errs.Wrap(err, errs.ErrInternal)
	}
	var err error
	switch v := m.(type) {
	case pgvAll:
		err = v.ValidateAll()
	case pgvOne:
		err = v.Validate()
	}
	if err == nil {
		return nil
	}
	all := []error{err}
	if me, ok := err.(pgvMultiError); ok { //nolint:errorlint // generated type, never wrapped
		all = me.AllErrors()
	}
	var vs []errs.FieldViolation
	for _, e := range all {
		if fe, ok := e.(pgvFieldError); ok { //nolint:errorlint // generated type, never wrapped
			vs = append(vs, errs.FieldViolation{Field: fe.Field(), Description: fe.Reason()})
		}
	}
	return errs.Wrap(err, errs.BadRequest(vs...))
}

func (s *Server) validateUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	return h(ctx, req)
}

func (s *Server) validateStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	return h(srv, &validatingStream{ServerStream: ss, s: s})
}

type validatingStream struct {
	grpc.ServerStream
	s *Server
}

func (v *validatingStream) RecvMsg(m any) error {
	if err := v.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	return v.s.validate(m)
}
