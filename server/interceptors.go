package server

import (
	"context"
	"errors"
	"log/slog"
	"net/netip"
	"runtime/debug"
	"strings"
	"time"

	"buf.build/go/protovalidate"
	"github.com/dungnh3/kitchen/reqctx"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const healthMethodPrefix = "/grpc.health.v1.Health/"

func isHealthMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, healthMethodPrefix)
}

// chain builds the interceptor chains, outermost first:
//
//	recovery → drain → request info → logging → user interceptors → validation
func (s *Server) chain() ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	unary := []grpc.UnaryServerInterceptor{
		s.recoverUnary, drainUnary(s.drain), s.reqctxUnary, s.logUnary,
	}
	unary = append(unary, s.cfg.unary...)
	stream := []grpc.StreamServerInterceptor{
		s.recoverStream, drainStream(s.drain), s.reqctxStream, s.logStream,
	}
	stream = append(stream, s.cfg.stream...)
	if s.validator != nil {
		unary = append(unary, s.validateUnary)
		stream = append(stream, s.validateStream)
	}
	return unary, stream
}

// --- recovery ---

func (s *Server) recovered(ctx context.Context, method string, p any) error {
	s.log.LogAttrs(ctx, slog.LevelError, "panic recovered",
		slog.String("rpc.method", method),
		slog.Any("panic", p),
		slog.String("stack", string(debug.Stack())))
	if s.cfg.recovery != nil {
		return s.cfg.recovery(ctx, p)
	}
	return status.Error(codes.Internal, "internal error")
}

func (s *Server) recoverUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if p := recover(); p != nil {
			resp, err = nil, s.recovered(ctx, info.FullMethod, p)
		}
	}()
	return h(ctx, req)
}

func (s *Server) recoverStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = s.recovered(ss.Context(), info.FullMethod, p)
		}
	}()
	return h(srv, ss)
}

// --- request info and identity sanitising ---

func peerAddr(ctx context.Context) (netip.Addr, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return netip.Addr{}, false
	}
	if p.Addr.Network() == inprocNetwork {
		return netip.Addr{}, true
	}
	return reqctx.AddrFromString(p.Addr.String()), false
}

// requestContext strips untrusted identity metadata and stores the request
// Info (and, from trusted peers, the propagated user id) in ctx.
func (s *Server) requestContext(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	addr, inproc := peerAddr(ctx)
	trusted := inproc || s.cfg.trustAllIdentity || s.cfg.trustedIdentityPeers.Contains(addr)
	if !trusted {
		var stripped metadata.MD
		for _, h := range reqctx.IdentityHeaders() {
			if len(md.Get(h)) > 0 {
				if stripped == nil {
					stripped = md.Copy()
				}
				stripped.Delete(h)
			}
		}
		if stripped != nil {
			md = stripped
			ctx = metadata.NewIncomingContext(ctx, md)
		}
	}

	info := reqctx.FromMetadata(md)
	if ip := reqctx.AddrFromString(firstMD(md, reqctx.HeaderClientIP)); trusted && ip.IsValid() {
		info.ClientIP = ip
	} else if !inproc {
		info.ClientIP = s.cfg.trustedProxies.ClientIP(addr, md.Get(reqctx.HeaderForwardedFor), firstMD(md, reqctx.HeaderCFConnectingIP))
	}
	if info.RequestID == "" {
		info.RequestID = reqctx.NewRequestID()
	}
	ctx = reqctx.NewContext(ctx, info)
	if trusted {
		uid := firstMD(md, reqctx.HeaderUserID)
		if uid == "" {
			uid = firstMD(md, reqctx.HeaderXUserID)
		}
		if uid != "" {
			ctx = reqctx.WithUserID(ctx, uid)
		}
	}
	return ctx
}

func firstMD(md metadata.MD, k string) string {
	if v := md.Get(k); len(v) > 0 {
		return strings.TrimSpace(v[0])
	}
	return ""
}

func (s *Server) reqctxUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	return h(s.requestContext(ctx), req)
}

func (s *Server) reqctxStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	return h(srv, &ctxStream{ServerStream: ss, ctx: s.requestContext(ss.Context())})
}

// --- logging ---

// codeLevel: success at debug (high volume), client errors at info,
// conditions worth a look at warn, server faults at error.
func codeLevel(c codes.Code) slog.Level {
	switch c {
	case codes.OK:
		return slog.LevelDebug
	case codes.Canceled, codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.Unauthenticated:
		return slog.LevelInfo
	case codes.DeadlineExceeded, codes.PermissionDenied, codes.ResourceExhausted, codes.FailedPrecondition,
		codes.Aborted, codes.OutOfRange, codes.Unavailable:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

func (s *Server) logRPC(ctx context.Context, method string, start time.Time, err error, req any) {
	if isHealthMethod(method) {
		return
	}
	code := status.Code(err)
	lvl := codeLevel(code)
	if !s.log.Enabled(ctx, lvl) {
		return
	}
	attrs := []slog.Attr{
		slog.String("rpc.method", method),
		slog.String("rpc.grpc.status_code", code.String()),
		slog.Duration("duration", time.Since(start)),
	}
	if info, ok := reqctx.FromContext(ctx); ok {
		attrs = append(attrs, slog.String("request_id", info.RequestID))
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	if s.cfg.payloadLogging && req != nil && s.log.Enabled(ctx, slog.LevelDebug) {
		if m, ok := req.(proto.Message); ok {
			attrs = append(attrs, slog.String("request", protojson.Format(m)))
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
		var ve *protovalidate.ValidationError
		if !errors.As(err, &ve) {
			s.log.Error("protovalidate rules failed", slog.String("error", err.Error()))
			return status.Error(codes.Internal, "request validation failed")
		}
		br := &errdetails.BadRequest{}
		for _, v := range ve.Violations {
			br.FieldViolations = append(br.FieldViolations, &errdetails.BadRequest_FieldViolation{
				Field:       protovalidate.FieldPathString(v.Proto.GetField()),
				Description: v.Proto.GetMessage(),
				Reason:      v.Proto.GetRuleId(),
			})
		}
		return invalidArgument(err.Error(), br)
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
	br := &errdetails.BadRequest{}
	errs := []error{err}
	if me, ok := err.(pgvMultiError); ok { //nolint:errorlint // generated type, never wrapped
		errs = me.AllErrors()
	}
	for _, e := range errs {
		if fe, ok := e.(pgvFieldError); ok { //nolint:errorlint // generated type, never wrapped
			br.FieldViolations = append(br.FieldViolations, &errdetails.BadRequest_FieldViolation{
				Field: fe.Field(), Description: fe.Reason(),
			})
		}
	}
	return invalidArgument(err.Error(), br)
}

func invalidArgument(msg string, br *errdetails.BadRequest) error {
	st := status.New(codes.InvalidArgument, msg)
	if len(br.GetFieldViolations()) > 0 {
		if d, err := st.WithDetails(br); err == nil {
			st = d
		}
	}
	return st.Err()
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
