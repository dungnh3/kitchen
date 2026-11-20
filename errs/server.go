package errs

import (
	"context"
	"log/slog"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Localizer translates an ErrorInfo reason for the request in ctx (package
// i18n's Bundle implements it). ok is false when no translation exists.
type Localizer interface {
	Localize(ctx context.Context, reason string) (locale, message string, ok bool)
}

// ServerConfig configures the server interceptors. The zero value is valid:
// slog.Default() and no localization.
type ServerConfig struct {
	Logger    *slog.Logger
	Localizer Localizer
}

// UnaryServerInterceptor converts every handler error with ToStatus. It logs
// sanitized errors and Internal/Unknown/DataLoss errors with their cause,
// and appends a google.rpc.LocalizedMessage when a Localizer knows the
// reason. Install it outermost, right after panic recovery.
func UnaryServerInterceptor(cfg ServerConfig) grpc.UnaryServerInterceptor {
	f := newFinalizer(cfg)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			return resp, f.finalize(ctx, info.FullMethod, err)
		}
		return resp, nil
	}
}

// StreamServerInterceptor is the streaming counterpart of
// UnaryServerInterceptor.
func StreamServerInterceptor(cfg ServerConfig) grpc.StreamServerInterceptor {
	f := newFinalizer(cfg)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := handler(srv, ss); err != nil {
			return f.finalize(ss.Context(), info.FullMethod, err)
		}
		return nil
	}
}

type finalizer struct {
	log *slog.Logger
	loc Localizer
}

func newFinalizer(cfg ServerConfig) finalizer {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return finalizer{log: log, loc: cfg.Localizer}
}

func (f finalizer) finalize(ctx context.Context, method string, err error) error {
	st, sanitized := ToStatus(err)
	switch {
	case sanitized:
		f.log.ErrorContext(ctx, "unhandled error sanitized to INTERNAL",
			slog.String("rpc.method", method), slog.Any("error", err))
	case isServerFault(st.Code()):
		f.log.ErrorContext(ctx, "server error",
			slog.String("rpc.method", method), slog.String("rpc.code", st.Code().String()), slog.Any("error", err))
	}
	if f.loc != nil {
		st = localize(ctx, f.loc, st)
	}
	return st.Err()
}

func isServerFault(c codes.Code) bool {
	return c == codes.Internal || c == codes.Unknown || c == codes.DataLoss
}

func localize(ctx context.Context, loc Localizer, st *status.Status) *status.Status {
	info := infoOf(st)
	reason := reasonFor(st.Code())
	if info != nil && info.GetReason() != "" {
		reason = info.GetReason()
	}
	for _, d := range st.Details() {
		if _, ok := d.(*errdetails.LocalizedMessage); ok {
			return st // already localized upstream
		}
	}
	locale, msg, ok := loc.Localize(ctx, reason)
	if !ok {
		return st
	}
	if out, err := st.WithDetails(&errdetails.LocalizedMessage{Locale: locale, Message: msg}); err == nil {
		return out
	}
	return st
}
