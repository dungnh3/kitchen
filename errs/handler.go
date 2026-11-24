package errs

import (
	"context"
	"errors"
	"net/http"

	"github.com/dungnh3/kitchen/log"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Localizer translates an ErrorInfo for the request in ctx. Package i18n's
// Bundle implements it; metadata is available to message templates. ok is
// false when no translation exists.
type Localizer interface {
	Localize(ctx context.Context, info *errdetails.ErrorInfo) (locale, message string, ok bool)
}

// Config configures a Handler. The zero value is valid: log.Default(), no
// default domain and no localization.
type Config struct {
	// Logger receives sanitized and server-fault errors with their cause.
	Logger *log.Logger
	// Localizer, when set, adds a google.rpc.LocalizedMessage.
	Localizer Localizer
	// Domain is put in ErrorInfo.domain when an error has none (AIP-193
	// requires one), for example "agents.example.com".
	Domain string
}

// Handler converts errors into the statuses and HTTP responses sent to
// clients. Build one at startup and use it for every transport so gRPC, the
// REST gateway and plain HTTP handlers produce the same errors:
//
//	eh := errs.NewHandler(errs.Config{Logger: log, Localizer: bundle, Domain: "agents.example.com"})
//	grpc.ChainUnaryInterceptor(recovery, eh.UnaryServerInterceptor())
//	runtime.WithErrorHandler(eh.GatewayErrorHandler)
//	eh.WriteHTTP(w, r, err) // in plain HTTP handlers
//
// A Handler is safe for concurrent use.
type Handler struct {
	log    *log.Logger
	loc    Localizer
	domain string
}

// NewHandler returns a Handler for cfg.
func NewHandler(cfg Config) *Handler {
	return &Handler{log: cfg.Logger, loc: cfg.Localizer, domain: cfg.Domain}
}

var defaultHandler = &Handler{}

func (h *Handler) logger() *log.Logger {
	if h.log != nil {
		return h.log
	}
	return log.Default()
}

// ToStatus converts err into the status that is safe to send to a client.
// It does not log; Handler.Status does.
//
//   - nil → nil, false.
//   - *Error anywhere in the chain → its GRPCStatus (cause stripped); a nil
//     *Error or one with code OK → INTERNAL (sanitized).
//   - a foreign gRPC status anywhere in the chain (typically returned by a
//     downstream service) → sanitizeForeign: client errors keep their code,
//     message and details; other codes keep their code with the generic
//     message and only RetryInfo; Unauthenticated and PermissionDenied
//     become INTERNAL, since they report this service's own credentials on
//     an outgoing call, not the client's.
//   - context.Canceled / DeadlineExceeded → CANCELED / DEADLINE_EXCEEDED.
//   - anything else → INTERNAL "internal error" (sanitized).
//
// When sanitized is true the caller should log err.
func ToStatus(err error) (st *status.Status, sanitized bool) {
	if err == nil {
		return nil, false
	}
	var e *Error
	if errors.As(err, &e) {
		if e == nil || e.code == codes.OK {
			return ErrInternal.GRPCStatus(), true
		}
		return e.GRPCStatus(), false
	}
	if s, ok := statusOf(err); ok {
		return sanitizeForeign(s)
	}
	switch {
	case errors.Is(err, context.Canceled):
		return ErrCanceled.GRPCStatus(), false
	case errors.Is(err, context.DeadlineExceeded):
		return ErrDeadlineExceeded.GRPCStatus(), false
	}
	return ErrInternal.GRPCStatus(), true
}

// Status converts err with ToStatus, logs it when it was sanitized or is a
// server fault (Internal, Unknown, DataLoss), and completes the status: an
// ErrorInfo is always present, its empty domain is set to Config.Domain, and
// a LocalizedMessage is added when the Localizer knows the reason. It
// returns nil for nil.
func (h *Handler) Status(ctx context.Context, err error) *status.Status {
	return h.status(ctx, err, "")
}

func (h *Handler) status(ctx context.Context, err error, op string) *status.Status {
	if err == nil {
		return nil
	}
	r := h.render(ctx, err, op)
	return r.status()
}

// render converts err (sanitizing and logging like Status) and completes it.
func (h *Handler) render(ctx context.Context, err error, op string) rendered {
	r, sanitized := renderError(err)
	h.logFault(ctx, op, err, r.code, sanitized)
	h.completeRendered(ctx, &r)
	return r
}

func (h *Handler) logFault(ctx context.Context, op string, err error, code codes.Code, sanitized bool) {
	if !sanitized && !isServerFault(code) {
		return
	}
	attrs := []log.Attr{log.String("code", CodeName(code)), log.Any("error", err)}
	if op != "" {
		attrs = append(attrs, log.String("operation", op))
	}
	if sanitized {
		h.logger().LogAttrs(ctx, log.LevelError, "error sanitized to INTERNAL", attrs...)
		return
	}
	h.logger().LogAttrs(ctx, log.LevelError, "server error", attrs...)
}

// sanitizeForeign makes a status this service did not create safe to send:
//
//   - client errors (InvalidArgument, NotFound, AlreadyExists,
//     FailedPrecondition, OutOfRange, ResourceExhausted, Aborted, Canceled)
//     are kept, details included;
//   - Unauthenticated, PermissionDenied and OK become INTERNAL;
//   - every other code (Unknown, Internal, DataLoss, Unavailable,
//     DeadlineExceeded, Unimplemented) keeps its code with the generic
//     message of that code and only a RetryInfo detail, so no downstream
//     error text (addresses, SQL errors) reaches the client.
//
// sanitized reports that the status was changed; the caller logs err.
func sanitizeForeign(s *status.Status) (st *status.Status, sanitized bool) {
	switch s.Code() {
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.FailedPrecondition,
		codes.OutOfRange, codes.ResourceExhausted, codes.Aborted, codes.Canceled:
		return s, false
	case codes.Unauthenticated, codes.PermissionDenied, codes.OK:
		return ErrInternal.GRPCStatus(), true
	}
	out := status.New(s.Code(), codeSentinel(s.Code()).Message())
	for _, d := range s.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			if ds, err := out.WithDetails(ri); err == nil {
				out = ds
			}
			break
		}
	}
	return out, true
}

func isServerFault(c codes.Code) bool {
	return c == codes.Internal || c == codes.Unknown || c == codes.DataLoss
}

// UnaryServerInterceptor converts every handler error with Status. Install
// it outermost, right after panic recovery.
func (h *Handler) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			return resp, h.status(ctx, err, info.FullMethod).Err()
		}
		return resp, nil
	}
}

// StreamServerInterceptor is the streaming counterpart of
// UnaryServerInterceptor.
func (h *Handler) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := handler(srv, ss); err != nil {
			return h.status(ss.Context(), err, info.FullMethod).Err()
		}
		return nil
	}
}

// WriteHTTP writes err as an AIP-193 JSON error response for a plain HTTP
// handler; see Status. It sets Content-Type, Retry-After when the status
// carries a RetryInfo, and WWW-Authenticate for 401.
func (h *Handler) WriteHTTP(w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		err = ErrInternal
	}
	h.render(r.Context(), err, r.Method+" "+r.URL.Path).write(w, 0)
}

// GatewayErrorHandler is a grpc-gateway runtime.ErrorHandlerFunc
// (runtime.WithErrorHandler) that renders errors as the AIP-193 envelope.
//
// Errors from the in-process gRPC call were already converted by the
// interceptors, so they are not sanitized again; errors produced by the
// gateway itself (bad JSON, unknown route) are completed the same way. An
// HTTP status chosen by the gateway (runtime.HTTPStatusError, such as 405)
// is kept.
func (h *Handler) GatewayErrorHandler(ctx context.Context, _ *runtime.ServeMux, _ runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
	httpCode := 0
	// HTTPStatusError has no Unwrap method; look inside it explicitly.
	var hse *runtime.HTTPStatusError
	if errors.As(err, &hse) {
		if hse.HTTPStatus >= 400 && hse.HTTPStatus <= 599 {
			httpCode = hse.HTTPStatus
		}
		err = hse.Err
	}
	if err == nil { // HTTPStatusError without a cause
		err = gatewaySentinel(httpCode)
	}
	var e *Error
	if errors.As(err, &e) && e != nil && e.code != codes.OK {
		rr := renderOf(e) // produced on this side of the gateway
		h.completeRendered(ctx, &rr)
		rr.write(w, httpCode)
		return
	}
	st, ok := statusOf(err)
	var rr rendered
	if ok {
		rr = renderStatus(st)
	}
	if !ok || rr.info == nil {
		// Not completed by an errs.Handler: a gateway error (bad JSON) or a
		// status from a backend that is not a kitchen service.
		h.gatewayUntrusted(ctx, w, r, err, httpCode)
		return
	}
	h.completeRendered(ctx, &rr)
	rr.write(w, httpCode) // 0: from the status
}

// gatewaySentinel is the error for a gateway HTTP status without a cause.
func gatewaySentinel(httpCode int) *Error {
	switch httpCode {
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return ErrUnimplemented
	case http.StatusBadRequest:
		return ErrInvalidArgument
	}
	return ErrInternal
}

// gatewayUntrusted handles a gateway error that no errs.Handler completed:
// it is converted with Status (sanitized and logged). An HTTP status the
// gateway chose is kept when the error kept its code.
func (h *Handler) gatewayUntrusted(ctx context.Context, w http.ResponseWriter, r *http.Request, err error, httpCode int) {
	rr := h.render(ctx, err, r.Method+" "+r.URL.Path)
	if rr.code != Code(err) {
		httpCode = 0 // sanitized: derive it from the new status
	}
	rr.write(w, httpCode)
}

// WriteHTTP writes err with a Handler that has no default domain and no
// localization, logging to log.Default(). Prefer a configured Handler.
func WriteHTTP(w http.ResponseWriter, r *http.Request, err error) {
	defaultHandler.WriteHTTP(w, r, err)
}
