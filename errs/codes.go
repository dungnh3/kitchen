package errs

import (
	"context"
	"errors"
	"net/http"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Generic sentinels, one per gRPC code. Their domain is empty, so each one
// matches (errors.Is) any *Error with the same reason in any domain. Their
// reason is the code name, which is also the i18n message ID used by the
// default catalogs in package i18n.
var (
	ErrCanceled           = New(codes.Canceled, "CANCELED", Message("request canceled"))
	ErrInvalidArgument    = New(codes.InvalidArgument, "INVALID_ARGUMENT", Message("invalid argument"))
	ErrDeadlineExceeded   = New(codes.DeadlineExceeded, "DEADLINE_EXCEEDED", Message("deadline exceeded"))
	ErrNotFound           = New(codes.NotFound, "NOT_FOUND", Message("not found"))
	ErrAlreadyExists      = New(codes.AlreadyExists, "ALREADY_EXISTS", Message("already exists"))
	ErrPermissionDenied   = New(codes.PermissionDenied, "PERMISSION_DENIED", Message("permission denied"))
	ErrResourceExhausted  = New(codes.ResourceExhausted, "RESOURCE_EXHAUSTED", Message("resource exhausted"))
	ErrFailedPrecondition = New(codes.FailedPrecondition, "FAILED_PRECONDITION", Message("failed precondition"))
	ErrAborted            = New(codes.Aborted, "ABORTED", Message("aborted"))
	ErrOutOfRange         = New(codes.OutOfRange, "OUT_OF_RANGE", Message("out of range"))
	ErrUnimplemented      = New(codes.Unimplemented, "UNIMPLEMENTED", Message("not implemented"))
	ErrInternal           = New(codes.Internal, "INTERNAL", Message("internal error"))
	ErrUnavailable        = New(codes.Unavailable, "UNAVAILABLE", Message("service unavailable"))
	ErrUnauthenticated    = New(codes.Unauthenticated, "UNAUTHENTICATED", Message("unauthenticated"))
)

var codeReasons = map[codes.Code]string{
	codes.Canceled:           "CANCELED",
	codes.Unknown:            "UNKNOWN",
	codes.InvalidArgument:    "INVALID_ARGUMENT",
	codes.DeadlineExceeded:   "DEADLINE_EXCEEDED",
	codes.NotFound:           "NOT_FOUND",
	codes.AlreadyExists:      "ALREADY_EXISTS",
	codes.PermissionDenied:   "PERMISSION_DENIED",
	codes.ResourceExhausted:  "RESOURCE_EXHAUSTED",
	codes.FailedPrecondition: "FAILED_PRECONDITION",
	codes.Aborted:            "ABORTED",
	codes.OutOfRange:         "OUT_OF_RANGE",
	codes.Unimplemented:      "UNIMPLEMENTED",
	codes.Internal:           "INTERNAL",
	codes.Unavailable:        "UNAVAILABLE",
	codes.DataLoss:           "DATA_LOSS",
	codes.Unauthenticated:    "UNAUTHENTICATED",
}

// reasonFor returns the canonical upper-snake name of c, which is also the
// "status" field of the AIP-193 HTTP envelope.
func reasonFor(c codes.Code) string {
	if r, ok := codeReasons[c]; ok {
		return r
	}
	if c == codes.OK {
		return "OK"
	}
	return "UNKNOWN"
}

// Code returns the gRPC code of err: OK for nil, Canceled/DeadlineExceeded
// for the context errors, the code of an *Error or of anything implementing
// GRPCStatus, and Unknown otherwise.
func Code(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	var e *Error
	if errors.As(err, &e) {
		return e.code
	}
	if st, ok := status.FromError(err); ok {
		return st.Code()
	}
	switch {
	case errors.Is(err, context.Canceled):
		return codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded
	}
	return codes.Unknown
}

// HTTPStatusFromCode maps a gRPC code to an HTTP status, following
// google.rpc.Code and grpc-gateway. Unknown codes map to 500.
func HTTPStatusFromCode(c codes.Code) int {
	switch c {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		return 499 // client closed request
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default: // Unknown, Internal, DataLoss and anything else
		return http.StatusInternalServerError
	}
}

// HTTPStatus returns HTTPStatusFromCode(Code(err)).
func HTTPStatus(err error) int { return HTTPStatusFromCode(Code(err)) }

// ToStatus converts err into the status that is safe to send to a client.
//
//   - nil → nil, false.
//   - *Error (anywhere in the chain) → its GRPCStatus (cause stripped).
//   - an error carrying a gRPC status → that status, details untouched.
//   - context.Canceled / DeadlineExceeded → CANCELED / DEADLINE_EXCEEDED.
//   - anything else → INTERNAL "internal error"; sanitized is true and the
//     caller should log err.
func ToStatus(err error) (st *status.Status, sanitized bool) {
	if err == nil {
		return nil, false
	}
	var e *Error
	if errors.As(err, &e) {
		return e.GRPCStatus(), false
	}
	if s, ok := status.FromError(err); ok {
		return s, false
	}
	switch {
	case errors.Is(err, context.Canceled):
		return ErrCanceled.GRPCStatus(), false
	case errors.Is(err, context.DeadlineExceeded):
		return ErrDeadlineExceeded.GRPCStatus(), false
	}
	return ErrInternal.GRPCStatus(), true
}

// infoOf returns the first ErrorInfo in st's details.
func infoOf(st *status.Status) *errdetails.ErrorInfo {
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info
		}
	}
	return nil
}
