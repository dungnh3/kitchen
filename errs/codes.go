package errs

import (
	"context"
	"errors"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	rpccode "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Code sentinels, one per gRPC code. Their reason is the code name (also the
// i18n message ID) and their domain is empty, so errors.Is(err, ErrNotFound)
// is true for every *Error whose code is NotFound, whatever its reason.
var (
	ErrCanceled           = generic(codes.Canceled, "request canceled")
	ErrUnknown            = generic(codes.Unknown, "unknown error")
	ErrInvalidArgument    = generic(codes.InvalidArgument, "invalid argument")
	ErrDeadlineExceeded   = generic(codes.DeadlineExceeded, "deadline exceeded")
	ErrNotFound           = generic(codes.NotFound, "not found")
	ErrAlreadyExists      = generic(codes.AlreadyExists, "already exists")
	ErrPermissionDenied   = generic(codes.PermissionDenied, "permission denied")
	ErrResourceExhausted  = generic(codes.ResourceExhausted, "resource exhausted")
	ErrFailedPrecondition = generic(codes.FailedPrecondition, "failed precondition")
	ErrAborted            = generic(codes.Aborted, "aborted")
	ErrOutOfRange         = generic(codes.OutOfRange, "out of range")
	ErrUnimplemented      = generic(codes.Unimplemented, "not implemented")
	ErrInternal           = generic(codes.Internal, "internal error")
	ErrUnavailable        = generic(codes.Unavailable, "service unavailable")
	ErrDataLoss           = generic(codes.DataLoss, "data loss")
	ErrUnauthenticated    = generic(codes.Unauthenticated, "unauthenticated")
)

var codeSentinels = map[codes.Code]*Error{}

func init() {
	for _, e := range []*Error{
		ErrCanceled, ErrUnknown, ErrInvalidArgument, ErrDeadlineExceeded, ErrNotFound, ErrAlreadyExists,
		ErrPermissionDenied, ErrResourceExhausted, ErrFailedPrecondition, ErrAborted, ErrOutOfRange,
		ErrUnimplemented, ErrInternal, ErrUnavailable, ErrDataLoss, ErrUnauthenticated,
	} {
		codeSentinels[e.code] = e
	}
}

// codeSentinel returns the generic sentinel of c (ErrUnknown for unknown
// codes).
func codeSentinel(c codes.Code) *Error {
	if e, ok := codeSentinels[c]; ok {
		return e
	}
	return ErrUnknown
}

func generic(c codes.Code, msg string) *Error {
	return New(c, CodeName(c), Message(msg))
}

// CodeName returns the canonical google.rpc.Code name of c ("NOT_FOUND"),
// which is also the "status" field of the AIP-193 HTTP envelope.
func CodeName(c codes.Code) string {
	if c > codes.Unauthenticated { // the last google.rpc.Code
		return rpccode.Code_UNKNOWN.String()
	}
	if name, ok := rpccode.Code_name[int32(c)]; ok {
		return name
	}
	return rpccode.Code_UNKNOWN.String()
}

// Code returns the gRPC code of err: OK for nil, the code of an *Error or of
// anything carrying a gRPC status, Canceled/DeadlineExceeded for the context
// errors, and Unknown otherwise.
func Code(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	var e *Error
	if errors.As(err, &e) {
		if e == nil {
			return codes.Internal
		}
		return e.code
	}
	if st, ok := statusOf(err); ok {
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

// HTTPStatusFromCode maps a gRPC code to an HTTP status with grpc-gateway's
// table (google.rpc.Code mapping, 499 for Canceled).
func HTTPStatusFromCode(c codes.Code) int { return runtime.HTTPStatusFromCode(c) }

// httpStatusByReason lists the only exceptions to HTTPStatusFromCode: catalog
// reasons with a dedicated HTTP status (RFC 9110) that HTTP clients, proxies
// and upload libraries act on. They apply to INVALID_ARGUMENT only, so the
// gRPC code stays the source of truth.
var httpStatusByReason = map[string]int{
	"PAYLOAD_TOO_LARGE":      http.StatusRequestEntityTooLarge, // 413
	"UNSUPPORTED_MEDIA_TYPE": http.StatusUnsupportedMediaType,  // 415
}

func httpStatusOf(c codes.Code, reason string) int {
	if c == codes.InvalidArgument {
		if s, ok := httpStatusByReason[reason]; ok {
			return s
		}
	}
	return HTTPStatusFromCode(c)
}

// HTTPStatusFromStatus returns the HTTP status for st: HTTPStatusFromCode of
// its code, except 413 for PAYLOAD_TOO_LARGE and 415 for
// UNSUPPORTED_MEDIA_TYPE (any domain). A nil st is 500.
func HTTPStatusFromStatus(st *status.Status) int {
	if st == nil {
		return http.StatusInternalServerError
	}
	return httpStatusOf(st.Code(), infoOf(st).GetReason())
}

// HTTPStatus returns the HTTP status for err, as HTTPStatusFromStatus.
func HTTPStatus(err error) int {
	info, _ := Info(err)
	return httpStatusOf(Code(err), info.GetReason())
}
