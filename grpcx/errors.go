package grpcx

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dungnh3/kitchen/errs"
)

// ErrCircuitOpen is the cause of the error returned while the circuit
// breaker is open: errs.ErrDependencyUnavailable wrapping it.
var ErrCircuitOpen = errors.New("grpcx: circuit breaker is open")

// StatusError is the status a downstream (or the gRPC client itself)
// returned for a call. It is the cause of the error a Conn returns (see
// ErrorMapper); get it with errors.AsType[*grpcx.StatusError](err), or use
// Status and Code.
//
// It has no GRPCStatus method on purpose: a StatusError that reaches a
// kitchen server unmapped is sanitised to INTERNAL instead of forwarding
// the downstream status to our own clients.
type StatusError struct {
	// Method is the full method name, "/pkg.Service/Method".
	Method string
	// Status is the downstream status: code, message and details.
	Status *status.Status
	// ctxErr is the caller's context error when the call ended because the
	// context was cancelled or expired.
	ctxErr error
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("grpcx: %s: %s: %s", e.Method, e.Status.Code(), e.Status.Message())
}

// Unwrap returns the caller's context error (context.Canceled or
// context.DeadlineExceeded) when the call ended because of it, so
// errors.Is(err, context.Canceled) works.
func (e *StatusError) Unwrap() error { return e.ctxErr }

// Status returns the downstream status of a *StatusError in err's chain,
// or nil.
func Status(err error) *status.Status {
	if se, ok := errors.AsType[*StatusError](err); ok {
		return se.Status
	}
	return nil
}

// Code returns the downstream gRPC code of a *StatusError in err's chain:
// codes.OK for a nil error and codes.Unknown when there is none.
func Code(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	if st := Status(err); st != nil {
		return st.Code()
	}
	return codes.Unknown
}

// ErrorMapper turns a failed call into the error returned to the caller. It
// should wrap se so errors.AsType still finds it.
type ErrorMapper func(se *StatusError) error

// DefaultErrorMapper maps a downstream status to an errs error wrapping se,
// with the same rule as httpx, so a kitchen server answers its own clients
// with a sensible code and never with the downstream's message:
//
//   - NOT_FOUND → errs.ErrNotFound
//   - DEADLINE_EXCEEDED → errs.ErrUpstreamTimeout
//   - UNAVAILABLE, RESOURCE_EXHAUSTED → errs.ErrDependencyUnavailable, with
//     the downstream RetryInfo when it sent one
//   - CANCELLED → errs.ErrCanceled
//   - anything else → errs.ErrInternal: a downstream INVALID_ARGUMENT,
//     PERMISSION_DENIED or UNAUTHENTICATED is our request or our credential,
//     not our caller's.
//
// To give a downstream reason a meaning of its own, rebuild it with
// errs.FromStatus(se.Status) in a custom mapper and fall back to this one.
func DefaultErrorMapper(se *StatusError) error {
	switch se.Status.Code() {
	case codes.NotFound:
		return errs.Wrap(se, errs.ErrNotFound)
	case codes.DeadlineExceeded:
		return errs.Wrap(se, errs.ErrUpstreamTimeout)
	case codes.Unavailable, codes.ResourceExhausted:
		var opts []errs.Option
		for _, d := range se.Status.Details() {
			if ri, ok := d.(*errdetails.RetryInfo); ok && ri.GetRetryDelay().AsDuration() > 0 {
				opts = append(opts, errs.RetryAfter(ri.GetRetryDelay().AsDuration()))
				break
			}
		}
		return errs.Wrap(se, errs.ErrDependencyUnavailable, opts...)
	case codes.Canceled:
		return errs.Wrap(se, errs.ErrCanceled)
	default:
		return errs.Wrap(se, errs.ErrInternal)
	}
}

// mapError builds the StatusError of a failed call and passes it through
// mapper (nil returns it as is). io.EOF (end of a stream) and the error of
// an open circuit breaker pass through unchanged.
func mapError(ctx context.Context, method string, err error, mapper ErrorMapper) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, ErrCircuitOpen) {
		return err
	}
	se := &StatusError{Method: method, Status: status.Convert(err)}
	if c := se.Status.Code(); c == codes.Canceled || c == codes.DeadlineExceeded {
		se.ctxErr = ctx.Err()
	}
	if mapper == nil {
		return se
	}
	return mapper(se)
}

func errorUnary(mapper ErrorMapper) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return mapError(ctx, method, invoker(ctx, method, req, reply, cc, opts...), mapper)
	}
}

func errorStream(mapper ErrorMapper) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, mapError(ctx, method, err, mapper)
		}
		return &mappedStream{ClientStream: s, method: method, mapper: mapper}, nil
	}
}

// mappedStream maps the errors of a stream's operations; io.EOF (the end
// of the stream) passes through.
type mappedStream struct {
	grpc.ClientStream
	method string
	mapper ErrorMapper
}

func (s *mappedStream) mapErr(err error) error {
	return mapError(s.Context(), s.method, err, s.mapper)
}

func (s *mappedStream) SendMsg(m any) error { return s.mapErr(s.ClientStream.SendMsg(m)) }

func (s *mappedStream) RecvMsg(m any) error { return s.mapErr(s.ClientStream.RecvMsg(m)) }

func (s *mappedStream) CloseSend() error { return s.mapErr(s.ClientStream.CloseSend()) }
