// Package errs is kitchen's error model: immutable, comparable errors that
// carry a gRPC code and a standard google.rpc.ErrorInfo (AIP-193).
//
// Define sentinels once and wrap them with context at the call site:
//
//	var ErrAgentNotFound = errs.New(codes.NotFound, "AGENT_NOT_FOUND",
//		errs.Domain("agents.example.com"), errs.Message("agent not found"))
//
//	return errs.Wrap(dbErr, ErrAgentNotFound, errs.Meta("agent_id", id))
//
// errors.Is matches by reason and domain, so it keeps working after Wrap and
// With. The wrapped cause is for logs only and never reaches the client. HTTP
// status is derived from the gRPC code (see HTTPStatusFromCode); there is no
// override.
//
// errs is a leaf package: it depends only on grpc, protobuf and the
// google.rpc errdetails types.
package errs

import (
	"errors"
	"maps"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// Error is an immutable error with a gRPC code, a machine-readable reason
// (UPPER_SNAKE_CASE) and an optional domain, message, metadata, extra
// google.rpc details and cause.
//
// Create one with New, derive copies with With or Wrap. The zero value is
// not useful.
type Error struct {
	code    codes.Code
	reason  string
	domain  string
	message string
	meta    map[string]string
	details []proto.Message
	cause   error
}

// Option configures an Error in New, With and Wrap.
type Option func(*Error)

// Domain sets ErrorInfo.domain, the logical owner of the reason (for example
// "agents.example.com"). Sentinels with an empty domain match errors with the
// same reason in any domain.
func Domain(d string) Option { return func(e *Error) { e.domain = d } }

// Message sets the client-visible, developer-facing message (the
// google.rpc.Status message). It must not contain internal details. When
// empty, the reason is used.
func Message(m string) Option { return func(e *Error) { e.message = m } }

// Meta adds one ErrorInfo.metadata entry. Values are client-visible.
func Meta(key, value string) Option {
	return func(e *Error) {
		if e.meta == nil {
			e.meta = make(map[string]string, 1)
		}
		e.meta[key] = value
	}
}

// Details appends extra google.rpc detail messages (for example
// errdetails.RetryInfo or errdetails.BadRequest). Nil messages are ignored.
func Details(ds ...proto.Message) Option {
	return func(e *Error) {
		for _, d := range ds {
			if d != nil {
				e.details = append(e.details, d)
			}
		}
	}
}

// New returns a new Error. code must not be codes.OK; reason should be a
// constant UPPER_SNAKE_CASE identifier, unique within its domain.
func New(code codes.Code, reason string, opts ...Option) *Error {
	e := &Error{code: code, reason: reason}
	for _, o := range opts {
		o(e)
	}
	return e
}

// With returns a copy of e with opts applied. e is not modified.
func (e *Error) With(opts ...Option) *Error {
	c := e.clone()
	for _, o := range opts {
		o(c)
	}
	return c
}

// Wrap returns a copy of sentinel that records cause (for logs and
// errors.Is/As on the cause chain) with opts applied. A nil cause is allowed.
func Wrap(cause error, sentinel *Error, opts ...Option) *Error {
	c := sentinel.With(opts...)
	c.cause = cause
	return c
}

func (e *Error) clone() *Error {
	c := *e
	c.meta = maps.Clone(e.meta)
	c.details = append([]proto.Message(nil), e.details...)
	return &c
}

// Code returns the gRPC code.
func (e *Error) Code() codes.Code { return e.code }

// Reason returns the ErrorInfo reason.
func (e *Error) Reason() string { return e.reason }

// Domain returns the ErrorInfo domain.
func (e *Error) Domain() string { return e.domain }

// Message returns the client-visible message, defaulting to the reason.
func (e *Error) Message() string {
	if e.message != "" {
		return e.message
	}
	return e.reason
}

// Metadata returns a copy of the ErrorInfo metadata.
func (e *Error) Metadata() map[string]string { return maps.Clone(e.meta) }

// Error implements error: "REASON: message: cause". It includes the cause
// and is meant for logs, not clients.
func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString(e.reason)
	if e.message != "" && e.message != e.reason {
		b.WriteString(": ")
		b.WriteString(e.message)
	}
	if e.cause != nil {
		b.WriteString(": ")
		b.WriteString(e.cause.Error())
	}
	return b.String()
}

// Unwrap returns the cause.
func (e *Error) Unwrap() error { return e.cause }

// Is reports whether target is an *Error with the same reason and either an
// empty domain or the same domain.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return t.reason == e.reason && (t.domain == "" || t.domain == e.domain)
}

// GRPCStatus returns the status sent to clients: code, message, an ErrorInfo
// and any extra details. The cause is never included.
func (e *Error) GRPCStatus() *status.Status {
	st := status.New(e.code, e.Message())
	ds := make([]protoadapt.MessageV1, 0, 1+len(e.details))
	ds = append(ds, &errdetails.ErrorInfo{Reason: e.reason, Domain: e.domain, Metadata: maps.Clone(e.meta)})
	for _, d := range e.details {
		ds = append(ds, protoadapt.MessageV1Of(d))
	}
	if withDetails, err := st.WithDetails(ds...); err == nil {
		return withDetails
	}
	return st
}

// Info returns the first google.rpc.ErrorInfo of err. It works for *Error
// and for any error carrying a gRPC status, such as one received by a client.
func Info(err error) (*errdetails.ErrorInfo, bool) {
	if err == nil {
		return nil, false
	}
	var e *Error
	if errors.As(err, &e) {
		return &errdetails.ErrorInfo{Reason: e.reason, Domain: e.domain, Metadata: maps.Clone(e.meta)}, true
	}
	st, ok := status.FromError(err)
	if !ok {
		return nil, false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info, true
		}
	}
	return nil, false
}

// FromStatus rebuilds an *Error from a gRPC status, typically one received
// by a client, so errors.Is works against local sentinels. The first
// ErrorInfo provides reason, domain and metadata; other details are kept.
// A status without ErrorInfo gets the code name as reason (for example
// "NOT_FOUND"). It returns nil for a nil or OK status.
func FromStatus(st *status.Status) *Error {
	if st == nil || st.Code() == codes.OK {
		return nil
	}
	e := &Error{code: st.Code(), reason: reasonFor(st.Code()), message: st.Message()}
	haveInfo := false
	for _, d := range st.Details() {
		m, ok := d.(proto.Message)
		if !ok {
			continue // undecodable detail (unknown type)
		}
		if info, ok := m.(*errdetails.ErrorInfo); ok && !haveInfo {
			haveInfo = true
			e.reason, e.domain, e.meta = info.GetReason(), info.GetDomain(), maps.Clone(info.GetMetadata())
			continue
		}
		e.details = append(e.details, m)
	}
	return e
}
