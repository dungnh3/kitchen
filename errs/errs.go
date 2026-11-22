// Package errs is kitchen's error model, following Google AIP-193: every
// error a client sees is a google.rpc.Status with a gRPC code, a
// developer-facing message and a google.rpc.ErrorInfo (reason, domain,
// metadata), optionally with other standard google.rpc details
// (LocalizedMessage, BadRequest, RetryInfo, ...).
//
// Define sentinels once and wrap them with context at the call site:
//
//	var ErrAgentNotFound = errs.New(codes.NotFound, "AGENT_NOT_FOUND",
//		errs.Domain("agents.example.com"), errs.Message("agent not found"))
//
//	return errs.Wrap(dbErr, ErrAgentNotFound, errs.Meta("agent_id", id))
//
// The package ships one sentinel per gRPC code (ErrNotFound, ...) and a
// catalog of reasons common to API servers (ErrTokenExpired,
// ErrVersionMismatch, ErrRateLimited, ...); package i18n ships English and
// Vietnamese messages for all of them.
//
// errors.Is matches a code sentinel (ErrNotFound) by gRPC code and any other
// sentinel by reason and domain, so it keeps working after Wrap and With.
// The wrapped cause is for logs only and never reaches the client. The HTTP
// status is derived from the gRPC code (see HTTPStatusFromCode), except 413
// for PAYLOAD_TOO_LARGE and 415 for UNSUPPORTED_MEDIA_TYPE
// (HTTPStatusFromStatus).
//
// A Handler turns any error into the status sent to clients (sanitizing
// unknown errors, filling the domain, localizing) for gRPC, the REST
// gateway and plain HTTP handlers alike.
package errs

import (
	"errors"
	"maps"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/durationpb"
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
// "agents.example.com"). Most errors leave it empty: a Handler fills in its
// Config.Domain before the error leaves the server. Set it only when one
// service reports errors on behalf of several domains.
func Domain(d string) Option { return func(e *Error) { e.domain = d } }

// Message sets the client-visible, developer-facing English message (the
// google.rpc.Status message). It must not contain internal details. When
// empty, the reason is used. End-user text comes from localization.
func Message(m string) Option { return func(e *Error) { e.message = m } }

// Meta adds one ErrorInfo.metadata entry. Values are client-visible and are
// available to localized message templates.
func Meta(key, value string) Option {
	return func(e *Error) {
		if e.meta == nil {
			e.meta = make(map[string]string, 1)
		}
		e.meta[key] = value
	}
}

// Details appends extra google.rpc detail messages (for example
// errdetails.BadRequest or errdetails.Help). Nil messages are ignored.
func Details(ds ...proto.Message) Option {
	return func(e *Error) {
		for _, d := range ds {
			if d != nil {
				e.details = append(e.details, d)
			}
		}
	}
}

// RetryAfter adds a google.rpc.RetryInfo with delay d. HTTP responses get a
// matching Retry-After header.
func RetryAfter(d time.Duration) Option {
	return Details(&errdetails.RetryInfo{RetryDelay: durationpb.New(d)})
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

// Details returns a copy of the extra google.rpc details.
func (e *Error) Details() []proto.Message { return append([]proto.Message(nil), e.details...) }

// Error implements error: "REASON: message: cause". It includes the cause
// and is meant for logs, not clients.
func (e *Error) Error() string {
	if e == nil {
		return "INTERNAL: nil *errs.Error"
	}
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
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// Is reports whether e matches target, an *Error:
//
//   - a generic target, whose reason is its code name and whose domain is
//     empty (ErrNotFound, or any copy of it), matches every *Error with the
//     same code;
//   - any other target matches when the reasons are equal and the target's
//     domain is empty or equal to e's.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok || e == nil || t == nil {
		return false
	}
	if t.isGeneric() {
		return t.code == e.code
	}
	return t.reason == e.reason && (t.domain == "" || t.domain == e.domain)
}

// isGeneric reports whether e only names a gRPC code.
func (e *Error) isGeneric() bool { return e.domain == "" && e.reason == CodeName(e.code) }

// GRPCStatus returns the status sent to clients: code, message, an ErrorInfo
// and any extra details. The cause is never included.
func (e *Error) GRPCStatus() *status.Status {
	if e == nil {
		return ErrInternal.GRPCStatus()
	}
	st := status.New(e.code, e.Message())
	ds := make([]protoadapt.MessageV1, 0, 1+len(e.details))
	ds = append(ds, e.info())
	for _, d := range e.details {
		ds = append(ds, protoadapt.MessageV1Of(d))
	}
	if withDetails, err := st.WithDetails(ds...); err == nil {
		return withDetails
	}
	return st
}

func (e *Error) info() *errdetails.ErrorInfo {
	return &errdetails.ErrorInfo{Reason: e.reason, Domain: e.domain, Metadata: maps.Clone(e.meta)}
}

// Info returns the google.rpc.ErrorInfo of err. It works for *Error and for
// any error carrying a gRPC status, such as one received by a client.
func Info(err error) (*errdetails.ErrorInfo, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e.info(), true
	}
	if st, ok := statusOf(err); ok {
		if info := infoOf(st); info != nil {
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
	e := &Error{code: st.Code(), reason: CodeName(st.Code()), message: st.Message()}
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

// FromError is FromStatus for an error returned by a gRPC client call. It
// returns the *Error already in err's chain if there is one, and nil when
// err is nil or carries no status.
func FromError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	st, ok := statusOf(err)
	if !ok {
		return nil
	}
	return FromStatus(st)
}

// grpcStatuser is implemented by grpc status errors and by *Error.
type grpcStatuser interface{ GRPCStatus() *status.Status }

// statusOf returns the status of the first error in err's chain that carries
// one. Unlike status.FromError, the message is that status's own message,
// never err.Error() of a wrapper (which would leak the wrap context).
func statusOf(err error) (*status.Status, bool) {
	var gs grpcStatuser
	if !errors.As(err, &gs) {
		return nil, false
	}
	st := gs.GRPCStatus()
	return st, st != nil
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
