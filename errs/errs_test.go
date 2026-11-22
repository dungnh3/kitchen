package errs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/log"

	"github.com/dungnh3/kitchen/errs"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

var errAgentNotFound = errs.New(codes.NotFound, "AGENT_NOT_FOUND",
	errs.Domain("agents.example.com"), errs.Message("agent not found"))

func TestIs(t *testing.T) {
	cause := errors.New("sql: no rows")
	err := fmt.Errorf("handler: %w", errs.Wrap(cause, errAgentNotFound, errs.Meta("id", "a1")))
	other := errs.New(codes.NotFound, "AGENT_NOT_FOUND", errs.Domain("other.example.com"))
	anyDomain := errs.New(codes.NotFound, "AGENT_NOT_FOUND")

	tests := []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{"same sentinel after wrap", err, errAgentNotFound, true},
		{"cause chain", err, cause, true},
		{"different domain", err, other, false},
		{"empty-domain target", err, anyDomain, true},
		{"code sentinel matches by code", err, errs.ErrNotFound, true},
		{"code sentinel other code", err, errs.ErrInternal, false},
		{"catalog reason, other code sentinel", errs.ErrTokenExpired, errs.ErrUnauthenticated, true},
		{"catalog reason vs other reason", errs.ErrTokenExpired, errs.ErrTokenInvalid, false},
		{"copy of a code sentinel still matches by code", errAgentNotFound, errs.ErrNotFound.With(errs.Meta("k", "v")), true},
		{"code sentinel with a domain matches by reason", errAgentNotFound, errs.ErrNotFound.With(errs.Domain("agents.example.com")), false},
		{"user-defined generic error matches by code", errAgentNotFound, errs.New(codes.NotFound, "NOT_FOUND"), true},
	}
	for _, tt := range tests {
		if got := errors.Is(tt.err, tt.target); got != tt.want {
			t.Errorf("%s: errors.Is = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestImmutability(t *testing.T) {
	a := errAgentNotFound.With(errs.Meta("k", "1"))
	b := a.With(errs.Meta("k", "2"), errs.Details(&errdetails.Help{}))
	if a.Metadata()["k"] != "1" || len(a.Details()) != 0 || len(errAgentNotFound.Metadata()) != 0 {
		t.Fatal("With mutated its receiver")
	}
	m := b.Metadata()
	m["k"] = "x"
	if b.Metadata()["k"] != "2" {
		t.Fatal("Metadata returned the internal map")
	}
}

func TestGRPCStatusNeverLeaksCause(t *testing.T) {
	err := errs.Wrap(errors.New("dsn=postgres://secret"), errAgentNotFound, errs.Meta("id", "a1"))
	st := status.Convert(err)
	if strings.Contains(fmt.Sprint(st.Proto()), "secret") {
		t.Fatalf("cause leaked: %v", st.Proto())
	}
	if !strings.Contains(err.Error(), "secret") {
		t.Fatal("Error() should keep the cause for logs")
	}
	info, ok := errs.Info(err)
	if !ok || info.GetReason() != "AGENT_NOT_FOUND" || info.GetMetadata()["id"] != "a1" {
		t.Fatalf("info = %v", info)
	}
}

func TestFromStatusAndFromError(t *testing.T) {
	sent := errAgentNotFound.With(errs.Meta("id", "a1"), errs.RetryAfter(time.Second))
	received := status.Convert(sent).Err() // what a gRPC client gets
	e := errs.FromError(fmt.Errorf("call: %w", received))
	if !errors.Is(e, errAgentNotFound) || e.Metadata()["id"] != "a1" || len(e.Details()) != 1 {
		t.Fatalf("round trip = %+v", e)
	}
	bare := errs.FromStatus(status.New(codes.Unavailable, "down"))
	if bare.Reason() != "UNAVAILABLE" || !errors.Is(bare, errs.ErrUnavailable) {
		t.Fatalf("bare = %+v", bare)
	}
	if errs.FromStatus(nil) != nil || errs.FromError(nil) != nil || errs.FromError(errors.New("x")) != nil {
		t.Fatal("nil cases")
	}
	if errs.FromError(sent) != sent {
		t.Fatal("FromError should return the *Error in the chain")
	}
}

func TestCodeAndHTTPStatus(t *testing.T) {
	tests := []struct {
		err  error
		code codes.Code
		http int
	}{
		{nil, codes.OK, 200},
		{errAgentNotFound, codes.NotFound, 404},
		{fmt.Errorf("w: %w", errs.ErrForbidden), codes.PermissionDenied, 403},
		{errs.ErrTokenExpired, codes.Unauthenticated, 401},
		{errs.ErrRateLimited, codes.ResourceExhausted, 429},
		{errs.ErrVersionMismatch, codes.Aborted, 409},
		{errs.ErrInvalidState, codes.FailedPrecondition, 400},
		{errs.ErrServiceDraining, codes.Unavailable, 503},
		{errs.ErrUpstreamTimeout, codes.DeadlineExceeded, 504},
		{status.Error(codes.Unavailable, "x"), codes.Unavailable, 503},
		{context.Canceled, codes.Canceled, 499},
		{fmt.Errorf("w: %w", context.DeadlineExceeded), codes.DeadlineExceeded, 504},
		{errors.New("boom"), codes.Unknown, 500},
		{status.Error(codes.DataLoss, "x"), codes.DataLoss, 500},
		// The two reason exceptions to the code mapping.
		{errs.ErrPayloadTooLarge, codes.InvalidArgument, 413},
		{errs.ErrUnsupportedMediaType.With(errs.Domain("svc")), codes.InvalidArgument, 415},
		{errs.ErrMalformedRequest, codes.InvalidArgument, 400},
		{errs.New(codes.FailedPrecondition, "PAYLOAD_TOO_LARGE"), codes.FailedPrecondition, 400},
	}
	for _, tt := range tests {
		if got := errs.Code(tt.err); got != tt.code {
			t.Errorf("Code(%v) = %v, want %v", tt.err, got, tt.code)
		}
		if got := errs.HTTPStatus(tt.err); got != tt.http {
			t.Errorf("HTTPStatus(%v) = %d, want %d", tt.err, got, tt.http)
		}
	}
	if errs.CodeName(codes.Canceled) != "CANCELLED" || errs.CodeName(codes.Code(99)) != "UNKNOWN" {
		t.Error("CodeName must follow google.rpc.Code")
	}
}

func TestCatalog(t *testing.T) {
	seen := map[string]bool{}
	for _, e := range errs.Catalog() {
		if seen[e.Reason()] {
			t.Errorf("duplicate reason %s", e.Reason())
		}
		seen[e.Reason()] = true
		if e.Domain() != "" || e.Code() == codes.OK || e.Message() == e.Reason() {
			t.Errorf("%s: domain=%q code=%v message=%q", e.Reason(), e.Domain(), e.Code(), e.Message())
		}
	}
}

func TestToStatus(t *testing.T) {
	st, sanitized := errs.ToStatus(errors.New("pq: connection refused at 10.0.0.1"))
	if !sanitized || st.Code() != codes.Internal || st.Message() != "internal error" {
		t.Fatalf("got %v %q %v", st.Code(), st.Message(), sanitized)
	}

	// A wrapped status keeps its own message, not the wrapper's text.
	in, _ := status.New(codes.ResourceExhausted, "slow down").WithDetails(
		&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)})
	st, sanitized = errs.ToStatus(fmt.Errorf("query user 42 on shard 3: %w", in.Err()))
	if sanitized || st.Code() != codes.ResourceExhausted || st.Message() != "slow down" || len(st.Details()) != 1 {
		t.Fatalf("wrapped status: %v %q %v", st.Code(), st.Message(), sanitized)
	}

	// A bare Unauthenticated/PermissionDenied (failed outgoing credential)
	// is not forwarded as the client's 401/403.
	for _, c := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied} {
		st, sanitized = errs.ToStatus(fmt.Errorf("call svc-b: %w", status.Error(c, "token for svc-b expired")))
		if !sanitized || st.Code() != codes.Internal {
			t.Errorf("bare %v: got %v sanitized=%v", c, st.Code(), sanitized)
		}
	}
	// ...even with an ErrorInfo: a downstream service's TOKEN_EXPIRED is
	// about this service's credentials (owner decision 2026-11-21).
	st, sanitized = errs.ToStatus(status.Convert(errs.ErrTokenExpired).Err())
	if !sanitized || st.Code() != codes.Internal {
		t.Errorf("downstream structured 401: %v %v", st.Code(), sanitized)
	}
	// A local *Error is trusted.
	if st, s := errs.ToStatus(errs.ErrTokenExpired); s || st.Code() != codes.Unauthenticated {
		t.Errorf("local 401: %v %v", st.Code(), s)
	}

	// Downstream server faults keep their code, never their text; only
	// RetryInfo survives.
	leak, _ := status.New(codes.Unavailable, `dial tcp 10.0.3.7:5432: connect: connection refused`).WithDetails(
		&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)},
		&errdetails.DebugInfo{Detail: "stack"})
	st, sanitized = errs.ToStatus(fmt.Errorf("call billing: %w", leak.Err()))
	if !sanitized || st.Code() != codes.Unavailable || st.Message() != "service unavailable" || len(st.Details()) != 1 {
		t.Errorf("downstream 503: %v %q %v %v", st.Code(), st.Message(), st.Details(), sanitized)
	}
	st, _ = errs.ToStatus(status.Error(codes.Unknown, `pq: password authentication failed for user "svc"`))
	if st.Code() != codes.Unknown || strings.Contains(st.Message(), "pq") {
		t.Errorf("downstream Unknown leaked: %q", st.Message())
	}
	// Downstream client errors keep their details.
	nf := status.Convert(errs.ErrNotFound.With(errs.Message("agent 7 not found"))).Err()
	if st, s := errs.ToStatus(nf); s || st.Message() != "agent 7 not found" {
		t.Errorf("downstream 404: %q %v", st.Message(), s)
	}

	// A typed-nil *Error and a code OK never pass as success or panic.
	var nilErr *errs.Error
	if st, s := errs.ToStatus(error(nilErr)); !s || st.Code() != codes.Internal || errs.Code(nilErr) != codes.Internal {
		t.Errorf("nil *Error: %v %v", st, s)
	}
	if errors.Is(errs.ErrNotFound, nilErr) || //nolint:staticcheck // a nil *Error as target is the case under test
		errors.Is(nilErr, errs.ErrNotFound) || nilErr.Error() == "" {
		t.Error("nil *Error Is/Error")
	}
	if st, s := errs.ToStatus(errs.New(codes.OK, "WEIRD")); !s || st.Code() != codes.Internal {
		t.Errorf("code OK: %v %v", st.Code(), s)
	}

	if st, _ := errs.ToStatus(nil); st != nil {
		t.Fatal("nil should give nil")
	}
	if st, s := errs.ToStatus(context.Canceled); s || st.Code() != codes.Canceled {
		t.Fatal("context.Canceled")
	}
}

type fakeLocalizer map[string]string

func (f fakeLocalizer) Localize(_ context.Context, info *errdetails.ErrorInfo) (string, string, bool) {
	m, ok := f[info.GetReason()]
	return "vi", strings.ReplaceAll(m, "{id}", info.GetMetadata()["id"]), ok
}

func details(st *status.Status) (info *errdetails.ErrorInfo, lm []*errdetails.LocalizedMessage) {
	for _, d := range st.Details() {
		switch m := d.(type) {
		case *errdetails.ErrorInfo:
			info = m
		case *errdetails.LocalizedMessage:
			lm = append(lm, m)
		}
	}
	return info, lm
}

func TestHandlerStatus(t *testing.T) {
	var logs bytes.Buffer
	h := errs.NewHandler(errs.Config{
		Logger:    log.New(&logs, log.Options{}),
		Localizer: fakeLocalizer{"AGENT_NOT_FOUND": "không tìm thấy {id}", "NOT_FOUND": "không tìm thấy", "INTERNAL": "lỗi hệ thống"},
		Domain:    "svc.example.com",
	})
	ctx := context.Background()

	// Own domain kept, metadata used by the localizer.
	info, lm := details(h.Status(ctx, errs.Wrap(errors.New("db"), errAgentNotFound, errs.Meta("id", "a1"))))
	if info.GetDomain() != "agents.example.com" || len(lm) != 1 || lm[0].GetMessage() != "không tìm thấy a1" {
		t.Fatalf("info=%v lm=%v", info, lm)
	}
	if logs.Len() != 0 {
		t.Fatalf("client errors must not be logged: %s", logs.String())
	}

	// Empty domain is filled in.
	if info, _ := details(h.Status(ctx, errs.ErrNotFound)); info.GetDomain() != "svc.example.com" || info.GetReason() != "NOT_FOUND" {
		t.Fatalf("default domain: %v", info)
	}

	// A bare status gets an ErrorInfo.
	st := h.Status(ctx, status.Error(codes.NotFound, "nope"))
	if info, lm := details(st); info.GetReason() != "NOT_FOUND" || info.GetDomain() != "svc.example.com" || len(lm) != 1 || st.Message() != "nope" {
		t.Fatalf("bare status: %v", st.Proto())
	}

	// Sanitized errors are logged with their cause.
	st = h.Status(ctx, errors.New("pq: secret dsn"))
	if st.Code() != codes.Internal || strings.Contains(fmt.Sprint(st.Proto()), "secret") {
		t.Fatal(st.Proto())
	}
	if !strings.Contains(logs.String(), "pq: secret dsn") {
		t.Fatalf("sanitized cause should be logged: %s", logs.String())
	}

	// Existing details survive and are not localized twice.
	pre, _ := status.New(codes.ResourceExhausted, "x").WithDetails(
		&errdetails.ErrorInfo{Reason: "RATE_LIMITED", Domain: "rl"},
		&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)},
		&errdetails.LocalizedMessage{Locale: "en", Message: "x"})
	if got := h.Status(ctx, pre.Err()); len(got.Details()) != 3 {
		t.Fatalf("details changed: %v", got.Details())
	}
	if h.Status(ctx, nil) != nil {
		t.Fatal("nil")
	}
}

func TestInterceptors(t *testing.T) {
	h := errs.NewHandler(errs.Config{Logger: log.Discard(), Domain: "d"})
	unary := h.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/agent.v1.AgentService/Get"}
	call := func(err error) error {
		_, got := unary(context.Background(), nil, info, func(context.Context, any) (any, error) { return nil, err })
		return got
	}
	if call(nil) != nil {
		t.Fatal("nil error changed")
	}
	if st := status.Convert(call(errAgentNotFound)); st.Code() != codes.NotFound {
		t.Fatal(st)
	}
	stream := h.StreamServerInterceptor()
	err := stream(nil, fakeStream{}, &grpc.StreamServerInfo{FullMethod: "/s/M"}, func(any, grpc.ServerStream) error {
		return errors.New("boom")
	})
	if status.Code(err) != codes.Internal {
		t.Fatal(err)
	}
}

type fakeStream struct{ grpc.ServerStream }

func (fakeStream) Context() context.Context { return context.Background() }

type envelope struct {
	Error struct {
		Code    int              `json:"code"`
		Message string           `json:"message"`
		Status  string           `json:"status"`
		Details []map[string]any `json:"details"`
	} `json:"error"`
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) envelope {
	t.Helper()
	var env envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err, rec.Body.String())
	}
	return env
}

func TestWriteHTTP(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/v1/x", nil)
	h := errs.NewHandler(errs.Config{Logger: log.Discard(), Domain: "svc"})

	rec := httptest.NewRecorder()
	h.WriteHTTP(rec, r, errs.ErrRateLimited.With(errs.RetryAfter(1500*time.Millisecond)))
	env := decode(t, rec)
	if rec.Code != 429 || rec.Header().Get("Retry-After") != "2" || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("code=%d headers=%v", rec.Code, rec.Header())
	}
	if env.Error.Code != 429 || env.Error.Status != "RESOURCE_EXHAUSTED" || len(env.Error.Details) != 2 {
		t.Fatalf("body = %+v", env)
	}
	if d := env.Error.Details[0]; d["@type"] != "type.googleapis.com/google.rpc.ErrorInfo" || d["reason"] != "RATE_LIMITED" || d["domain"] != "svc" {
		t.Fatalf("detail = %v", d)
	}

	rec = httptest.NewRecorder()
	errs.WriteHTTP(rec, r, errs.ErrTokenMissing)
	if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("401 headers: %d %v", rec.Code, rec.Header())
	}

	rec = httptest.NewRecorder()
	h.WriteHTTP(rec, r, errors.New("secret internals"))
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("unknown error leaked: %s", rec.Body.String())
	}
}

func TestGatewayErrorHandler(t *testing.T) {
	h := errs.NewHandler(errs.Config{Logger: log.Discard(), Domain: "svc"})
	r := httptest.NewRequest(http.MethodPost, "/v1/x", nil)
	serve := func(err error) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.GatewayErrorHandler(r.Context(), nil, nil, rec, r, err)
		return rec
	}

	// Status from the in-process call: kept as is (already converted).
	rec := serve(status.Convert(errs.ErrTokenExpired).Err())
	if env := decode(t, rec); rec.Code != 401 || env.Error.Details[0]["reason"] != "TOKEN_EXPIRED" {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}

	// HTTP status chosen by the gateway (405) is kept.
	rec = serve(&runtime.HTTPStatusError{HTTPStatus: http.StatusMethodNotAllowed, Err: status.Error(codes.Unimplemented, "Method Not Allowed")})
	if env := decode(t, rec); rec.Code != 405 || env.Error.Code != 405 || env.Error.Details[0]["domain"] != "svc" {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}

	// HTTPStatusError without a cause keeps the gateway's status.
	rec = serve(&runtime.HTTPStatusError{HTTPStatus: http.StatusMethodNotAllowed})
	if rec.Code != 405 {
		t.Fatalf("nil Err: %d %s", rec.Code, rec.Body.String())
	}

	// A raw status from a non-kitchen backend (no ErrorInfo) is sanitized.
	rec = serve(status.Error(codes.Internal, "panic: runtime error at db.go:42"))
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "db.go") {
		t.Fatalf("raw backend status leaked: %s", rec.Body.String())
	}

	// A plain error is sanitized.
	rec = serve(errors.New("secret"))
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

func TestBadRequest(t *testing.T) {
	err := errs.BadRequest(errs.FieldViolation{Field: "agent.name", Description: "required", Reason: "required"})
	if !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatal("BadRequest must be INVALID_ARGUMENT")
	}
	var br *errdetails.BadRequest
	for _, d := range status.Convert(err).Details() {
		if m, ok := d.(*errdetails.BadRequest); ok {
			br = m
		}
	}
	if br == nil || br.GetFieldViolations()[0].GetField() != "agent.name" {
		t.Fatalf("details = %v", status.Convert(err).Details())
	}
}

func TestHTTPStatusByReason(t *testing.T) {
	h := errs.NewHandler(errs.Config{Logger: log.Discard(), Domain: "svc"})
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/x", nil)
	for _, tt := range []struct {
		err  *errs.Error
		want int
	}{{errs.ErrPayloadTooLarge, 413}, {errs.ErrUnsupportedMediaType, 415}} {
		rec := httptest.NewRecorder()
		h.WriteHTTP(rec, r, tt.err)
		env := decode(t, rec)
		if rec.Code != tt.want || env.Error.Code != tt.want || env.Error.Status != "INVALID_ARGUMENT" {
			t.Fatalf("WriteHTTP %s: %d %s", tt.err.Reason(), rec.Code, rec.Body.String())
		}
		// Through the gateway (status from the in-process gRPC call).
		rec = httptest.NewRecorder()
		h.GatewayErrorHandler(r.Context(), nil, nil, rec, r, status.Convert(tt.err).Err())
		if rec.Code != tt.want {
			t.Fatalf("gateway %s: %d", tt.err.Reason(), rec.Code)
		}
		if _, code := errs.MarshalHTTP(tt.err.GRPCStatus()); code != tt.want {
			t.Fatalf("MarshalHTTP %s: %d", tt.err.Reason(), code)
		}
		if errs.HTTPStatusFromStatus(tt.err.GRPCStatus()) != tt.want {
			t.Fatalf("HTTPStatusFromStatus %s", tt.err.Reason())
		}
	}
	if errs.HTTPStatusFromStatus(nil) != 500 {
		t.Fatal("nil status must be 500")
	}
}

func TestMarshalHTTPNil(t *testing.T) {
	body, code := errs.MarshalHTTP(nil)
	if code != 500 || !strings.Contains(string(body), `"status":"INTERNAL"`) {
		t.Fatalf("%d %s", code, body)
	}
}
