package errs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
)

var errAgentNotFound = errs.New(codes.NotFound, "AGENT_NOT_FOUND",
	errs.Domain("agents.example.com"), errs.Message("agent not found"))

func TestIsAcrossWrapAndWith(t *testing.T) {
	cause := errors.New("sql: no rows")
	err := errs.Wrap(cause, errAgentNotFound, errs.Meta("agent_id", "a1"))
	wrapped := fmt.Errorf("handler: %w", err)

	tests := []struct {
		name   string
		target error
		want   bool
	}{
		{"same sentinel", errAgentNotFound, true},
		{"generic reason-only sentinel does not match other reason", errs.ErrNotFound, false},
		{"cause still reachable", cause, true},
		{"different domain", errs.New(codes.NotFound, "AGENT_NOT_FOUND", errs.Domain("other")), false},
		{"empty-domain sentinel same reason", errs.New(codes.NotFound, "AGENT_NOT_FOUND"), true},
		{"plain error", errors.New("x"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errors.Is(wrapped, tt.target); got != tt.want {
				t.Fatalf("errors.Is = %v, want %v", got, tt.want)
			}
		})
	}
	if got := errAgentNotFound.With(errs.Meta("k", "v")); !errors.Is(got, errAgentNotFound) {
		t.Fatal("With broke errors.Is")
	}
}

func TestImmutability(t *testing.T) {
	base := errs.New(codes.InvalidArgument, "BAD", errs.Meta("a", "1"))
	_ = base.With(errs.Meta("a", "2"), errs.Message("changed"))
	_ = errs.Wrap(errors.New("c"), base, errs.Meta("b", "3"))
	if got := base.Metadata(); len(got) != 1 || got["a"] != "1" {
		t.Fatalf("base metadata mutated: %v", got)
	}
	if base.Message() != "BAD" || base.Unwrap() != nil {
		t.Fatal("base mutated")
	}
	m := base.Metadata()
	m["a"] = "x"
	if base.Metadata()["a"] != "1" {
		t.Fatal("Metadata returned internal map")
	}
}

func TestGRPCStatusNeverLeaksCause(t *testing.T) {
	err := errs.Wrap(errors.New("password=hunter2"), errAgentNotFound, errs.Meta("agent_id", "a1"))
	st := status.Convert(err)
	if st.Code() != codes.NotFound || st.Message() != "agent not found" {
		t.Fatalf("status = %v %q", st.Code(), st.Message())
	}
	if strings.Contains(fmt.Sprint(st.Proto()), "hunter2") {
		t.Fatal("cause leaked into status")
	}
	info, ok := errs.Info(st.Err())
	if !ok || info.GetReason() != "AGENT_NOT_FOUND" || info.GetDomain() != "agents.example.com" || info.GetMetadata()["agent_id"] != "a1" {
		t.Fatalf("Info = %v %v", info, ok)
	}
	if !strings.Contains(err.Error(), "hunter2") {
		t.Fatal("Error() should include the cause for logs")
	}
}

func TestFromStatusRoundTrip(t *testing.T) {
	ri := &errdetails.RetryInfo{RetryDelay: durationpb.New(2 * time.Second)}
	sent := errAgentNotFound.With(errs.Meta("agent_id", "a1"), errs.Details(ri))
	got := errs.FromStatus(status.Convert(sent))
	if !errors.Is(got, errAgentNotFound) || got.Code() != codes.NotFound || got.Metadata()["agent_id"] != "a1" {
		t.Fatalf("round trip lost data: %v", got)
	}
	st := got.GRPCStatus()
	if len(st.Details()) != 2 {
		t.Fatalf("details = %v", st.Details())
	}
	if errs.FromStatus(status.New(codes.OK, "")) != nil || errs.FromStatus(nil) != nil {
		t.Fatal("OK/nil status should give nil")
	}
	plain := errs.FromStatus(status.New(codes.Unavailable, "down"))
	if !errors.Is(plain, errs.ErrUnavailable) {
		t.Fatal("status without ErrorInfo should match the generic sentinel")
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
		{fmt.Errorf("w: %w", errs.ErrPermissionDenied), codes.PermissionDenied, 403},
		{errs.ErrUnauthenticated, codes.Unauthenticated, 401},
		{errs.ErrResourceExhausted, codes.ResourceExhausted, 429},
		{errs.ErrAborted, codes.Aborted, 409},
		{errs.ErrFailedPrecondition, codes.FailedPrecondition, 400},
		{status.Error(codes.Unavailable, "x"), codes.Unavailable, 503},
		{context.Canceled, codes.Canceled, 499},
		{fmt.Errorf("w: %w", context.DeadlineExceeded), codes.DeadlineExceeded, 504},
		{errors.New("boom"), codes.Unknown, 500},
		{status.Error(codes.DataLoss, "x"), codes.DataLoss, 500},
	}
	for _, tt := range tests {
		if got := errs.Code(tt.err); got != tt.code {
			t.Errorf("Code(%v) = %v, want %v", tt.err, got, tt.code)
		}
		if got := errs.HTTPStatus(tt.err); got != tt.http {
			t.Errorf("HTTPStatus(%v) = %d, want %d", tt.err, got, tt.http)
		}
	}
}

func TestToStatus(t *testing.T) {
	st, sanitized := errs.ToStatus(errors.New("pq: connection refused at 10.0.0.1"))
	if !sanitized || st.Code() != codes.Internal || st.Message() != "internal error" {
		t.Fatalf("got %v %q %v", st.Code(), st.Message(), sanitized)
	}
	ri := &errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)}
	in, _ := status.New(codes.ResourceExhausted, "slow down").WithDetails(ri)
	st, sanitized = errs.ToStatus(fmt.Errorf("wrapped: %w", in.Err()))
	if sanitized || st.Code() != codes.ResourceExhausted || len(st.Details()) != 1 {
		t.Fatalf("status error not preserved: %v %v", st, sanitized)
	}
	if st, _ := errs.ToStatus(nil); st != nil {
		t.Fatal("nil should give nil")
	}
	if st, s := errs.ToStatus(context.Canceled); s || st.Code() != codes.Canceled {
		t.Fatal("context.Canceled")
	}
}

func TestWriteHTTP(t *testing.T) {
	ri := &errdetails.RetryInfo{RetryDelay: durationpb.New(1500 * time.Millisecond)}
	rec := httptest.NewRecorder()
	errs.WriteHTTP(rec, errs.New(codes.ResourceExhausted, "RATE_LIMITED", errs.Domain("d"), errs.Details(ri)))
	if rec.Code != 429 || rec.Header().Get("Retry-After") != "2" {
		t.Fatalf("code=%d retry-after=%q", rec.Code, rec.Header().Get("Retry-After"))
	}
	var body struct {
		Error struct {
			Code    int              `json:"code"`
			Message string           `json:"message"`
			Status  string           `json:"status"`
			Details []map[string]any `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err, rec.Body.String())
	}
	if body.Error.Code != 429 || body.Error.Status != "RESOURCE_EXHAUSTED" || len(body.Error.Details) != 2 {
		t.Fatalf("body = %+v", body)
	}
	if body.Error.Details[0]["@type"] != "type.googleapis.com/google.rpc.ErrorInfo" || body.Error.Details[0]["reason"] != "RATE_LIMITED" {
		t.Fatalf("detail = %v", body.Error.Details[0])
	}

	rec = httptest.NewRecorder()
	errs.WriteHTTP(rec, errs.ErrUnauthenticated)
	if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("401 headers: %d %v", rec.Code, rec.Header())
	}

	rec = httptest.NewRecorder()
	errs.WriteHTTP(rec, errors.New("secret internals"))
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("unknown error leaked: %s", rec.Body.String())
	}
}

func TestBadRequest(t *testing.T) {
	err := errs.BadRequest(errs.FieldViolation{Field: "name", Description: "required", Reason: "required"})
	if !errors.Is(err, errs.ErrInvalidArgument) || errs.Code(err) != codes.InvalidArgument {
		t.Fatal("BadRequest should be INVALID_ARGUMENT")
	}
	br := findBadRequest(t, err.GRPCStatus())
	if len(br.GetFieldViolations()) != 1 || br.GetFieldViolations()[0].GetField() != "name" {
		t.Fatalf("violations = %v", br)
	}
}

// violationsDescriptor mirrors the subset of buf.validate.Violations that
// FromViolationsProto reads, so the test does not depend on protovalidate.
func violationsDescriptor(t *testing.T) *dynamicpb.Message {
	t.Helper()
	opt := func(s string) *string { return &s }
	i32 := func(i int32) *int32 { return &i }
	rep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	optl := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	msg := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	u64 := descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum()
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    opt("test/validate.proto"),
		Package: opt("buf.validate"),
		Syntax:  opt("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: opt("Violations"), Field: []*descriptorpb.FieldDescriptorProto{
				{Name: opt("violations"), Number: i32(1), Label: rep, Type: msg, TypeName: opt(".buf.validate.Violation")},
			}},
			{Name: opt("Violation"), Field: []*descriptorpb.FieldDescriptorProto{
				{Name: opt("rule_id"), Number: i32(2), Label: optl, Type: str},
				{Name: opt("message"), Number: i32(3), Label: optl, Type: str},
				{Name: opt("field"), Number: i32(5), Label: optl, Type: msg, TypeName: opt(".buf.validate.FieldPath")},
			}},
			{Name: opt("FieldPath"), Field: []*descriptorpb.FieldDescriptorProto{
				{Name: opt("elements"), Number: i32(1), Label: rep, Type: msg, TypeName: opt(".buf.validate.FieldPathElement")},
			}},
			{
				Name: opt("FieldPathElement"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: opt("field_name"), Number: i32(2), Label: optl, Type: str},
					{Name: opt("index"), Number: i32(6), Label: optl, Type: u64, OneofIndex: i32(0)},
					{Name: opt("string_key"), Number: i32(10), Label: optl, Type: str, OneofIndex: i32(0)},
				},
				OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: opt("subscript")}},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatal(err)
	}
	return dynamicpb.NewMessage(fd.Messages().ByName("Violations"))
}

func TestFromViolationsProto(t *testing.T) {
	root := violationsDescriptor(t)
	md := root.Descriptor()
	vmd := md.Fields().ByName("violations").Message()
	pmd := vmd.Fields().ByName("field").Message()
	emd := pmd.Fields().ByName("elements").Message()

	elem := func(name string, set func(m *dynamicpb.Message)) *dynamicpb.Message {
		e := dynamicpb.NewMessage(emd)
		e.Set(emd.Fields().ByName("field_name"), protoValueString(name))
		if set != nil {
			set(e)
		}
		return e
	}
	path := dynamicpb.NewMessage(pmd)
	elems := path.Mutable(pmd.Fields().ByName("elements")).List()
	elems.Append(protoValueMsg(elem("items", func(m *dynamicpb.Message) {
		m.Set(emd.Fields().ByName("index"), protoValueUint(2))
	})))
	elems.Append(protoValueMsg(elem("labels", func(m *dynamicpb.Message) {
		m.Set(emd.Fields().ByName("string_key"), protoValueString("env"))
	})))
	elems.Append(protoValueMsg(elem("value", nil)))

	v := dynamicpb.NewMessage(vmd)
	v.Set(vmd.Fields().ByName("rule_id"), protoValueString("string.min_len"))
	v.Set(vmd.Fields().ByName("message"), protoValueString("too short"))
	v.Set(vmd.Fields().ByName("field"), protoValueMsg(path))
	root.Mutable(md.Fields().ByName("violations")).List().Append(protoValueMsg(v))

	err := errs.FromViolationsProto(root)
	br := findBadRequest(t, err.GRPCStatus())
	fv := br.GetFieldViolations()
	if len(fv) != 1 {
		t.Fatalf("violations = %v", fv)
	}
	if fv[0].GetField() != `items[2].labels["env"].value` || fv[0].GetDescription() != "too short" || fv[0].GetReason() != "string.min_len" {
		t.Fatalf("violation = %v", fv[0])
	}
	if br := findBadRequest(t, errs.FromViolationsProto(&errdetails.ErrorInfo{}).GRPCStatus()); len(br.GetFieldViolations()) != 0 {
		t.Fatal("unknown shape should give empty BadRequest")
	}
}

func findBadRequest(t *testing.T, st *status.Status) *errdetails.BadRequest {
	t.Helper()
	for _, d := range st.Details() {
		if br, ok := d.(*errdetails.BadRequest); ok {
			return br
		}
	}
	t.Fatalf("no BadRequest in %v", st.Details())
	return nil
}

type fakeLocalizer map[string]string

func (f fakeLocalizer) Localize(_ context.Context, reason string) (string, string, bool) {
	m, ok := f[reason]
	return "vi", m, ok
}

func TestUnaryServerInterceptor(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	ic := errs.UnaryServerInterceptor(errs.ServerConfig{
		Logger:    log,
		Localizer: fakeLocalizer{"AGENT_NOT_FOUND": "không tìm thấy agent", "INTERNAL": "lỗi hệ thống"},
	})
	info := &grpc.UnaryServerInfo{FullMethod: "/agent.v1.AgentService/Get"}
	call := func(err error) error {
		_, got := ic(context.Background(), nil, info, func(context.Context, any) (any, error) { return nil, err })
		return got
	}

	if call(nil) != nil {
		t.Fatal("nil error changed")
	}

	got := status.Convert(call(errs.Wrap(errors.New("db"), errAgentNotFound)))
	if got.Code() != codes.NotFound {
		t.Fatal(got)
	}
	var lm *errdetails.LocalizedMessage
	for _, d := range got.Details() {
		if m, ok := d.(*errdetails.LocalizedMessage); ok {
			lm = m
		}
	}
	if lm == nil || lm.GetLocale() != "vi" || lm.GetMessage() != "không tìm thấy agent" {
		t.Fatalf("localized = %v", got.Details())
	}
	if logs.Len() != 0 {
		t.Fatalf("client errors must not be logged at error level: %s", logs.String())
	}

	got = status.Convert(call(errors.New("pq: secret dsn")))
	if got.Code() != codes.Internal || strings.Contains(got.Message(), "secret") {
		t.Fatal(got)
	}
	if !strings.Contains(logs.String(), "pq: secret dsn") {
		t.Fatalf("sanitized cause should be logged: %s", logs.String())
	}

	// Existing status details survive and are not localized twice.
	pre, _ := status.New(codes.ResourceExhausted, "x").WithDetails(
		&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)},
		&errdetails.LocalizedMessage{Locale: "en", Message: "x"})
	got = status.Convert(call(pre.Err()))
	if len(got.Details()) != 2 {
		t.Fatalf("details changed: %v", got.Details())
	}
}

type fakeStream struct {
	grpc.ServerStream
}

func (fakeStream) Context() context.Context { return context.Background() }

func TestStreamServerInterceptor(t *testing.T) {
	ic := errs.StreamServerInterceptor(errs.ServerConfig{Logger: slog.New(slog.DiscardHandler)})
	err := ic(nil, fakeStream{}, &grpc.StreamServerInfo{FullMethod: "/s/M"}, func(any, grpc.ServerStream) error {
		return errors.New("boom")
	})
	if status.Code(err) != codes.Internal {
		t.Fatal(err)
	}
}

func protoValueString(s string) protoreflect.Value { return protoreflect.ValueOfString(s) }
func protoValueUint(u uint64) protoreflect.Value   { return protoreflect.ValueOfUint64(u) }
func protoValueMsg(m *dynamicpb.Message) protoreflect.Value {
	return protoreflect.ValueOfMessage(m)
}
