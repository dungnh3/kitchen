package otelx_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/goleak"
	"google.golang.org/grpc/stats"

	"github.com/dungnh3/kitchen/otelx"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// fakeSpan records what the helpers do to a span.
type fakeSpan struct {
	noop.Span
	mu     sync.Mutex
	name   string
	errs   []error
	code   codes.Code
	desc   string
	ended  bool
	sc     trace.SpanContext
	events []trace.EventOption
}

func (s *fakeSpan) SpanContext() trace.SpanContext { return s.sc }
func (s *fakeSpan) IsRecording() bool              { return true }
func (s *fakeSpan) End(...trace.SpanEndOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ended = true
}

func (s *fakeSpan) RecordError(err error, opts ...trace.EventOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, err)
	s.events = append(s.events, opts...)
}

func (s *fakeSpan) SetStatus(c codes.Code, d string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.code, s.desc = c, d
}

type fakeTP struct {
	embedded.TracerProvider
	mu    sync.Mutex
	spans []*fakeSpan
}

func (p *fakeTP) Tracer(string, ...trace.TracerOption) trace.Tracer { return fakeTracer{p: p} }

type fakeTracer struct {
	embedded.Tracer
	p *fakeTP
}

func (t fakeTracer) Start(ctx context.Context, name string, _ ...trace.SpanStartOption) (context.Context, trace.Span) {
	s := &fakeSpan{name: name, sc: trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
	})}
	t.p.mu.Lock()
	t.p.spans = append(t.p.spans, s)
	t.p.mu.Unlock()
	return trace.ContextWithSpan(ctx, s), s
}

func install(t *testing.T) *fakeTP {
	t.Helper()
	prev := otel.GetTracerProvider()
	tp := &fakeTP{}
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return tp
}

func TestRecordError(t *testing.T) {
	s := &fakeSpan{}
	otelx.RecordError(s, nil)
	if len(s.errs) != 0 || s.code != codes.Unset {
		t.Fatal("nil error must be a no-op")
	}
	otelx.RecordError(nil, errors.New("x")) // must not panic
	otelx.RecordError(s, errors.New("boom"))
	if len(s.errs) != 1 || s.code != codes.Error || s.desc != "boom" {
		t.Fatalf("got %+v", s)
	}
}

func TestEnd(t *testing.T) {
	s := &fakeSpan{}
	err := errors.New("e")
	otelx.End(s, &err)
	if !s.ended || s.code != codes.Error {
		t.Fatalf("got %+v", s)
	}
	s2 := &fakeSpan{}
	var nilErr error
	otelx.End(s2, &nilErr)
	if !s2.ended || s2.code != codes.Unset {
		t.Fatalf("got %+v", s2)
	}
	s3 := &fakeSpan{}
	otelx.End(s3, nil)
	if !s3.ended {
		t.Fatal("not ended")
	}
}

func TestWithSpan(t *testing.T) {
	tp := install(t)
	want := errors.New("fail")
	err := otelx.WithSpan(context.Background(), "op", func(ctx context.Context) error {
		if otelx.TraceID(ctx) == "" || otelx.SpanID(ctx) == "" {
			t.Error("span not in ctx")
		}
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
	s := tp.spans[0]
	if s.name != "op" || !s.ended || s.code != codes.Error {
		t.Fatalf("span = %+v", s)
	}
}

func TestWithSpanRepanics(t *testing.T) {
	tp := install(t)
	defer func() {
		if r := recover(); r != "kaboom" {
			t.Fatalf("recover = %v", r)
		}
		s := tp.spans[0]
		if !s.ended || s.code != codes.Error || len(s.errs) != 1 {
			t.Fatalf("span = %+v", s)
		}
	}()
	_ = otelx.WithSpan(context.Background(), "op", func(context.Context) error { panic("kaboom") })
}

func TestIDsEmptyWithoutSpan(t *testing.T) {
	if otelx.TraceID(context.Background()) != "" || otelx.SpanID(context.Background()) != "" {
		t.Fatal("expected empty ids")
	}
}

func TestInjectExtract(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{0xa}, SpanID: trace.SpanID{0xb}, TraceFlags: trace.FlagsSampled,
	})
	carrier := propagation.MapCarrier{}
	otelx.Inject(trace.ContextWithSpanContext(context.Background(), sc), carrier)
	if carrier["traceparent"] == "" {
		t.Fatal("traceparent not injected")
	}
	got := trace.SpanContextFromContext(otelx.Extract(context.Background(), carrier))
	if got.TraceID() != sc.TraceID() || !got.IsRemote() {
		t.Fatalf("extracted %v", got)
	}
}

func TestDefaultGRPCFilter(t *testing.T) {
	tests := map[string]bool{
		"/grpc.health.v1.Health/Check":                              false,
		"/grpc.health.v1.Health/Watch":                              false,
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo": false,
		"/orders.v1.Orders/Create":                                  true,
	}
	for m, want := range tests {
		if got := otelx.DefaultGRPCFilter(&stats.RPCTagInfo{FullMethodName: m}); got != want {
			t.Errorf("%s: %v", m, got)
		}
	}
	if otelx.GRPCServerHandler() == nil || otelx.GRPCClientHandler() == nil {
		t.Fatal("nil handler")
	}
}

func TestHTTPInstrumentationPropagates(t *testing.T) {
	install(t)
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	var gotTraceparent string
	srv := httptest.NewServer(otelx.HTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTraceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNoContent)
	}), "test"))
	defer srv.Close()

	client := &http.Client{Transport: otelx.HTTPTransport(nil)}
	ctx, span := otelx.Start(context.Background(), "client")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	span.End()
	client.CloseIdleConnections()
	if gotTraceparent == "" {
		t.Fatal("traceparent not propagated")
	}
}
