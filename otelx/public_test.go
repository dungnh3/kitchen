package otelx_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/dungnh3/kitchen/otelx"
)

// A public endpoint must not adopt the caller's trace: it starts a new trace
// and links to the caller's span context.
func TestPublicHTTPHandlerStartsNewTrace(t *testing.T) {
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	callerTrace, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	for _, tc := range []struct {
		name    string
		handler func(http.Handler, string, ...otelhttp.Option) http.Handler
		public  bool
	}{
		{"internal", otelx.HTTPHandler, false},
		{"public", otelx.PublicHTTPHandler, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := tracetest.NewSpanRecorder()
			prov := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			defer func() { _ = prov.Shutdown(t.Context()) }()
			h := tc.handler(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}), "op",
				otelhttp.WithTracerProvider(prov), otelhttp.WithPropagators(propagation.TraceContext{}))
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			req.Header.Set("traceparent", tp)
			h.ServeHTTP(httptest.NewRecorder(), req)

			spans := rec.Ended()
			if len(spans) != 1 {
				t.Fatalf("spans = %d", len(spans))
			}
			s := spans[0]
			sameTrace := s.SpanContext().TraceID() == callerTrace
			linked := len(s.Links()) == 1 && s.Links()[0].SpanContext.TraceID() == callerTrace
			if tc.public && (sameTrace || !linked) {
				t.Fatalf("public: sameTrace=%v linked=%v", sameTrace, linked)
			}
			if !tc.public && !sameTrace {
				t.Fatal("internal: caller trace not continued")
			}
		})
	}
}
