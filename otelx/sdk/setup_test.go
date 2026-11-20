package sdk_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/otelx/sdk"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// clearEnv makes tests independent of the developer's OTEL_* environment.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, n := range []string{
		"OTEL_SDK_DISABLED", "OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES",
		"OTEL_TRACES_EXPORTER", "OTEL_METRICS_EXPORTER", "OTEL_LOGS_EXPORTER",
		"OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG",
		"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
		"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
	} {
		t.Setenv(n, "")
		// t.Setenv("") still counts as set; unset explicitly for LookupEnv.
		unsetEnv(t, n)
	}
}

func newMem() *tracetest.InMemoryExporter { return tracetest.NewInMemoryExporter() }

func TestSetupInjectedExportersAndResource(t *testing.T) {
	clearEnv(t)
	ctx := context.Background()
	mem := newMem()
	reader := sdkmetric.NewManualReader()
	logs := &memProcessor{}
	tel, err := sdk.Setup(ctx, sdk.Config{
		ServiceName:    "svc",
		ServiceVersion: "1.2.3",
		Environment:    "test",
		ResourceAttrs:  []attribute.KeyValue{attribute.String("vendor.app", "x")},
		SpanExporters:  []sdktrace.SpanExporter{mem},
		MetricReaders:  []sdkmetric.Reader{reader},
		LogProcessors:  []sdklog.Processor{logs},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, tel)

	// Globals are installed.
	if otel.GetTracerProvider() != tel.TracerProvider() {
		t.Fatal("global tracer provider not installed")
	}
	_, span := otel.Tracer("t").Start(ctx, "op")
	span.End()
	c, _ := otel.Meter("t").Int64Counter("hits")
	c.Add(ctx, 2)
	slog.New(tel.SlogHandler("t")).InfoContext(ctx, "hello")

	if err := tel.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	spans := mem.GetSpans()
	if len(spans) != 1 || spans[0].Name != "op" {
		t.Fatalf("spans = %v", spans)
	}
	res := spans[0].Resource
	for k, want := range map[string]string{
		"service.name": "svc", "service.version": "1.2.3",
		"deployment.environment.name": "test", "vendor.app": "x",
	} {
		v, ok := res.Set().Value(attribute.Key(k))
		if !ok || v.AsString() != want {
			t.Errorf("resource %s = %v", k, v)
		}
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil || len(rm.ScopeMetrics) != 1 {
		t.Fatalf("metrics = %+v, %v", rm, err)
	}
	if logs.count() != 1 {
		t.Fatalf("log records = %d", logs.count())
	}
}

func TestShutdownRestoresGlobalsAndIsIdempotent(t *testing.T) {
	clearEnv(t)
	prevProp := otel.GetTextMapPropagator()
	prevLP := global.GetLoggerProvider()
	tel, err := sdk.Setup(context.Background(), sdk.Config{SpanExporters: []sdktrace.SpanExporter{newMem()}})
	if err != nil {
		t.Fatal(err)
	}
	if otel.GetTextMapPropagator() == prevProp {
		t.Fatal("propagator not installed")
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if otel.GetTextMapPropagator() != prevProp || global.GetLoggerProvider() != prevLP {
		t.Fatal("globals not restored")
	}
}

func TestNoGlobal(t *testing.T) {
	clearEnv(t)
	prev := otel.GetTracerProvider()
	tel, err := sdk.Setup(context.Background(), sdk.Config{NoGlobal: true, SpanExporters: []sdktrace.SpanExporter{newMem()}})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, tel)
	if otel.GetTracerProvider() != prev {
		t.Fatal("global changed with NoGlobal")
	}
}

func TestDisabled(t *testing.T) {
	clearEnv(t)
	tel, err := sdk.Setup(context.Background(), sdk.Config{Disabled: true, Traces: sdk.SignalConfig{Exporter: "bogus"}})
	if err != nil {
		t.Fatal(err)
	}
	_, span := tel.TracerProvider().Tracer("t").Start(context.Background(), "x")
	if span.SpanContext().IsValid() {
		t.Fatal("expected no-op span")
	}
	shutdown(t, tel)

	// Env wins over code.
	t.Setenv("OTEL_SDK_DISABLED", "true")
	tel, err = sdk.Setup(context.Background(), sdk.Config{})
	if err != nil {
		t.Fatal(err)
	}
	shutdown(t, tel)

	t.Setenv("OTEL_SDK_DISABLED", "maybe")
	if _, err := sdk.Setup(context.Background(), sdk.Config{}); err == nil {
		t.Fatal("expected error for invalid OTEL_SDK_DISABLED")
	}
}

func TestInvalidConfigErrors(t *testing.T) {
	clearEnv(t)
	bad := 1.5
	tests := map[string]sdk.Config{
		"exporter":  {Traces: sdk.SignalConfig{Exporter: "zipkin"}},
		"protocol":  {Traces: sdk.SignalConfig{Protocol: "http/json"}},
		"ratio":     {SampleRatio: &bad},
		"metrics":   {Metrics: sdk.SignalConfig{Exporter: "prometheus"}},
		"logs":      {Logs: sdk.SignalConfig{Exporter: "stdout"}},
		"logsproto": {Logs: sdk.SignalConfig{Exporter: sdk.ExporterOTLP, Protocol: "udp"}},
	}
	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := sdk.Setup(context.Background(), cfg); err == nil || !strings.HasPrefix(err.Error(), "otelx/sdk: ") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestEnvSamplerOverridesCode(t *testing.T) {
	clearEnv(t)
	zero := 0.0
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "1")
	mem := newMem()
	tel, err := sdk.Setup(context.Background(), sdk.Config{NoGlobal: true, SampleRatio: &zero, SpanExporters: []sdktrace.SpanExporter{mem}})
	if err != nil {
		t.Fatal(err)
	}
	_, span := tel.TracerProvider().Tracer("t").Start(context.Background(), "x")
	span.End()
	flush(t, tel)
	if len(mem.GetSpans()) != 1 {
		t.Fatal("env sampler arg did not override SampleRatio=0")
	}

	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")
	mem = newMem()
	tel, err = sdk.Setup(context.Background(), sdk.Config{NoGlobal: true, SpanExporters: []sdktrace.SpanExporter{mem}})
	if err != nil {
		t.Fatal(err)
	}
	_, span = tel.TracerProvider().Tracer("t").Start(context.Background(), "x")
	span.End()
	flush(t, tel)
	if len(mem.GetSpans()) != 0 {
		t.Fatal("always_off ignored")
	}

	t.Setenv("OTEL_TRACES_SAMPLER", "jaeger_remote")
	if _, err := sdk.Setup(context.Background(), sdk.Config{NoGlobal: true}); err == nil {
		t.Fatal("expected unsupported sampler error")
	}
}

func TestOTLPHTTPExportAndEnvEndpointWins(t *testing.T) {
	clearEnv(t)
	var codeHits, envHits atomic.Int32
	var gotAuth atomic.Value
	codeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/traces" {
			codeHits.Add(1)
			gotAuth.Store(r.Header.Get("Authorization"))
		}
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	defer codeSrv.Close()
	envSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		envHits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	defer envSrv.Close()

	run := func() {
		tel, err := sdk.Setup(context.Background(), sdk.Config{
			NoGlobal: true,
			Traces: sdk.SignalConfig{
				Protocol: sdk.ProtocolHTTPProtobuf,
				Endpoint: codeSrv.URL + "/v1/traces",
				Headers:  map[string]string{"Authorization": "Bearer k"},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		_, span := tel.TracerProvider().Tracer("t").Start(context.Background(), "op")
		span.End()
		shutdown(t, tel)
	}

	run()
	if codeHits.Load() != 1 || gotAuth.Load() != "Bearer k" {
		t.Fatalf("code endpoint hits=%d auth=%v", codeHits.Load(), gotAuth.Load())
	}

	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", envSrv.URL+"/v1/traces")
	run()
	if envHits.Load() != 1 || codeHits.Load() != 1 {
		t.Fatalf("env endpoint did not win: env=%d code=%d", envHits.Load(), codeHits.Load())
	}
}

func TestOTLPGRPCExporterIsLazy(t *testing.T) {
	clearEnv(t)
	// Default config: OTLP/gRPC to localhost:4317. Construction must not
	// dial or fail without a collector.
	tel, err := sdk.Setup(context.Background(), sdk.Config{NoGlobal: true, Traces: sdk.SignalConfig{Insecure: true}})
	if err != nil {
		t.Fatal(err)
	}
	shutdown(t, tel) // nothing buffered, so no export attempt
}

func TestSamplerFilter(t *testing.T) {
	clearEnv(t)
	mem := newMem()
	tel, err := sdk.Setup(context.Background(), sdk.Config{
		NoGlobal:          true,
		ExcludeOperations: []string{"/healthz"},
		SpanExporters:     []sdktrace.SpanExporter{mem},
	})
	if err != nil {
		t.Fatal(err)
	}
	tr := tel.TracerProvider().Tracer("t")
	for _, name := range []string{
		"google.pubsub.v1.Subscriber/StreamingPull", "grpc.health.v1.Health/Check", "GET /healthz", "orders.v1.Orders/Create",
	} {
		_, s := tr.Start(context.Background(), name)
		s.End()
	}
	_, s := tr.Start(context.Background(), "client", trace.WithAttributes(attribute.String("rpc.method", "grpc.health.v1.Health/Watch")))
	s.End()
	flush(t, tel)
	if got := mem.GetSpans(); len(got) != 1 || got[0].Name != "orders.v1.Orders/Create" {
		t.Fatalf("spans = %v", got)
	}
}

func TestConcurrentShutdown(t *testing.T) {
	clearEnv(t)
	tel, err := sdk.Setup(context.Background(), sdk.Config{NoGlobal: true, SpanExporters: []sdktrace.SpanExporter{newMem()}})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _ = tel.Shutdown(context.Background()) })
	}
	wg.Wait()
}

// flush exports buffered spans and shuts down; the in-memory exporter is
// read after ForceFlush because its Shutdown resets it.
func flush(t *testing.T, tel *sdk.Telemetry) {
	t.Helper()
	if err := tel.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
}

func shutdown(t *testing.T, tel *sdk.Telemetry) {
	t.Helper()
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type memProcessor struct {
	mu sync.Mutex
	n  int
}

func (p *memProcessor) OnEmit(context.Context, *sdklog.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n++
	return nil
}
func (p *memProcessor) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (p *memProcessor) Shutdown(context.Context) error                         { return nil }
func (p *memProcessor) ForceFlush(context.Context) error                       { return nil }
func (p *memProcessor) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}
