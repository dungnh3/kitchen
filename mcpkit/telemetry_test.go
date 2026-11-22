package mcpkit_test

import (
	"context"
	"testing"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/mcpkit"
	"github.com/dungnh3/kitchen/mcpkit/mcpkittest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func discardLogger() *log.Logger { return log.Discard() }

func TestTelemetry(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	s := newServer(t, mcpkit.WithTracerProvider(tp), mcpkit.WithMeterProvider(mp))
	mcpkit.AddTool(s, &mcp.Tool{Name: "greet"}, greet)
	cs := mcpkittest.Connect(t, s, "")
	call(t, cs, "greet", map[string]any{"name": "a"})
	call(t, cs, "greet", map[string]any{"name": "error"})

	var calls []sdktrace.ReadOnlySpan
	for _, sp := range spans.Ended() {
		if sp.Name() == "tools/call greet" {
			calls = append(calls, sp)
		}
	}
	if len(calls) != 2 {
		t.Fatalf("got %d tools/call spans, want 2", len(calls))
	}
	attrs := attribute.NewSet(calls[0].Attributes()...)
	if v, _ := attrs.Value("gen_ai.tool.name"); v.AsString() != "greet" {
		t.Errorf("gen_ai.tool.name = %q", v.AsString())
	}
	if v, _ := attrs.Value("mcp.method.name"); v.AsString() != "tools/call" {
		t.Errorf("mcp.method.name = %q", v.AsString())
	}
	if calls[1].Status().Code != codes.Error {
		t.Errorf("tool error span status = %v, want Error", calls[1].Status())
	}
	errAttrs := attribute.NewSet(calls[1].Attributes()...)
	if v, _ := errAttrs.Value("error.type"); v.AsString() != "tool_error" {
		t.Errorf("error.type = %q, want tool_error", v.AsString())
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "mcp.server.operation.duration" {
				found = true
			}
		}
	}
	if !found {
		t.Error("mcp.server.operation.duration not recorded")
	}
}
