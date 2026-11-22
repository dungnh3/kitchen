package sdk

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestIgnoreRemoteSampled(t *testing.T) {
	zero := 0.0
	smp, err := sampler(Config{SampleRatio: &zero, IgnoreRemoteSampled: true})
	if err != nil {
		t.Fatal(err)
	}
	remote := trace.ContextWithRemoteSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}, TraceFlags: trace.FlagsSampled, Remote: true,
	}))
	p := sdktrace.SamplingParameters{ParentContext: remote, TraceID: trace.TraceID{1}, Name: "GET /"}
	if d := smp.ShouldSample(p).Decision; d != sdktrace.Drop {
		t.Fatalf("remote sampled parent with ratio 0: decision %v, want Drop", d)
	}
	trusting, _ := sampler(Config{SampleRatio: &zero})
	if d := trusting.ShouldSample(p).Decision; d != sdktrace.RecordAndSample {
		t.Fatalf("default sampler: decision %v, want RecordAndSample", d)
	}
}

func TestConsoleExporterAccepted(t *testing.T) {
	if e, err := resolveExporter("TRACES", SignalConfig{Exporter: ExporterConsole}, ExporterOTLP); err != nil || e != ExporterConsole {
		t.Fatalf("resolveExporter = %v, %v", e, err)
	}
}
