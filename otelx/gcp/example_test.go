package gcp_test

import (
	"context"

	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/dungnh3/kitchen/otelx/gcp"
)

// Wire the exporter and detector into otelx/sdk.Config:
//
//	sdk.Setup(ctx, sdk.Config{
//		ServiceName:       "orders",
//		SpanExporters:     exporters,
//		ResourceDetectors: detectors,
//	})
func ExampleTraceExporter() {
	ctx := context.Background()
	exp, err := gcp.TraceExporter(ctx, gcp.Config{}) // ADC / Workload Identity
	if err != nil {
		return
	}
	exporters := []sdktrace.SpanExporter{exp}
	detectors := []resource.Detector{gcp.ResourceDetector(gcp.Config{})}
	_, _ = exporters, detectors
}
