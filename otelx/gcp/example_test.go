package gcp_test

import (
	"context"

	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/dungnh3/kitchen/otelx/gcp"
)

// Build the Google Cloud exporters and the detector, then pass them to
// otelx/sdk:
//
//	sdk.Setup(ctx, sdk.Config{
//		ServiceName:       "orders",
//		SpanExporters:     exporters,
//		MetricReaders:     readers,
//		LogProcessors:     processors,
//		ResourceDetectors: detectors,
//	})
func ExampleTraceExporter() {
	ctx := context.Background()
	cfg := gcp.Config{} // ADC / Workload Identity; project from the credentials or metadata server
	texp, err := gcp.TraceExporter(ctx, cfg)
	if err != nil {
		return
	}
	mexp, err := gcp.MetricExporter(ctx, cfg)
	if err != nil {
		return
	}
	lexp, err := gcp.LogExporter(ctx, cfg)
	if err != nil {
		return
	}
	exporters := []sdktrace.SpanExporter{texp}
	readers := []sdkmetric.Reader{sdkmetric.NewPeriodicReader(mexp)}
	processors := []sdklog.Processor{sdklog.NewBatchProcessor(lexp)}
	detectors := []resource.Detector{gcp.ResourceDetector(cfg)} // adds gcp.project_id, required by the API
	_, _, _, _ = exporters, readers, processors, detectors
}
