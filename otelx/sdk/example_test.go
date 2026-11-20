package sdk_test

import (
	"context"
	"log/slog"
	"os"

	"github.com/dungnh3/kitchen/otelx/sdk"
)

func ExampleSetup() {
	ctx := context.Background()
	tel, err := sdk.Setup(ctx, sdk.Config{
		ServiceName:    "orders",
		ServiceVersion: "1.4.0",
		Environment:    "production",
		// Any OTLP backend; vendor auth is just a header.
		Traces: sdk.SignalConfig{
			Endpoint: "otel-collector.observability:4317",
			Insecure: true,
		},
	})
	if err != nil {
		slog.Error("telemetry setup failed", "error", err)
		os.Exit(1)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	// Optional: also ship logs over OTLP (requires Logs.Exporter = otlp):
	//   logger := slog.New(slog.NewMultiHandler(
	//       log.NewHandler(os.Stdout, opts), // stdout JSON with trace keys
	//       tel.SlogHandler("orders"),       // OTLP; carries the span context natively
	//   ))
}
