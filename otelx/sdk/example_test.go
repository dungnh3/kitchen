package sdk_test

import (
	"context"
	"os"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/otelx/sdk"
)

func ExampleSetup() {
	ctx := context.Background()
	opts, _ := log.OptionsFromEnv()
	logger, _ := log.Setup(opts) // the process logger; OpenTelemetry's errors go through it too
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
		logger.ErrorContext(ctx, "telemetry setup failed", log.Err(err))
		os.Exit(1)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()
}
