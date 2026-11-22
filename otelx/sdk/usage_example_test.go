package sdk_test

import (
	"context"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/otelx/sdk"
)

// Ship logs over OTLP as well as stdout: add the OTel bridge as a
// kitchen/log sink. Setup telemetry first, then the logger.
func Example_otlpLogs() {
	ctx := context.Background()
	tel, err := sdk.Setup(ctx, sdk.Config{
		ServiceName: "orders",
		Logs:        sdk.SignalConfig{Exporter: sdk.ExporterOTLP},
	})
	if err != nil {
		return
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	opts, _ := log.OptionsFromEnv()
	opts.Handlers = append(opts.Handlers, tel.SlogHandler("orders")) // stdout JSON + OTLP
	logger, _ := log.Setup(opts)
	logger.InfoContext(ctx, "started")
}

// An internet-facing service: sample 10% of new traces and ignore the
// caller's sampled flag, so clients cannot force 100% sampling.
func Example_publicService() {
	ratio := 0.1
	tel, err := sdk.Setup(context.Background(), sdk.Config{
		ServiceName:         "gateway",
		SampleRatio:         &ratio,
		IgnoreRemoteSampled: true,
		Metrics:             sdk.SignalConfig{Exporter: sdk.ExporterOTLP},
	})
	if err != nil {
		return
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()
}
