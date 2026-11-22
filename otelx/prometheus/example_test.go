package prometheus_test

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dungnh3/kitchen/otelx/prometheus"
)

// Pass the reader to otelx/sdk and serve the registry on the admin port:
//
//	tel, _ := sdk.Setup(ctx, sdk.Config{MetricReaders: []sdkmetric.Reader{reader}})
//	srv, _ := server.New(server.WithAdmin(":9090", server.WithAdminHandler("GET /metrics", promhttp.Handler())))
func ExampleNewReader() {
	reader, err := prometheus.NewReader(prometheus.Options{}) // prometheus.DefaultRegisterer
	if err != nil {
		return
	}
	metrics := promhttp.Handler() // serves the default registry, now with OTel metrics
	_, _ = reader, metrics
}
