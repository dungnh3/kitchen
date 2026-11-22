// Package prometheus exposes OpenTelemetry metrics through a Prometheus
// registry, so services that are scraped on /metrics keep working while
// instrumentation moves to the OTel metrics API.
//
//	reader, err := prometheus.NewReader(prometheus.Options{})
//	tel, err := sdk.Setup(ctx, sdk.Config{MetricReaders: []sdkmetric.Reader{reader}})
//	http.Handle("GET /metrics", promhttp.Handler())
package prometheus

import (
	"fmt"

	promclient "github.com/prometheus/client_golang/prometheus"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Options configures [NewReader].
type Options struct {
	// Registerer receives the OTel collector. nil means
	// prometheus.DefaultRegisterer, so the metrics appear on the existing
	// promhttp.Handler().
	Registerer promclient.Registerer
	// WithoutTargetInfo stops the target_info metric (resource attributes)
	// from being exported.
	WithoutTargetInfo bool
	// WithoutScopeInfo stops the otel_scope_* labels from being added.
	WithoutScopeInfo bool
}

// NewReader returns a metric reader that registers a collector with
// opts.Registerer. Create one reader per registry: two readers on the same
// registry export every metric twice.
func NewReader(opts Options) (sdkmetric.Reader, error) {
	reg := opts.Registerer
	if reg == nil {
		reg = promclient.DefaultRegisterer
	}
	o := []otelprom.Option{otelprom.WithRegisterer(reg)}
	if opts.WithoutTargetInfo {
		o = append(o, otelprom.WithoutTargetInfo())
	}
	if opts.WithoutScopeInfo {
		o = append(o, otelprom.WithoutScopeInfo())
	}
	r, err := otelprom.New(o...)
	if err != nil {
		return nil, fmt.Errorf("otelx/prometheus: %w", err)
	}
	return r, nil
}
