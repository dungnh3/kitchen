// Package cloudtrace wraps the legacy Google Cloud Trace v2 span exporter.
//
// Deprecated: Google deprecated the underlying exporter
// (github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace)
// and will archive it after 2027-01-01. Use otelx/gcp.TraceExporter (OTLP to
// the Cloud Telemetry API) instead. This module exists only for services that
// cannot move yet.
package cloudtrace

import (
	"context"
	"errors"
	"fmt"
	"time"

	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace" //nolint:staticcheck // deprecated upstream; see package doc
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

// Config configures [New]. Resolve Credentials and ProjectID with
// otelx/gcp.Credentials and otelx/gcp.ProjectID.
type Config struct {
	// ProjectID receives the traces. Required.
	ProjectID string
	// Credentials authenticate the export. nil means Application Default
	// Credentials as found by the Cloud Trace client.
	Credentials *google.Credentials
	// Timeout is the per-export timeout. Zero means 10s.
	Timeout time.Duration
}

// New returns a Cloud Trace v2 span exporter.
//
// Deprecated: see the package documentation.
func New(_ context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	if cfg.ProjectID == "" {
		return nil, errors.New("cloudtrace: ProjectID is required")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	opts := []texporter.Option{
		texporter.WithProjectID(cfg.ProjectID),
		texporter.WithTimeout(timeout),
	}
	if cfg.Credentials != nil {
		opts = append(opts, texporter.WithTraceClientOptions([]option.ClientOption{option.WithCredentials(cfg.Credentials)}))
	}
	exp, err := texporter.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("cloudtrace: %w", err)
	}
	return exp, nil
}
