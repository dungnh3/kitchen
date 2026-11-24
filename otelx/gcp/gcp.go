// Package gcp provides Google Cloud trace exporters and resource detection to
// plug into otelx/sdk:
//
//	exp, err := gcp.TraceExporter(ctx, gcp.Config{})
//	tel, err := sdk.Setup(ctx, sdk.Config{
//		ServiceName:       "orders",
//		SpanExporters:     []sdktrace.SpanExporter{exp},
//		ResourceDetectors: []resource.Detector{gcp.ResourceDetector(gcp.Config{})},
//	})
//
// [TraceExporter], [MetricExporter] and [LogExporter] send OTLP/gRPC to the
// Cloud Telemetry API (telemetry.googleapis.com), the path Google recommends
// for Cloud Trace, Cloud Monitoring and Cloud Logging, authenticated with
// Application Default Credentials (Workload Identity on GKE/Cloud Run) unless
// explicit credentials are given:
//
//	mexp, err := gcp.MetricExporter(ctx, gcp.Config{})
//	lexp, err := gcp.LogExporter(ctx, gcp.Config{})
//	sdk.Config{
//		MetricReaders: []sdkmetric.Reader{sdkmetric.NewPeriodicReader(mexp)},
//		LogProcessors: []sdklog.Processor{sdklog.NewBatchProcessor(lexp)},
//	}
//
// Metrics land in Cloud Monitoring under prometheus.googleapis.com/… names.
// The deprecated legacy Cloud Trace exporter lives in the separate module
// otelx/gcp/cloudtrace.
//
// The Telemetry API requires the gcp.project_id resource attribute, which
// [ResourceDetector] adds.
package gcp

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/compute/metadata"
	gcpdetector "go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

// DefaultEndpoint is the Cloud Telemetry API OTLP endpoint.
const DefaultEndpoint = "telemetry.googleapis.com:443"

// Scope is the OAuth scope used for export.
const Scope = "https://www.googleapis.com/auth/cloud-platform"

// Config configures the exporters and the detector. The zero value uses ADC,
// the project of the credentials or the metadata server, and DefaultEndpoint.
type Config struct {
	// ProjectID is the project that receives the telemetry; ResourceDetector
	// writes it as gcp.project_id. Empty means the credentials' project, then
	// the GCE/GKE/Cloud Run metadata server.
	ProjectID string
	// CredentialsJSON is the content of a credentials file. It takes
	// precedence over CredentialsFile. Only the service_account,
	// authorized_user, external_account and impersonated_service_account
	// types are accepted. Never commit it; load it from a secret.
	CredentialsJSON []byte
	// CredentialsFile is the path of a credentials file (same types).
	CredentialsFile string
	// Endpoint overrides DefaultEndpoint for the exporters.
	Endpoint string
	// Timeout is the per-export timeout. Zero means 10s.
	Timeout time.Duration
	// DisableDetection skips the GCP platform detector and the metadata
	// server lookup (local development, tests).
	DisableDetection bool
}

func (c Config) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return 10 * time.Second
}

// exportConn holds the settings shared by the three exporters.
type exportConn struct {
	endpoint string
	tls      credentials.TransportCredentials
	perRPC   grpc.DialOption
	timeout  time.Duration
	headers  map[string]string
}

func newExportConn(ctx context.Context, cfg Config) (exportConn, error) {
	creds, err := findCredentials(ctx, cfg)
	if err != nil {
		return exportConn{}, err
	}
	c := exportConn{
		endpoint: cfg.Endpoint,
		tls:      credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
		perRPC:   grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: creds.TokenSource}),
		timeout:  cfg.timeout(),
	}
	if c.endpoint == "" {
		c.endpoint = DefaultEndpoint
	}
	if qp := quotaProject(creds); qp != "" {
		// User credentials need a quota project; service accounts bill
		// their own project.
		c.headers = map[string]string{"x-goog-user-project": qp}
	}
	return c, nil
}

// TraceExporter returns an OTLP/gRPC span exporter for the Cloud Telemetry
// API, authenticated with OAuth over TLS. The connection is established
// lazily, so no export call happens here; finding Application Default
// Credentials may still query the metadata server.
func TraceExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	c, err := newExportConn(ctx, cfg)
	if err != nil {
		return nil, err
	}
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(c.endpoint),
		otlptracegrpc.WithTLSCredentials(c.tls),
		otlptracegrpc.WithDialOption(c.perRPC),
		otlptracegrpc.WithTimeout(c.timeout),
	}
	if c.headers != nil {
		opts = append(opts, otlptracegrpc.WithHeaders(c.headers))
	}
	exp, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcp: trace exporter: %w", err)
	}
	return exp, nil
}

// MetricExporter returns an OTLP/gRPC metric exporter for the Cloud Telemetry
// API (Cloud Monitoring), set up like [TraceExporter]. Wrap it in a
// sdkmetric.PeriodicReader; Cloud Monitoring accepts at most one point per
// time series every 5s, so keep the export interval at 10s or more.
func MetricExporter(ctx context.Context, cfg Config) (sdkmetric.Exporter, error) {
	c, err := newExportConn(ctx, cfg)
	if err != nil {
		return nil, err
	}
	opts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(c.endpoint),
		otlpmetricgrpc.WithTLSCredentials(c.tls),
		otlpmetricgrpc.WithDialOption(c.perRPC),
		otlpmetricgrpc.WithTimeout(c.timeout),
	}
	if c.headers != nil {
		opts = append(opts, otlpmetricgrpc.WithHeaders(c.headers))
	}
	exp, err := otlpmetricgrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcp: metric exporter: %w", err)
	}
	return exp, nil
}

// LogExporter returns an OTLP/gRPC log exporter for the Cloud Telemetry API
// (Cloud Logging), set up like [TraceExporter]. Wrap it in a
// sdklog.BatchProcessor.
func LogExporter(ctx context.Context, cfg Config) (sdklog.Exporter, error) {
	c, err := newExportConn(ctx, cfg)
	if err != nil {
		return nil, err
	}
	opts := []otlploggrpc.Option{
		otlploggrpc.WithEndpoint(c.endpoint),
		otlploggrpc.WithTLSCredentials(c.tls),
		otlploggrpc.WithDialOption(c.perRPC),
		otlploggrpc.WithTimeout(c.timeout),
	}
	if c.headers != nil {
		opts = append(opts, otlploggrpc.WithHeaders(c.headers))
	}
	exp, err := otlploggrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcp: log exporter: %w", err)
	}
	return exp, nil
}

// Credentials resolves the credentials described by cfg (explicit JSON, a
// file, or ADC) with [Scope]. It is exported for exporters that live in
// other modules, such as otelx/gcp/cloudtrace.
func Credentials(ctx context.Context, cfg Config) (*google.Credentials, error) {
	return findCredentials(ctx, cfg)
}

// ProjectID resolves the project: cfg.ProjectID, then the credentials'
// project, then the metadata server (unless cfg.DisableDetection).
func ProjectID(ctx context.Context, cfg Config, creds *google.Credentials) (string, error) {
	return resolveProject(ctx, cfg, creds)
}

// ResourceDetector returns a detector that adds gcp.project_id (required by
// the Telemetry API) and, unless cfg.DisableDetection, the GCP platform
// attributes (cloud.*, k8s.*, faas.*, host.*).
func ResourceDetector(cfg Config) resource.Detector { return detector{cfg: cfg} }

type detector struct{ cfg Config }

func (d detector) Detect(ctx context.Context) (*resource.Resource, error) {
	var creds *google.Credentials
	if d.cfg.ProjectID == "" {
		c, err := findCredentials(ctx, d.cfg)
		if err != nil {
			return nil, err
		}
		creds = c
	}
	project, err := resolveProject(ctx, d.cfg, creds)
	if err != nil {
		return nil, err
	}
	res := resource.NewSchemaless(attribute.String("gcp.project_id", project))
	if d.cfg.DisableDetection {
		return res, nil
	}
	platform, err := gcpdetector.NewDetector().Detect(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcp: detect platform: %w", err)
	}
	return resource.Merge(platform, res)
}

// allowedTypes are the credential file types accepted from configuration.
// Other types (and unvalidated configs) are rejected: a crafted
// external_account-like file must not make the process fetch arbitrary
// URLs or files.
var allowedTypes = []google.CredentialsType{
	google.ServiceAccount,
	google.AuthorizedUser,
	google.ExternalAccount,
	google.ImpersonatedServiceAccount,
}

func findCredentials(ctx context.Context, cfg Config) (*google.Credentials, error) {
	data := cfg.CredentialsJSON
	if len(data) == 0 && cfg.CredentialsFile != "" {
		b, err := os.ReadFile(cfg.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("gcp: read credentials file: %w", err)
		}
		data = b
	}
	if len(data) == 0 {
		creds, err := google.FindDefaultCredentials(ctx, Scope)
		if err != nil {
			return nil, fmt.Errorf("gcp: application default credentials: %w", err)
		}
		return creds, nil
	}
	return credentialsFromJSON(ctx, data)
}

func credentialsFromJSON(ctx context.Context, data []byte) (*google.Credentials, error) {
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, fmt.Errorf("gcp: parse credentials JSON: %w", err)
	}
	t := google.CredentialsType(head.Type)
	ok := false
	for _, a := range allowedTypes {
		ok = ok || t == a
	}
	if !ok {
		return nil, fmt.Errorf("gcp: unsupported credentials type %q", head.Type)
	}
	creds, err := google.CredentialsFromJSONWithType(ctx, data, t, Scope)
	if err != nil {
		return nil, fmt.Errorf("gcp: load credentials: %w", err)
	}
	return creds, nil
}

// errNoProject is returned when no project id can be determined.
var errNoProject = errors.New("gcp: project id could not be determined; set Config.ProjectID")

func resolveProject(ctx context.Context, cfg Config, creds *google.Credentials) (string, error) {
	if cfg.ProjectID != "" {
		return cfg.ProjectID, nil
	}
	if creds != nil && creds.ProjectID != "" {
		return creds.ProjectID, nil
	}
	if !cfg.DisableDetection && metadata.OnGCE() {
		if id, err := metadata.ProjectIDWithContext(ctx); err == nil && strings.TrimSpace(id) != "" {
			return strings.TrimSpace(id), nil
		}
	}
	return "", errNoProject
}

// quotaProject returns the project to bill when it differs from the
// credentials' own: GOOGLE_CLOUD_QUOTA_PROJECT, else the quota_project_id of
// user credentials.
func quotaProject(creds *google.Credentials) string {
	if v := strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_QUOTA_PROJECT")); v != "" {
		return v
	}
	if creds == nil || len(creds.JSON) == 0 {
		return ""
	}
	var f struct {
		QuotaProjectID string `json:"quota_project_id"`
	}
	if json.Unmarshal(creds.JSON, &f) != nil {
		return ""
	}
	return strings.TrimSpace(f.QuotaProjectID)
}
