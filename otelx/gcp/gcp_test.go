package gcp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func serviceAccountJSON(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(map[string]string{ //nolint:gosec // G101: fake service-account key for the test
		"type":           "service_account",
		"project_id":     "sa-project",
		"private_key_id": "k1",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   "sa@sa-project.iam.gserviceaccount.com",
		"client_id":      "1",
		"token_uri":      "https://oauth2.googleapis.com/token",
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCredentialsValidation(t *testing.T) {
	ctx := context.Background()
	tests := map[string][]byte{
		"not json":     []byte("{"),
		"unknown type": []byte(`{"type":"gdch_service_account"}`),
		"empty type":   []byte(`{}`),
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := TraceExporter(ctx, Config{CredentialsJSON: data}); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := TraceExporter(ctx, Config{CredentialsFile: filepath.Join(t.TempDir(), "missing.json")}); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestExportersWithServiceAccount(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sa.json")
	if err := os.WriteFile(path, serviceAccountJSON(t), 0o600); err != nil {
		t.Fatal(err)
	}
	exp, err := TraceExporter(ctx, Config{CredentialsFile: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	mexp, err := MetricExporter(ctx, Config{CredentialsFile: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := mexp.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	lexp, err := LogExporter(ctx, Config{CredentialsFile: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := lexp.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	// Invalid credentials fail for every signal.
	if _, err := MetricExporter(ctx, Config{CredentialsJSON: []byte(`{"type":"evil"}`)}); err == nil {
		t.Fatal("metric exporter accepted bad credentials")
	}
	if _, err := LogExporter(ctx, Config{CredentialsJSON: []byte(`{"type":"evil"}`)}); err == nil {
		t.Fatal("log exporter accepted bad credentials")
	}
}

func TestExportedResolvers(t *testing.T) {
	ctx := context.Background()
	cfg := Config{CredentialsJSON: serviceAccountJSON(t), DisableDetection: true}
	creds, err := Credentials(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if p, err := ProjectID(ctx, cfg, creds); err != nil || p != "sa-project" {
		t.Fatalf("ProjectID = %q, %v", p, err)
	}
}

func TestResourceDetector(t *testing.T) {
	ctx := context.Background()
	res, err := ResourceDetector(Config{ProjectID: "p1", DisableDetection: true}).Detect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := res.Set().Value("gcp.project_id"); v.AsString() != "p1" {
		t.Fatalf("gcp.project_id = %v", v)
	}

	res, err = ResourceDetector(Config{CredentialsJSON: serviceAccountJSON(t), DisableDetection: true}).Detect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := res.Set().Value(attribute.Key("gcp.project_id")); v.AsString() != "sa-project" {
		t.Fatalf("gcp.project_id from credentials = %v", v)
	}
}

func TestResolveProjectFails(t *testing.T) {
	_, err := resolveProject(context.Background(), Config{DisableDetection: true}, nil)
	if !errors.Is(err, errNoProject) {
		t.Fatalf("err = %v", err)
	}
}

func TestQuotaProject(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", "")
	creds, err := credentialsFromJSON(context.Background(), []byte(
		`{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"r","quota_project_id":"qp"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := quotaProject(creds); got != "qp" {
		t.Fatalf("quota project = %q", got)
	}
	t.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", " env ")
	if got := quotaProject(creds); got != "env" {
		t.Fatalf("quota project = %q", got)
	}
	if !strings.Contains(Scope, "cloud-platform") {
		t.Fatal("scope")
	}
}
