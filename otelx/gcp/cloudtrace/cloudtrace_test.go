package cloudtrace_test

import (
	"context"
	"testing"

	"go.uber.org/goleak"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/dungnh3/kitchen/otelx/gcp/cloudtrace"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// The Cloud Trace client starts OpenCensus workers that never stop.
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	)
}

func TestNew(t *testing.T) {
	ctx := context.Background()
	if _, err := cloudtrace.New(ctx, cloudtrace.Config{}); err == nil {
		t.Fatal("expected error without ProjectID")
	}
	creds := &google.Credentials{ProjectID: "p", TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "t"})}
	exp, err := cloudtrace.New(ctx, cloudtrace.Config{ProjectID: "p", Credentials: creds})
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
