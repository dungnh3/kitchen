package prometheus_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/otelx/prometheus"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestReaderExposesMetrics(t *testing.T) {
	reg := promclient.NewRegistry()
	reader, err := prometheus.NewReader(prometheus.Options{Registerer: reg, WithoutTargetInfo: true})
	if err != nil {
		t.Fatal(err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	c, err := mp.Meter("test").Int64Counter("requests")
	if err != nil {
		t.Fatal(err)
	}
	c.Add(context.Background(), 3)

	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()
	resp, err := http.Get(srv.URL) //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "requests_total") || strings.Contains(string(body), "target_info") {
		t.Fatalf("unexpected exposition:\n%s", body)
	}
	srv.CloseClientConnections()
	http.DefaultClient.CloseIdleConnections()
}
