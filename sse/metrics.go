package sse

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// ScopeName is the OTel instrumentation scope used by this module.
const ScopeName = "github.com/dungnh3/kitchen/sse"

type serverMetrics struct {
	conns    metric.Int64UpDownCounter
	rejected metric.Int64Counter
	sent     metric.Int64Counter
	writeErr metric.Int64Counter
}

func newServerMetrics(mp metric.MeterProvider) serverMetrics {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	m := mp.Meter(ScopeName)
	// Instrument creation only fails for invalid names; the no-op fallback
	// returned alongside the error keeps the server usable.
	conns, _ := m.Int64UpDownCounter("sse.connections", metric.WithDescription("Active SSE connections."))
	rejected, _ := m.Int64Counter("sse.admission.rejected", metric.WithDescription("Connections refused by admission control."))
	sent, _ := m.Int64Counter("sse.events.sent", metric.WithDescription("Events written to clients."))
	writeErr, _ := m.Int64Counter("sse.write.errors", metric.WithDescription("Connections ended by a write error or timeout."))
	return serverMetrics{conns: conns, rejected: rejected, sent: sent, writeErr: writeErr}
}
