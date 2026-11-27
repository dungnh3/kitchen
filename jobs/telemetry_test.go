package jobs_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/jobs/jobstest"
)

func TestTelemetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := tracetest.NewSpanRecorder()
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

		drv := jobstest.New()
		reg := jobs.NewRegistry()
		register(t, reg, func(context.Context, *jobs.Job[echo]) error { return nil })
		register(t, reg, func(context.Context, *jobs.Job[tick]) error { return jobs.Cancel(errors.New("no")) })
		c := newClient(t, drv, reg, jobs.WithTracerProvider(tp), jobs.WithMeterProvider(mp), jobs.WithPropagator(propagation.TraceContext{}))
		stop := start(t, c)
		defer stop()

		// The enqueuer is inside a request span.
		ctx, req := tp.Tracer("test").Start(context.Background(), "request")
		if _, err := c.Insert(ctx, echo{}, nil); err != nil {
			t.Fatal(err)
		}
		req.End()
		if _, err := c.Insert(context.Background(), tick{}, nil); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		synctest.Wait()

		var insert, work trace.SpanContext
		var workParent trace.SpanID
		for _, s := range rec.Ended() {
			switch s.Name() {
			case "jobs.insert":
				if s.Parent().SpanID() == req.SpanContext().SpanID() {
					insert = s.SpanContext()
				}
			case "jobs.work echo":
				work, workParent = s.SpanContext(), s.Parent().SpanID()
			}
		}
		if !insert.IsValid() || !work.IsValid() {
			t.Fatalf("spans: insert %v work %v in %d spans", insert.IsValid(), work.IsValid(), len(rec.Ended()))
		}
		if work.TraceID() != req.SpanContext().TraceID() || workParent != insert.SpanID() {
			t.Fatalf("the work span is not a child of the insert span: trace %s parent %s, want %s %s",
				work.TraceID(), workParent, req.SpanContext().TraceID(), insert.SpanID())
		}

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatal(err)
		}
		counts := map[string]int64{}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				sum, ok := m.Data.(metricdata.Sum[int64])
				if !ok {
					continue
				}
				for _, dp := range sum.DataPoints {
					outcome, _ := dp.Attributes.Value("kitchen.jobs.outcome")
					counts[m.Name+"/"+outcome.AsString()] += dp.Value
				}
			}
		}
		for k, want := range map[string]int64{
			"kitchen.jobs.inserted/":          2,
			"kitchen.jobs.finished/completed": 1,
			"kitchen.jobs.finished/cancelled": 1,
		} {
			if counts[k] != want {
				t.Errorf("%s = %d, want %d (all: %v)", k, counts[k], want, counts)
			}
		}
	})
}
