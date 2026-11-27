package jobs

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/dungnh3/kitchen/log"
)

const scopeName = "github.com/dungnh3/kitchen/jobs"

// Outcomes recorded on kitchen.jobs.finished and kitchen.jobs.duration.
const (
	outcomeCompleted = "completed"
	outcomeRetried   = "retried"
	outcomeSnoozed   = "snoozed"
	outcomeCancelled = "cancelled"
	outcomeDiscarded = "discarded"
	outcomeReleased  = "released" // given back at shutdown
	outcomeLost      = "lost"     // the claim was lost while running
)

var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

type telemetry struct {
	tracer   trace.Tracer
	prop     propagation.TextMapPropagator
	inserted metric.Int64Counter
	finished metric.Int64Counter
	duration metric.Float64Histogram
	lag      metric.Float64Histogram
}

func newTelemetry(o *options) *telemetry {
	tp, mp, prop := o.tp, o.mp, o.prop
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	if prop == nil {
		prop = otel.GetTextMapPropagator()
	}
	m := mp.Meter(scopeName)
	t := &telemetry{tracer: tp.Tracer(scopeName), prop: prop}
	// Instrument creation only fails for invalid names, which are constants
	// here; a failed instrument is a no-op.
	t.inserted, _ = m.Int64Counter("kitchen.jobs.inserted",
		metric.WithUnit("{job}"), metric.WithDescription("Jobs inserted, duplicates excluded."))
	t.finished, _ = m.Int64Counter("kitchen.jobs.finished",
		metric.WithUnit("{job}"), metric.WithDescription("Job attempts that ended, by outcome."))
	t.duration, _ = m.Float64Histogram("kitchen.jobs.duration",
		metric.WithUnit("s"), metric.WithDescription("Duration of a job attempt."),
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	t.lag, _ = m.Float64Histogram("kitchen.jobs.queue_lag",
		metric.WithUnit("s"), metric.WithDescription("Time a job waited past its scheduled time before it was claimed."),
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	return t
}

func jobAttrs(kind, queue string) []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("kitchen.jobs.kind", kind), attribute.String("kitchen.jobs.queue", queue)}
}

// startInsert starts the producer span of an insert and returns the trace
// context to store with the jobs.
func (t *telemetry) startInsert(ctx context.Context, n int) (context.Context, trace.Span, map[string]string) {
	ctx, span := t.tracer.Start(ctx, "jobs.insert", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.Int("kitchen.jobs.count", n)))
	meta := map[string]string{}
	t.prop.Inject(ctx, propagation.MapCarrier(meta))
	return ctx, span, meta
}

// startWork starts the consumer span of an attempt, a child of the
// enqueuer's span when the row carries one.
func (t *telemetry) startWork(ctx context.Context, c Claimed, meta map[string]string) (context.Context, trace.Span) {
	if len(meta) > 0 {
		ctx = t.prop.Extract(ctx, propagation.MapCarrier(meta))
	}
	return t.tracer.Start(ctx, "jobs.work "+c.Kind, trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(append(jobAttrs(c.Kind, c.Queue),
			attribute.Int64("kitchen.jobs.id", c.ID), attribute.Int("kitchen.jobs.attempt", c.Attempt))...))
}

func (t *telemetry) recordFinish(ctx context.Context, c Claimed, outcome string, d time.Duration) {
	attrs := metric.WithAttributes(append(jobAttrs(c.Kind, c.Queue), attribute.String("kitchen.jobs.outcome", outcome))...)
	t.finished.Add(ctx, 1, attrs)
	t.duration.Record(ctx, d.Seconds(), attrs)
}

func metricAttrs(c Claimed) metric.MeasurementOption {
	return metric.WithAttributes(jobAttrs(c.Kind, c.Queue)...)
}

// logAttrs are the log attributes of a claimed job. Args are never logged.
func logAttrs(c Claimed) []any {
	return []any{
		log.Int64("job_id", c.ID), log.String("job_kind", c.Kind), log.String("queue", c.Queue),
		log.Int("attempt", c.Attempt), log.Int("max_attempts", c.MaxAttempts),
	}
}
