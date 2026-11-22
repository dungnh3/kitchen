// Package otelx holds API-only OpenTelemetry helpers: span helpers, error
// recording, context propagation and gRPC/HTTP instrumentation with sensible
// filters.
//
// Everything here uses the global providers of go.opentelemetry.io/otel, which
// stay no-op until the application installs an SDK (see the
// github.com/dungnh3/kitchen/otelx/sdk module). Libraries may import this
// package freely; it pulls in no SDK.
package otelx

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the instrumentation scope used by [Tracer].
const ScopeName = "github.com/dungnh3/kitchen/otelx"

// Tracer returns a tracer from the global TracerProvider. It is resolved on
// every call, so it follows later calls to otel.SetTracerProvider.
func Tracer() trace.Tracer { return otel.Tracer(ScopeName) }

// Start starts a span with the global tracer. The caller must end it.
func Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, opts...)
}

// RecordError records err as an exception event on span and sets the span
// status to Error with err.Error() as description. A nil err does nothing.
func RecordError(span trace.Span, err error, opts ...trace.EventOption) {
	if err == nil || span == nil {
		return
	}
	span.RecordError(err, opts...)
	span.SetStatus(codes.Error, err.Error())
}

// End ends span, recording *errp first when it is a non-nil error. It is meant
// for deferred use with a named error result:
//
//	func charge(ctx context.Context) (err error) {
//		ctx, span := otelx.Start(ctx, "charge")
//		defer otelx.End(span, &err)
//		...
//	}
func End(span trace.Span, errp *error) {
	if errp != nil {
		RecordError(span, *errp)
	}
	span.End()
}

// WithSpan runs fn inside a new span, recording the returned error. A panic in
// fn is recorded on the span and then re-raised.
func WithSpan(ctx context.Context, name string, fn func(context.Context) error, opts ...trace.SpanStartOption) (err error) {
	ctx, span := Start(ctx, name, opts...)
	defer func() {
		if r := recover(); r != nil {
			RecordError(span, fmt.Errorf("panic: %v", r), trace.WithStackTrace(true))
			span.End()
			panic(r)
		}
		End(span, &err)
	}()
	return fn(ctx)
}

// Inject writes the span context and baggage of ctx into carrier using the
// global propagator. Use propagation.HeaderCarrier for HTTP headers and
// propagation.MapCarrier for message attributes.
func Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// Extract returns ctx with the span context and baggage read from carrier
// using the global propagator.
func Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// TraceID returns the hex trace id of the span in ctx, or "" if none.
func TraceID(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// SpanID returns the hex span id of the span in ctx, or "" if none.
func SpanID(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.HasSpanID() {
		return sc.SpanID().String()
	}
	return ""
}
