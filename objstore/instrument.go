package objstore

import (
	"context"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/otelx"
)

const scope = "github.com/dungnh3/kitchen/objstore"

// InstrumentOption configures Instrument.
type InstrumentOption func(*instrumented)

// WithTracerProvider sets the tracer provider. Default the global one.
func WithTracerProvider(tp trace.TracerProvider) InstrumentOption {
	return func(i *instrumented) { i.tp = tp }
}

// WithSystem names the backend in the "objstore.system" span attribute
// ("gcs", "s3", ...).
func WithSystem(name string) InstrumentOption {
	return func(i *instrumented) { i.system = name }
}

// Instrumented is a Bucket and Signer whose operations are traced.
type Instrumented interface {
	Bucket
	Signer
}

// Instrument wraps b so every operation runs in a span named "objstore.<Op>"
// with the key and, for writes, the byte count; errors are recorded with
// otelx. A writer's span lasts until Close. If b is not a Signer, the
// signing methods return errs.ErrUnimplemented.
func Instrument(b Bucket, opts ...InstrumentOption) Instrumented {
	i := &instrumented{b: b}
	for _, o := range opts {
		o(i)
	}
	return i
}

type instrumented struct {
	b      Bucket
	tp     trace.TracerProvider
	system string
}

func (i *instrumented) start(ctx context.Context, op, key string) (context.Context, trace.Span) {
	tp := i.tp
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	attrs := []attribute.KeyValue{attribute.String("objstore.key", key)}
	if i.system != "" {
		attrs = append(attrs, attribute.String("objstore.system", i.system))
	}
	return tp.Tracer(scope).Start(ctx, "objstore."+op, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
}

func (i *instrumented) NewWriter(ctx context.Context, key string, opts *WriterOptions) (io.WriteCloser, error) {
	ctx, span := i.start(ctx, "Write", key)
	w, err := i.b.NewWriter(ctx, key, opts)
	if err != nil {
		otelx.End(span, &err)
		return nil, err
	}
	return &tracedWriter{w: w, span: span}, nil
}

type tracedWriter struct {
	w    io.WriteCloser
	span trace.Span
	n    int64
	done bool
}

func (t *tracedWriter) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	t.n += int64(n)
	return n, err
}

func (t *tracedWriter) Close() error {
	err := t.w.Close()
	if !t.done {
		t.done = true
		t.span.SetAttributes(attribute.Int64("objstore.bytes", t.n))
		otelx.End(t.span, &err)
	}
	return err
}

func (i *instrumented) NewReader(ctx context.Context, key string) (_ io.ReadCloser, err error) {
	ctx, span := i.start(ctx, "Read", key)
	defer otelx.End(span, &err)
	return i.b.NewReader(ctx, key)
}

func (i *instrumented) Attributes(ctx context.Context, key string) (_ *Attrs, err error) {
	ctx, span := i.start(ctx, "Attributes", key)
	defer otelx.End(span, &err)
	return i.b.Attributes(ctx, key)
}

func (i *instrumented) Delete(ctx context.Context, key string) (err error) {
	ctx, span := i.start(ctx, "Delete", key)
	defer otelx.End(span, &err)
	return i.b.Delete(ctx, key)
}

func (i *instrumented) Close() error { return i.b.Close() }

func (i *instrumented) SignedURL(ctx context.Context, key string, opts SignOptions) (_ string, err error) {
	ctx, span := i.start(ctx, "SignedURL", key)
	defer otelx.End(span, &err)
	s, ok := i.b.(Signer)
	if !ok {
		return "", errs.ErrUnimplemented
	}
	return s.SignedURL(ctx, key, opts)
}

func (i *instrumented) PostPolicy(ctx context.Context, key string, opts PostPolicyOptions) (_ *PostPolicy, err error) {
	ctx, span := i.start(ctx, "PostPolicy", key)
	defer otelx.End(span, &err)
	s, ok := i.b.(Signer)
	if !ok {
		return nil, errs.ErrUnimplemented
	}
	return s.PostPolicy(ctx, key, opts)
}
