package otelx_test

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/propagation"

	"github.com/dungnh3/kitchen/otelx"
)

func charge(ctx context.Context, amount int) (err error) {
	ctx, span := otelx.Start(ctx, "charge")
	defer otelx.End(span, &err) // records err and sets status Error when non-nil

	_ = ctx
	if amount <= 0 {
		return errors.New("invalid amount")
	}
	return nil
}

func ExampleEnd() {
	fmt.Println(charge(context.Background(), 0))
	// Output: invalid amount
}

func ExampleInject() {
	// Producer: write trace context into message attributes.
	attrs := propagation.MapCarrier{}
	otelx.Inject(context.Background(), attrs)

	// Consumer: continue the trace.
	ctx := otelx.Extract(context.Background(), attrs)
	_ = otelx.WithSpan(ctx, "handle message", func(ctx context.Context) error { return nil })
}
