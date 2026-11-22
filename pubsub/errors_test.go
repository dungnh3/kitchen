package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/pubsub"
)

func TestPublishErrorCodes(t *testing.T) {
	tel := pubsub.NewTelemetry("test")
	boom := errors.New("broker down")
	for _, tc := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"broker failure", boom, codes.Unavailable},
		{"key paused", fmt.Errorf("k: %w", pubsub.ErrKeyPaused), codes.FailedPrecondition},
		{"closed", pubsub.ErrClosed, codes.Unavailable},
		{"canceled", context.Canceled, codes.Canceled},
		{"partial", &pubsub.PublishError{IDs: []string{"", "1"}, Failed: map[int]error{0: boom}}, codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tel.Publish(context.Background(), "t", []*pubsub.Message{{Data: []byte("x")}},
				func(context.Context, []*pubsub.Message) ([]string, error) { return nil, tc.err })
			if !errors.Is(err, tc.err) {
				t.Fatalf("cause lost: %v", err)
			}
			if got := errs.Code(err); got != tc.want {
				t.Fatalf("code = %v, want %v (%v)", got, tc.want, err)
			}
		})
	}
	_, err := tel.Publish(context.Background(), "t", []*pubsub.Message{nil},
		func(context.Context, []*pubsub.Message) ([]string, error) { return nil, nil })
	if got := errs.Code(err); got != codes.InvalidArgument {
		t.Fatalf("nil message code = %v", got)
	}
}
