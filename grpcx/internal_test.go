package grpcx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
)

func TestServiceConfig(t *testing.T) {
	tests := []struct {
		name  string
		lb    string
		retry *RetryPolicy
		want  string
	}{
		{
			"defaults", "round_robin", &RetryPolicy{},
			`{"loadBalancingConfig":[{"round_robin":{}}],"methodConfig":[{"name":[{}],"retryPolicy":{"maxAttempts":3,"initialBackoff":"0.1s","maxBackoff":"1s","backoffMultiplier":2,"retryableStatusCodes":["UNAVAILABLE"]}}],"retryThrottling":{"maxTokens":10,"tokenRatio":0.1}}`,
		},
		{"no retry", "pick_first", nil, `{"loadBalancingConfig":[{"pick_first":{}}]}`},
		{
			"custom", "round_robin", &RetryPolicy{MaxAttempts: 5, InitialBackoff: 50 * time.Millisecond, MaxBackoff: 2 * time.Second, BackoffMultiplier: 1.5, Codes: []codes.Code{codes.Unavailable, codes.Canceled}},
			`{"loadBalancingConfig":[{"round_robin":{}}],"methodConfig":[{"name":[{}],"retryPolicy":{"maxAttempts":5,"initialBackoff":"0.05s","maxBackoff":"2s","backoffMultiplier":1.5,"retryableStatusCodes":["UNAVAILABLE","CANCELLED"]}}],"retryThrottling":{"maxTokens":10,"tokenRatio":0.1}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := serviceConfig(tt.lb, tt.retry)
			if err != nil || got != tt.want {
				t.Errorf("got %s, %v\nwant %s", got, err, tt.want)
			}
		})
	}
}

// call runs one unary call through the breaker with a fake invoker that
// returns err.
func call(ctx context.Context, b *breaker, err error) error {
	inv := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return mapError(ctx, "/s/M", err, DefaultErrorMapper)
	}
	return b.unary(ctx, "/s/M", nil, nil, nil, inv)
}

func TestBreakerLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var changes []string
		b := newBreaker("t", BreakerConfig{
			FailureThreshold: 2,
			ResetTimeout:     10 * time.Second,
			OnStateChange:    func(from, to string) { changes = append(changes, from+">"+to) },
		}, log.Discard())
		down := status.Error(codes.Unavailable, "down")
		ctx := context.Background()

		_ = call(ctx, b, down)
		_ = call(ctx, b, status.Error(codes.NotFound, "x")) // not a failure
		_ = call(ctx, b, down)
		if err := call(ctx, b, nil); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("after 2 failures: %v, want open", err)
		}
		time.Sleep(10*time.Second + time.Millisecond) // fake clock: the breaker becomes half-open
		if err := call(ctx, b, nil); err != nil {
			t.Fatalf("probe: %v", err)
		}
		if err := call(ctx, b, down); errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("closed again, got %v", err)
		}
		if got := strings.Join(changes, ","); got != "closed>open,open>half-open,half-open>closed" {
			t.Errorf("changes %s", got)
		}
	})
}

func TestBreakerRatioAndWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBreaker("t", BreakerConfig{FailureRatio: 0.5, MinRequests: 4, ResetTimeout: 10 * time.Second}, log.Discard())
		ctx := context.Background()
		down := status.Error(codes.Internal, "boom")
		_ = call(ctx, b, down)
		_ = call(ctx, b, nil)
		_ = call(ctx, b, down)
		// 2 of 3 failed but MinRequests is 4: still closed.
		if err := call(ctx, b, nil); errors.Is(err, ErrCircuitOpen) {
			t.Fatal("opened before MinRequests")
		}
		// The window slides: old calls are forgotten.
		time.Sleep(11 * time.Second)
		_ = call(ctx, b, down)
		if err := call(ctx, b, nil); errors.Is(err, ErrCircuitOpen) {
			t.Fatal("old calls still counted")
		}
		_ = call(ctx, b, down)
		_ = call(ctx, b, down)
		if err := call(ctx, b, nil); !errors.Is(err, ErrCircuitOpen) || !errors.Is(err, errs.ErrDependencyUnavailable) {
			t.Fatalf("3 of 4 failed: %v, want open", err)
		}
	})
}

func TestBreakerCustomIsFailure(t *testing.T) {
	b := newBreaker("t", BreakerConfig{FailureThreshold: 1, IsFailure: func(c codes.Code) bool { return c == codes.ResourceExhausted }}, log.Discard())
	ctx := context.Background()
	_ = call(ctx, b, status.Error(codes.Unavailable, "x"))
	if err := call(ctx, b, nil); err != nil {
		t.Fatalf("UNAVAILABLE counted: %v", err)
	}
	_ = call(ctx, b, status.Error(codes.ResourceExhausted, "x"))
	if err := call(ctx, b, nil); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("RESOURCE_EXHAUSTED not counted: %v", err)
	}
}
