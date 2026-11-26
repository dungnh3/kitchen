package grpcx

import (
	"context"
	"errors"
	"time"

	"github.com/sony/gobreaker/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
)

// BreakerConfig configures the circuit breaker of a Conn. A Conn talks to
// one target, so it has one breaker. While the breaker is open, calls fail
// at once with an errs.ErrDependencyUnavailable that wraps ErrCircuitOpen,
// without being sent. After ResetTimeout SuccessThreshold probes are let
// through (half-open). The fields and defaults are those of
// httpx.BreakerConfig. Zero fields take the defaults.
type BreakerConfig struct {
	// FailureThreshold opens the breaker after this many failures within
	// ResetTimeout. Default 5. Ignored when FailureRatio is set.
	FailureThreshold uint64
	// SuccessThreshold closes a half-open breaker after this many
	// successful probes. Default 1.
	SuccessThreshold uint32
	// FailureRatio (0..1], when set, opens the breaker when this share of
	// the calls within ResetTimeout failed, after MinRequests calls.
	FailureRatio float64
	// MinRequests is the minimum number of calls before FailureRatio
	// applies. Default 10.
	MinRequests uint64
	// ResetTimeout is both the sliding window (10 buckets) and how long the
	// breaker stays open. Default 30s.
	ResetTimeout time.Duration
	// IsFailure classifies the code of a failed call. Default: UNAVAILABLE,
	// DEADLINE_EXCEEDED, INTERNAL, UNKNOWN and DATA_LOSS, the codes that say
	// the downstream is unwell. A call cancelled by its caller never counts.
	IsFailure func(codes.Code) bool
	// OnStateChange is called with "closed", "open" or "half-open". State
	// changes are also logged.
	OnStateChange func(from, to string)
}

// DefaultIsFailure is the default BreakerConfig.IsFailure.
func DefaultIsFailure(c codes.Code) bool {
	switch c {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.Unknown, codes.DataLoss:
		return true
	}
	return false
}

// errNotFailure marks a call that ended with an error the breaker counts
// as a success (for example NOT_FOUND).
var errNotFailure = errors.New("grpcx: not a breaker failure")

type breaker struct {
	cb        *gobreaker.TwoStepCircuitBreaker[struct{}]
	isFailure func(codes.Code) bool
}

func newBreaker(target string, c BreakerConfig, logger *log.Logger) *breaker {
	reset := cmpOr(c.ResetTimeout, 30*time.Second)
	b := &breaker{isFailure: c.IsFailure}
	if b.isFailure == nil {
		b.isFailure = DefaultIsFailure
	}
	st := gobreaker.Settings{
		Name:         target,
		MaxRequests:  cmpOr(c.SuccessThreshold, 1),
		Interval:     reset,
		BucketPeriod: reset / 10,
		Timeout:      reset,
		IsSuccessful: func(err error) bool { return err == nil || errors.Is(err, errNotFailure) },
		IsExcluded:   func(err error) bool { return errors.Is(err, context.Canceled) },
		OnStateChange: func(_ string, from, to gobreaker.State) {
			logger.Warn("grpcx: circuit breaker", log.String("target", target),
				log.String("from", stateName(from)), log.String("to", stateName(to)))
			if c.OnStateChange != nil {
				c.OnStateChange(stateName(from), stateName(to))
			}
		},
	}
	if c.FailureRatio > 0 {
		ratio, minReq := c.FailureRatio, cmpOr(c.MinRequests, 10)
		st.ReadyToTrip = func(n gobreaker.Counts) bool {
			return uint64(n.Requests) >= minReq && float64(n.TotalFailures) >= ratio*float64(n.Requests)
		}
	} else {
		threshold := cmpOr(c.FailureThreshold, 5)
		st.ReadyToTrip = func(n gobreaker.Counts) bool { return uint64(n.TotalFailures) >= threshold }
	}
	b.cb = gobreaker.NewTwoStepCircuitBreaker[struct{}](st)
	return b
}

// outcome is what the breaker records for a call that ended with err:
// nil and errNotFailure count as successes, context.Canceled (the caller
// gave up) is not counted, anything else is a failure.
func (b *breaker) outcome(ctx context.Context, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled):
		return context.Canceled
	case b.isFailure(Code(err)):
		return err
	default:
		return errNotFailure
	}
}

// allow returns the callback that records the call's outcome, or the error
// returned while the breaker is open (or half-open with its probes in
// flight).
func (b *breaker) allow() (func(error), error) {
	done, err := b.cb.Allow()
	if err != nil {
		return nil, errs.Wrap(ErrCircuitOpen, errs.ErrDependencyUnavailable)
	}
	return done, nil
}

func (b *breaker) unary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	done, err := b.allow()
	if err != nil {
		return err
	}
	err = invoker(ctx, method, req, reply, cc, opts...)
	done(b.outcome(ctx, err))
	return err
}

// stream records whether the stream could be opened. A connection failure
// shows there; the outcome of the rest of a long-lived stream does not
// count.
func (b *breaker) stream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	done, err := b.allow()
	if err != nil {
		return nil, err
	}
	s, err := streamer(ctx, desc, cc, method, opts...)
	done(b.outcome(ctx, err))
	return s, err
}

func stateName(s gobreaker.State) string {
	switch s {
	case gobreaker.StateOpen:
		return "open"
	case gobreaker.StateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}
