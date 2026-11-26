package grpcx_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/grpcx"
	"github.com/dungnh3/kitchen/log"
)

// exampleServer starts an in-memory server with the health service and,
// when check is set, a health service that answers Check with it. It
// returns the dial option to reach it and a stop function. Tests use
// grpcxtest instead.
func exampleServer(check func() error) (grpc.DialOption, func()) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	if check == nil {
		healthpb.RegisterHealthServer(srv, health.NewServer())
	} else {
		healthpb.RegisterHealthServer(srv, &fakeHealth{check: func(context.Context, int32) error { return check() }})
	}
	go func() { _ = srv.Serve(lis) }()
	dial := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })
	return dial, srv.Stop
}

func Example() {
	dial, stop := exampleServer(nil)
	defer stop()

	// One Conn per downstream, built in main and shared. A real target is
	// "dns:///billing.internal:443" with TLS (the default).
	conn, err := grpcx.New("passthrough:///bufnet",
		grpcx.WithInsecure(), // the in-memory server has no TLS
		grpcx.WithDialOptions(dial),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	client := healthpb.NewHealthClient(conn) // generated stubs take a *grpcx.Conn
	res, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	fmt.Println(res.GetStatus(), err)
	// Output: SERVING <nil>
}

func ExampleDefaultErrorMapper() {
	dial, stop := exampleServer(func() error {
		return status.Error(codes.NotFound, "row 42 missing in shard 7") // downstream detail
	})
	defer stop()
	conn, err := grpcx.New("passthrough:///bufnet", grpcx.WithInsecure(), grpcx.WithDialOptions(dial))
	if err != nil {
		panic(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	_, err = healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	fmt.Println(errors.Is(err, errs.ErrNotFound), grpcx.Code(err))
	// What a kitchen server sends its own client: our message, not the
	// downstream's.
	fmt.Println(status.Convert(err).Message())
	// Output:
	// true NotFound
	// not found
}

func ExampleWithErrorMapper() {
	// The downstream's sentinel, from its API package.
	errCardDeclined := errs.New(codes.FailedPrecondition, "CARD_DECLINED", errs.Domain("billing"))
	// Ours: what our clients see.
	errPaymentDeclined := errs.New(codes.FailedPrecondition, "PAYMENT_DECLINED", errs.Message("payment declined"))

	dial, stop := exampleServer(func() error { return errCardDeclined.GRPCStatus().Err() })
	defer stop()
	conn, err := grpcx.New("passthrough:///bufnet", grpcx.WithInsecure(), grpcx.WithDialOptions(dial),
		grpcx.WithErrorMapper(func(se *grpcx.StatusError) error {
			if errors.Is(errs.FromStatus(se.Status), errCardDeclined) {
				return errs.Wrap(se, errPaymentDeclined) // keep se in the chain
			}
			return grpcx.DefaultErrorMapper(se)
		}))
	if err != nil {
		panic(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	_, err = healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	fmt.Println(errors.Is(err, errPaymentDeclined), errs.Code(err))
	// Output: true FailedPrecondition
}

func ExampleWithRetry() {
	attempts := 0
	dial, stop := exampleServer(func() error {
		attempts++
		if attempts < 3 {
			return status.Error(codes.Unavailable, "restarting")
		}
		return nil
	})
	defer stop()
	conn, err := grpcx.New("passthrough:///bufnet", grpcx.WithInsecure(), grpcx.WithDialOptions(dial),
		grpcx.WithRetry(grpcx.RetryPolicy{
			MaxAttempts:    4,                     // first attempt included; gRPC caps it at 5
			InitialBackoff: 10 * time.Millisecond, // random delay in [0, backoff]
			MaxBackoff:     100 * time.Millisecond,
			// Codes: UNAVAILABLE only by default; add others only for
			// idempotent methods.
		}))
	if err != nil {
		panic(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	_, err = healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	fmt.Println(err, attempts)
	// Output: <nil> 3
}

func ExampleWithCircuitBreaker() {
	dial, stop := exampleServer(func() error { return status.Error(codes.Unavailable, "down") })
	defer stop()
	conn, err := grpcx.New("passthrough:///bufnet", grpcx.WithInsecure(), grpcx.WithDialOptions(dial),
		grpcx.WithoutRetry(), // only to keep the example short
		grpcx.WithLogger(log.Discard()),
		grpcx.WithCircuitBreaker(grpcx.BreakerConfig{
			FailureThreshold: 2,                // UNAVAILABLE, DEADLINE_EXCEEDED, INTERNAL, ... count
			ResetTimeout:     30 * time.Second, // window, and how long it stays open
			OnStateChange: func(from, to string) {
				fmt.Println("breaker:", from, "->", to)
			},
		}))
	if err != nil {
		panic(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	client := healthpb.NewHealthClient(conn)
	for range 3 {
		_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	}
	fmt.Println(errors.Is(err, grpcx.ErrCircuitOpen), errs.Code(err))
	// Output:
	// breaker: closed -> open
	// true Unavailable
}

func ExampleWithTokenSource() {
	// A token per call, over TLS (the default).
	conn, err := grpcx.New("dns:///billing.internal:443",
		grpcx.WithTokenSource(grpcx.TokenSourceFunc(func(context.Context) (string, error) {
			return "token-from-your-idp", nil // cache and refresh it in the source
		})))
	if err != nil {
		panic(err)
	}
	_ = conn.Close(context.Background())

	// Credentials over plaintext are refused.
	_, err = grpcx.New("billing:9090", grpcx.WithInsecure(), grpcx.WithTokenSource(grpcx.StaticToken("key")))
	fmt.Println(err)
	// Output: grpcx: credentials (WithTokenSource, WithForwardAuthorization) require TLS; remove WithInsecure
}

func Example_wiring() {
	logger := log.Default() // after log.Setup in main
	billing, err := grpcx.New("dns:///billing.internal:443",
		grpcx.WithLogger(logger),
		grpcx.WithTimeout(3*time.Second), // unary calls without a ctx deadline
		grpcx.WithCircuitBreaker(grpcx.BreakerConfig{}),
		grpcx.WithForwardAuthorization(), // call on behalf of our caller
		// Traces and metrics use the global providers that otelx/sdk.Setup
		// installs; WithTracerProvider / WithMeterProvider override them.
	)
	if err != nil {
		logger.Error("billing client", log.Err(err))
		return
	}
	// Close after the server stopped, with the other dependencies.
	defer func() { _ = billing.Close(context.Background()) }()
	fmt.Println(billing.Target())
	// Output: dns:///billing.internal:443
}
