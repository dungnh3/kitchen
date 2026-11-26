# grpcx

`grpcx` is kitchen's client for calling other services over gRPC. Every
outbound gRPC connection goes through it, not through `grpc.NewClient`. It
is the canonical package for gRPC clients
([D7](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22)); the
design is in [docs/design/grpcx.md](../docs/design/grpcx.md).

## Install

```sh
go get github.com/dungnh3/kitchen
```

`grpcx` and `grpcx/grpcxtest` live in the core module.

## Quick start

Build one `*grpcx.Conn` per downstream service in `main` and share it. It
implements `grpc.ClientConnInterface`, so generated stubs take it as is.

```go
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
```

`New` never dials. The first call connects, so a service starts while its
downstream is down, and its readiness does not depend on it.

The defaults are safe:

- **TLS** 1.2+ with the system roots. Plaintext needs `WithInsecure()`.
- **Credentials need TLS.** `WithTokenSource` and `WithForwardAuthorization`
  are refused with `WithInsecure()`, so a token never travels in clear.
- **round_robin** over every address the resolver returns. Use a
  `dns:///` target and a headless Kubernetes service so calls spread over
  all pods (the gRPC default, `pick_first`, pins one pod).
- **Retries** of `UNAVAILABLE` for every method, 3 attempts, backoff
  100ms→1s, with retry throttling.
- **Deadline.** A unary call whose ctx has no deadline gets 10s. Streams
  only follow their ctx.
- **Keepalive.** A ping after 30s without activity while calls are active;
  the connection is dropped when it is not answered within 10s.
- **Messages** up to 4 MiB in both directions, as kitchen's `server`.
- **Errors are `errs` errors**, and the downstream's message never reaches
  your own clients (see below).
- **Propagation and tracing.** Request id, user id and the other
  `kitchen/context` values go out as metadata; calls are traced through
  `otelx.GRPCClientHandler`. Incoming metadata such as `authorization` is
  never copied.
- **User-Agent** starts with `kitchen-grpcx`.

The circuit breaker is opt-in.

## Configuration

| Option | Default | When to change it |
|---|---|---|
| `WithTLSConfig(c)` | TLS 1.2+, system roots | A private CA (`RootCAs`), client certificates, a server name. |
| `WithInsecure()` | off | A mesh does mTLS for you, or a local server. |
| `WithTimeout(d)` | 10s for unary calls without a ctx deadline | Slower methods; `0` turns it off. Per call: a ctx deadline. |
| `WithRetry(RetryPolicy{})` | `UNAVAILABLE`, 3 attempts, 100ms→1s | More attempts, or more codes for idempotent methods. |
| `WithoutRetry()` | | The mesh retries already, or calls must never repeat. |
| `WithCircuitBreaker(BreakerConfig{})` | off | A downstream that fails for minutes at a time. |
| `WithKeepalive(p)` | 30s / 10s, only with active calls | A server with a stricter ping policy. |
| `WithLoadBalancing(policy)` | `round_robin` | `pick_first` for a single address you want to stick to. |
| `WithMaxMessageSize(n)` | 4 MiB | Known large messages (raise the server's limit too). |
| `WithTokenSource(ts)`, `StaticToken(k)` | none | The downstream wants a bearer token from this service. |
| `WithForwardAuthorization()` | off | Call the downstream on behalf of your caller. |
| `WithErrorMapper(m)` | `DefaultErrorMapper` | A downstream reason needs a meaning of its own. |
| `WithUnaryInterceptors`, `WithStreamInterceptors` | none | Your own interceptors; they run inside kitchen's. |
| `WithDialOptions(...)` | none | Anything else (a custom dialer). Applied last; it can override every default. |
| `WithLogger(l)` | `log.Default()` | Pass the service's `*log.Logger`. |
| `WithTracerProvider`, `WithMeterProvider`, `WithPropagator` | OTel globals | Tests, or a non-global provider. |

`grpcx` reads no environment variables. `zen add client` generates the
`<NAME>_ADDR`, `<NAME>_TIMEOUT`, `<NAME>_INSECURE` and `<NAME>_CA_FILE`
settings for you.

## Integration with kitchen

### Logger, telemetry and close order

```go
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
```

Close a Conn after the server has stopped serving, so in-flight requests
can still call the downstream. Do not add a readiness check on it: a
downstream that is down must not take this service out of the load
balancer.

### Errors

`DefaultErrorMapper` follows the same rule as `httpx`:

| Downstream code | errs error | Our client sees |
|---|---|---|
| NOT_FOUND | `errs.ErrNotFound` | NotFound |
| DEADLINE_EXCEEDED | `errs.ErrUpstreamTimeout` | DeadlineExceeded |
| UNAVAILABLE, RESOURCE_EXHAUSTED, open breaker | `errs.ErrDependencyUnavailable` (with the downstream RetryInfo) | Unavailable |
| CANCELLED | `errs.ErrCanceled` | Canceled |
| anything else | `errs.ErrInternal` | Internal |

A downstream INVALID_ARGUMENT, PERMISSION_DENIED or UNAUTHENTICATED is our
request or our credential, not our caller's, so it is Internal. The
downstream message stays in the cause, a `*grpcx.StatusError`, for logs:

```go
_, err = healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
fmt.Println(errors.Is(err, errs.ErrNotFound), grpcx.Code(err))
// What a kitchen server sends its own client: our message, not the
// downstream's.
fmt.Println(status.Convert(err).Message())
```

Handlers can return these errors as they are. Read the downstream status
with `grpcx.Status(err)`, `grpcx.Code(err)` or
`errors.AsType[*grpcx.StatusError](err)`. When a call ended because its ctx
was cancelled, `errors.Is(err, context.Canceled)` is true.

### Authentication

`WithTokenSource` sends `authorization: Bearer <token>` on every call. The
source is called per call: cache the token and refresh it before it
expires inside the source.

```go
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
```

A token source error fails the call without sending it, as an
`errs.ErrInternal` (its message stays in the cause).

`WithForwardAuthorization` forwards the incoming bearer token of the call
being served (exactly one well-formed `Bearer` value). An outgoing
`authorization` the caller set itself wins. Nothing else from the incoming
metadata is forwarded.

### Server wiring

A service opens its Conns with its other dependencies, passes the generated
stubs to its handlers and closes the Conns after `srv.Run` returns. For the
full service setup, see [server/USAGE.md](../server/USAGE.md);
`zen add client <name>` generates this wiring.

## Advanced

### Retries

```go
conn, err := grpcx.New("passthrough:///bufnet", grpcx.WithInsecure(), grpcx.WithDialOptions(dial),
	grpcx.WithRetry(grpcx.RetryPolicy{
		MaxAttempts:    4,                     // first attempt included; gRPC caps it at 5
		InitialBackoff: 10 * time.Millisecond, // random delay in [0, backoff]
		MaxBackoff:     100 * time.Millisecond,
		// Codes: UNAVAILABLE only by default; add others only for
		// idempotent methods.
	}))
```

- Retries are gRPC's own, set through the default service config. gRPC
  replays the buffered request, so a stream is retried too until its first
  response arrives.
- `UNAVAILABLE` almost always means the downstream did not process the
  request (connection refused, draining, GOAWAY). A server that returns it
  after a write gets the write twice: such methods need an idempotency key.
- Retry throttling: each failure takes a token from a budget of 10, each
  success gives back 0.1, and below 5 tokens gRPC stops retrying. A down
  downstream does not get a retry storm.
- The ctx deadline bounds the call, retries included.
- Behind a mesh that retries already, use `WithoutRetry()` so attempts do
  not multiply.

### Circuit breaker

Use it when a downstream can be down for minutes, so callers fail fast
instead of waiting on timeouts. One Conn has one breaker.

```go
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
```

While open, calls return `errs.ErrDependencyUnavailable` wrapping
`grpcx.ErrCircuitOpen` without being sent. The fields are those of
`httpx.BreakerConfig`: `FailureRatio` and `MinRequests` for a ratio breaker,
`IsFailure` to choose the counted codes (default `DefaultIsFailure`). A
call cancelled by its caller never counts. For a stream, only opening it
counts. State changes are logged at warn level.

### Mapping a downstream reason

```go
// The downstream's sentinel, from its API package.
errCardDeclined := errs.New(codes.FailedPrecondition, "CARD_DECLINED", errs.Domain("billing"))
// Ours: what our clients see.
errPaymentDeclined := errs.New(codes.FailedPrecondition, "PAYMENT_DECLINED", errs.Message("payment declined"))

conn, err := grpcx.New("passthrough:///bufnet", grpcx.WithInsecure(), grpcx.WithDialOptions(dial),
	grpcx.WithErrorMapper(func(se *grpcx.StatusError) error {
		if errors.Is(errs.FromStatus(se.Status), errCardDeclined) {
			return errs.Wrap(se, errPaymentDeclined) // keep se in the chain
		}
		return grpcx.DefaultErrorMapper(se)
	}))
```

Map to your own sentinel; never return the downstream's error as is, or
its message reaches your clients. `WithErrorMapper(nil)` returns the bare
`*StatusError`, which `errs.Handler` sanitises to Internal.

### Behind a service mesh

**Warning:** `WithInsecure()` sends calls in clear. It is safe only when a
mesh sidecar encrypts the traffic (mTLS) or for a local server. Without a
mesh, use TLS with your CA in `WithTLSConfig`. Credentials are refused over
plaintext; with a mesh, let the mesh authenticate the workloads.

## Testing

`grpcx/grpcxtest` starts a real gRPC server on an in-memory listener and
returns a Conn to it, with every kitchen default on:

```go
srv := grpcxtest.NewServer(t, func(r grpc.ServiceRegistrar) {
	billingv1.RegisterBillingServiceServer(r, fake)
})
client := billingv1.NewBillingServiceClient(srv.Conn(t, grpcx.WithLogger(log.Discard())))
```

- `NewTLSServer` serves TLS with a certificate made for the test; its Conn
  trusts it, so `WithTokenSource` and `WithForwardAuthorization` work.
- The server and the Conn are closed with `t.Cleanup`.
- Fakes may return `status.Error(...)` in tests; assert with
  `errors.Is(err, errs.ErrNotFound)`, `errs.Code(err)` and `grpcx.Code(err)`.
- Use `WithoutRetry()` or a small `InitialBackoff` in retry tests.

## Observability

- **Spans and metrics:** OTel gRPC client spans and metrics from
  `otelx.GRPCClientHandler`, one span per attempt, with trace context
  injected. The providers are the OTel globals unless set with
  `WithTracerProvider` / `WithMeterProvider`.
- **Logs:** only circuit breaker state changes (`grpcx: circuit breaker`,
  with `target`, `from`, `to`). Add an interceptor for per-call logs.
- **errs reasons:** `NOT_FOUND`, `UPSTREAM_TIMEOUT`,
  `DEPENDENCY_UNAVAILABLE`, `CANCELLED` and `INTERNAL`.

## Production checklist

- One Conn per downstream, built in main, shared, closed after the server.
- A `dns:///` target and a headless Kubernetes service, so round_robin
  sees every pod.
- TLS unless a mesh does mTLS; credentials only over TLS.
- A ctx deadline per call, or a `WithTimeout` that fits the downstream.
- A circuit breaker for downstreams that can be down for long.
- Map downstream reasons to your own sentinels, never pass them through.
- No readiness check on a downstream.
