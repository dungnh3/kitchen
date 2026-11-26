# Design: `grpcx` (outbound gRPC client) and `zen add client`

Status: accepted. The owner chose the four recommended options in one popup
(TLS, retry, error mapping, zen). Migration notes are not needed: kitchen had
no outbound gRPC client before.

## 1. Why

Most services call sibling services over gRPC, and kitchen has the pieces
for it but nothing that assembles them: `otelx.GRPCClientHandler` (stats
handler), `xcontext.UnaryClientInterceptor` / `StreamClientInterceptor`
(request id, user id and the other propagated values) and
`errs.FromStatus`. Without a canonical client every service builds its own
`grpc.NewClient` call, and each one gets TLS, retries, deadlines, keepalive
and error mapping slightly wrong. `grpcx` is that canonical client (D7), the
gRPC counterpart of `httpx`.

## 2. API

```go
conn, err := grpcx.New("dns:///billing.internal:443",
	grpcx.WithLogger(logger),
	grpcx.WithTokenSource(tokens),      // per-RPC bearer token (TLS only)
	grpcx.WithCircuitBreaker(grpcx.BreakerConfig{}),
)
defer conn.Close(ctx)
billing := billingv1.NewBillingServiceClient(conn) // *grpcx.Conn is a grpc.ClientConnInterface
```

- `New(target string, opts ...Option) (*Conn, error)`: functional options,
  like `httpx`. It never dials: `grpc.NewClient` connects lazily, so a
  service starts while its downstream is down and readiness is unaffected.
- `*Conn` implements `grpc.ClientConnInterface` (`Invoke`, `NewStream`) and
  has `Close(ctx) error`, `Target() string` and `State()` for diagnostics.
- `StatusError` is the downstream status kept as the cause of every
  returned error; `Status(err)` and `Code(err)` read it.
- `ErrorMapper` / `WithErrorMapper` replace `DefaultErrorMapper`.
- `grpcx/grpcxtest` starts a real `grpc.Server` on `bufconn` and returns a
  `*grpcx.Conn` to it, with every default on except TLS.

### Options

| Option | Default |
|---|---|
| `WithTLSConfig(*tls.Config)` | TLS 1.2+ with the system roots |
| `WithInsecure()` | off: plaintext must be asked for |
| `WithTimeout(d)` | 10s deadline for unary calls whose ctx has none; streams get none |
| `WithRetry(RetryPolicy{})` / `WithoutRetry()` | on: UNAVAILABLE, 3 attempts, 100ms→1s, throttled |
| `WithCircuitBreaker(BreakerConfig{})` | off (as in `httpx`) |
| `WithKeepalive(keepalive.ClientParameters)` | ping after 30s idle, 10s timeout, only with active RPCs |
| `WithLoadBalancing(policy)` | `round_robin` |
| `WithMaxMessageSize(n)` | 4 MiB send and receive, as `server` |
| `WithTokenSource(TokenSource)` | none; refused with `WithInsecure` |
| `WithForwardAuthorization()` | off; refused with `WithInsecure` |
| `WithErrorMapper(m)` | `DefaultErrorMapper` |
| `WithUnaryInterceptors`, `WithStreamInterceptors` | none; run inside kitchen's |
| `WithDialOptions(...grpc.DialOption)` | none; the escape hatch, applied last |
| `WithLogger`, `WithTracerProvider`, `WithMeterProvider`, `WithPropagator` | `log.Default()`, OTel globals |

### Interceptor order (outermost first)

1. default deadline (unary only);
2. circuit breaker (when enabled);
3. error mapping (status → `errs`);
4. `xcontext` propagation;
5. per-call credentials (forwarded bearer);
6. the service's own interceptors.

The OTel stats handler sits below every interceptor and records each
attempt; gRPC's own retries run below the interceptors too, so the breaker
and the error mapper see the result after retries.

## 3. Decisions

| # | Decision | Chosen | Alternatives | Why |
|---|---|---|---|---|
| G1 | TLS | Required by default (system roots, TLS 1.2+); plaintext only with `WithInsecure()` | Plaintext by default in cluster, trusting a mesh | Fail closed, like the database defaults. Without a mesh, plaintext would carry tokens and PII in clear. A mesh deployment says so explicitly once per client (`<SERVICE>_INSECURE=true` in zen). |
| G2 | Retry | Service config: every method, `UNAVAILABLE` only, 3 attempts, backoff 100ms→1s ×2, retry throttling (10 tokens, ratio 0.1) | Per-method idempotent list; no retry | grpc-go enables retries but has no default policy. UNAVAILABLE almost always means the request was not processed (connection refused, draining, GOAWAY). Throttling stops retry storms when a downstream is down. The retry is gRPC's own (it replays buffered messages, works for streams before the first response), so it does not use `timex` (D7 backoff) — the formula lives in grpc-go and kitchen only sets its parameters. |
| G3 | Error mapping | As `httpx`: NOT_FOUND → `ErrNotFound`; DEADLINE_EXCEEDED → `ErrUpstreamTimeout`; UNAVAILABLE, RESOURCE_EXHAUSTED, open breaker → `ErrDependencyUnavailable` (downstream RetryInfo kept); CANCELLED → `ErrCanceled`; everything else → `ErrInternal` | Keep client-fault codes; everything → `ErrDependencyUnavailable` | One rule for every outbound call. A downstream PERMISSION_DENIED or INVALID_ARGUMENT is usually our own bug or credential, not the caller's. The downstream message never reaches our clients: the status is only the cause (`StatusError`, logs). `WithErrorMapper` plus `errs.FromStatus` map a downstream reason to a local sentinel when a service needs it. |
| G4 | zen | `zen add client <service>` | Docs only | Every service that calls another one gets the same wiring: config, TLS, timeout, close order, env docs. |
| G5 | Load balancing | `round_robin` in the default service config | `pick_first` (grpc default) | `pick_first` pins every call of a process to one backend. With a `dns:///` target and a headless Kubernetes service, `round_robin` spreads calls over all pods. A ClusterIP target still works (one address). |
| G6 | Constructor | `grpc.NewClient` (lazy) | `grpc.Dial` / `DialContext` (deprecated) | `Dial` is deprecated and blocks or hides connection errors; `NewClient` uses the `dns` resolver by default and never blocks. |
| G7 | Keepalive | Ping after 30s without activity, 10s timeout, `PermitWithoutStream: false` | grpc default (no pings); pings without streams | Detects dead connections (a node that vanished) within ~40s during calls. 30s is above `server`'s enforcement `MinTime` (10s); without streams no pings are sent, so servers with grpc's stricter default policy do not answer with `too_many_pings`. |
| G8 | Default deadline | 10s for unary calls without a ctx deadline | None; 30s as `httpx` | A call without a deadline can hang forever on a stuck downstream. Internal gRPC calls are short; services raise it per client. Streams are long-lived and only follow ctx. |
| G9 | Credentials | `TokenSource` per RPC and `WithForwardAuthorization`, both refused with `WithInsecure` (`New` returns an error) | Allow tokens over plaintext | A bearer token over plaintext is readable by anyone on the path. Forwarding sends only a single well-formed `Bearer` value from the incoming metadata, never other incoming metadata. |
| G10 | Circuit breaker | Opt-in, `BreakerConfig` with the same fields as `httpx`, per `Conn` (one target); counts UNAVAILABLE, DEADLINE_EXCEEDED, INTERNAL, UNKNOWN, DATA_LOSS (a call cancelled by its caller never counts, as in `httpx`); window of `ResetTimeout` in 10 buckets; implemented with `sony/gobreaker/v2` inside `grpcx` | Custom code; a new canonical breaker package | Same behaviour and names as `httpx` for the user. gobreaker is small, dependency-free and widely used; it stays an implementation detail (no gobreaker type in the API), and D7 bans it outside `grpcx`. |
| G11 | Message size | 4 MiB send and receive | grpc default (4 MiB receive, unlimited send) | Matches `server`, so a request we can send is one a kitchen server accepts. |

## 4. Error mapping in detail

`DefaultErrorMapper(se *StatusError) error`:

| Downstream code | Returned | Client sees |
|---|---|---|
| NOT_FOUND | `errs.Wrap(se, errs.ErrNotFound)` | NotFound, our message |
| DEADLINE_EXCEEDED | `errs.ErrUpstreamTimeout` | DeadlineExceeded |
| UNAVAILABLE, RESOURCE_EXHAUSTED | `errs.ErrDependencyUnavailable` + downstream RetryInfo | Unavailable |
| CANCELLED | `errs.ErrCanceled` | Canceled |
| anything else | `errs.ErrInternal` | Internal |

`StatusError` keeps the method and the downstream status (code, message,
details) and, when the call ended because the caller's ctx was cancelled or
expired, unwraps to `ctx.Err()` so `errors.Is(err, context.Canceled)` works.
`StatusError` has no `GRPCStatus` method on purpose: an unmapped
`StatusError` (`WithErrorMapper(nil)`) is sanitised to INTERNAL by
`errs.Handler` instead of forwarding the downstream status.

## 5. D7

New row: "Outbound gRPC client → `grpcx`; not allowed outside it:
`grpc.NewClient`, `grpc.Dial`, `grpc.DialContext`, sony/gobreaker". forbidigo
forbids the three constructors outside `grpcx/**` and `server/**` (the
gateway's in-process loopback); tests may still call them. The zen service
template's `.golangci.yml` gets the same rule.

## 6. `zen add client <name>`

Runs in a service made by `zen new service`; `<name>` is the downstream
service, lower-case words with dashes (`billing`, `user-profile`). It:

- adds `config.Client` fields (`<NAME>_ADDR` required, `<NAME>_TIMEOUT`
  default 10s, `<NAME>_INSECURE` default false, `<NAME>_CA_FILE` optional)
  under `Config.Clients`, with validation;
- writes `internal/app/client_<name>.go` that opens the `*grpcx.Conn`
  (logger, telemetry providers, timeout, TLS with the optional CA file, the
  circuit breaker on);
- opens it in `openDeps` and closes it with the other deps; readiness is not
  changed (a lazily connecting client does not make the service unready);
- adds the `<NAME>_*` rows to `docs/ENV.md` and test values to the config
  and app tests.

The service then builds its generated stub from `d.<name>`, for example
`billingv1.NewBillingServiceClient(d.billing)`. New markers in the service
template: `zen:clients` (config), `zen:clients-validate`, `zen:clients-env`,
`zen:clients-test`, `zen:client-fields` and `zen:client-open` (deps) and the
`docs/ENV.md` table. Services generated before these markers get the usual
"add the change by hand" error.

## 7. What it does not do

- No service registry or discovery beyond gRPC name resolution (`dns:///`,
  Kubernetes headless services). No xDS.
- No code generation of stubs: services keep using `buf generate` on the
  downstream's protos.
- No service mesh integration: with a mesh, use `WithInsecure()` (the mesh
  does mTLS) and let the mesh do its own retries or turn `grpcx`'s off.
- No hedging, no client-side rate limiting, no per-method timeouts (set a
  ctx deadline per call).
- No connection pooling across targets: one `Conn` per downstream, shared
  by the whole process.
