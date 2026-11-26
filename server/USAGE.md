# server

`server` runs a gRPC service, its REST gateway, plain HTTP routes, health
probes and an admin port in one process. It owns the interceptor chain:
recovery, request info, logging, errors, auth, rate limits and validation.
Every request goes through that chain once, over gRPC and REST alike.

`server` is the canonical package for HTTP and gRPC serving
([D7](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22)). Do not
call `grpc.NewServer` or `http.ListenAndServe` yourself; the D7 lint forbids
both. This file is also the hub for wiring a whole service: the
[Integration](#integration-with-kitchen) section shows every kitchen package
together, and the other USAGE files link here.

## Install

```sh
go get github.com/dungnh3/kitchen
```

`server` is in the core module; `auth`, `otelx/sdk` and `ratelimit/redis` are separate modules.

## Quick start

A service implements `RegisterGRPC`. It may also implement `RegisterHTTP`
for plain routes, `RegisterGateway` for the generated REST mapping, and
`Close` for shutdown:

```go
type files struct{}

func (files) RegisterGRPC(grpc.ServiceRegistrar) {} // pb.RegisterFilesServer(r, f)

func (files) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/files/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "42" {
			errs.WriteHTTP(w, r, errs.ErrNotFound) // AIP-193 JSON, 404
			return
		}
		_, _ = io.WriteString(w, "file 42")
	})
}

func (files) Close(context.Context) error { return nil } // flush, close clients
```

`main` sets up the logger, builds the server and runs it until SIGTERM:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

opts, err := log.OptionsFromEnv()
logger, _ := log.Setup(opts)
if err != nil {
	logger.WarnContext(ctx, "invalid log config", log.Err(err))
}

srv, err := server.New(server.WithServices(files{}))
if err != nil {
	logger.ErrorContext(ctx, "server", log.Err(err))
	return
}
if err := srv.Run(ctx); err != nil {
	logger.ErrorContext(ctx, "server", log.Err(err))
}
```

What you get with no options:

- gRPC on `:10000` with `grpc.health.v1`. HTTP on `:8080` (HTTP/1.1, h2c)
  for the REST gateway, `RegisterHTTP` routes, `/livez` and `/readyz`.
- One error format: every gRPC error, gateway error, unknown route, panic and
  oversized body becomes a `google.rpc.Status`. On HTTP it is the AIP-193
  JSON envelope. Internal errors never reach clients (see
  [errs](../errs/USAGE.md)).
- Request validation (protovalidate) on every RPC. Panics become `INTERNAL`.
- An access log through `log.Default()`, OTel spans and metrics through the
  global providers.
- Safe parsing: gateway bodies and gRPC messages limited to 4 MiB, gRPC
  headers to 64 KiB, `ReadHeaderTimeout` 10s.
- `X-Forwarded-For` is ignored and client identity headers are stripped,
  until you name trusted peers.
- Graceful shutdown within 25s: not ready, drain, then `Close`.

## Configuration

| Option | Default | When to change it |
|---|---|---|
| `WithGRPCAddr`, `WithHTTPAddr` | `:10000`, `:8080` | Other ports. `""` disables a port (`WithGRPCAddr("")` gives an HTTP-only server). |
| `WithAdmin(addr, ...)` | off | Always in production: probes with details, `/version`, pprof, metrics. Bind it to a private address. |
| `WithErrors(h)` / `WithI18n(b)` | plain `errs.Handler` | Set a `Domain` and a localizer. Use one or the other, not both. |
| `WithAuth`, `WithRateLimit` | off | Protected APIs. See Integration. |
| `WithTrustedProxies(p)` | none | Behind a load balancer or CDN: the client IP comes from `X-Forwarded-For` only through these. |
| `WithTrustedIdentityPeers(p)` | none | Internal callers that forward `user-id` and other identity metadata. |
| `WithReadiness(checks...)` | none | Gate `/readyz` on your dependencies. |
| `WithShutdownTimeout` | 25s | Keep it below the pod's `terminationGracePeriodSeconds`. |
| `WithShutdownDelay` | 0 | About 5s on Kubernetes, so the load balancer stops routing first. |
| `WithMaxMessageSize` | 4 MiB | Larger uploads over gRPC or REST. |
| `WithMaxConcurrentStreams` | 1000 | Many long streams per connection. |
| `WithCORS(cfg)` | off | A browser app on another origin. |
| `WithCompression()` | off | No CDN or ingress compresses in front of you. |
| `WithStreamingMarshalers()` | off | Server streams over REST as SSE or NDJSON. |
| `WithGatewayBodyTimeout(d)` | 30s | A REST body must arrive within `d`, or 408. Raise it for large JSON uploads from slow networks; `0` disables it. `HTTPRegistrar` routes are not affected. |
| `WithGRPCServerOptions(o...)` | none | Public gRPC port only (`grpc.Creds` for TLS, `keepalive`). Never reaches the in-process server behind REST. |
| `WithHTTPServerConfig(f)` | see godoc | `http.Server` timeouts; setting `TLSConfig` (with a certificate) makes the HTTP port serve TLS (h1 + h2). |
| `WithForwardHeaders(h...)` | `xcontext.DefaultForwardHeaders` | More HTTP headers to reach gRPC metadata and `xcontext.Info`. |
| `WithLogger`, `WithTracerProvider`, `WithMeterProvider`, `WithPropagator` | process logger, OTel globals | Tests, or a second server with its own telemetry. |

The server reads no env vars; `log` reads `LOG_*` and `otelx/sdk` `OTEL_*`.

## Integration with kitchen

This is the full wiring of a service. It is compiled from
`server/example/service_example_test.go`, a small module of its own so that
it can use `auth`, `otelx/sdk` and `ratelimit/redis`.

```go
func run() error {
	// 1. Shutdown on SIGTERM (Kubernetes) or Ctrl-C.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. One process logger from LOG_* env vars. Every kitchen package logs
	// through it (log.Default) unless given another *log.Logger.
	opts, envErr := log.OptionsFromEnv()
	logger, err := log.Setup(opts)
	if err != nil {
		return err
	}
	if envErr != nil {
		logger.WarnContext(ctx, "invalid log config, using defaults", log.Err(envErr))
	}

	// 3. Telemetry (OTEL_* env vars). The server, httpx, pgx and redisx use
	// the global providers it installs. Logs also go to OTLP.
	tel, err := sdk.Setup(ctx, sdk.Config{ServiceName: "agents", ServiceVersion: "1.0.0"})
	if err != nil {
		return err
	}
	defer func() { _ = tel.Shutdown(context.WithoutCancel(ctx)) }()
	opts.Handlers = append(opts.Handlers, tel.SlogHandler("agents"))
	if _, err := log.Setup(opts); err != nil {
		return err
	}

	// 4. Dependencies, opened with their canonical packages.
	db, err := pgx.Open(ctx, pgx.Config{URL: os.Getenv("DATABASE_URL")})
	if err != nil {
		return err
	}
	defer db.Close()
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{os.Getenv("REDIS_ADDR")}})
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	// 5. Localized errors: one AIP-193 format for gRPC, REST and plain HTTP.
	bundle, err := i18n.New(language.English, i18n.WithDefaults())
	if err != nil {
		return err
	}
	eh := errs.NewHandler(errs.Config{Localizer: bundle, Domain: "agents.example.com"})

	// 6. Authentication (fail-closed): tokens from the OIDC provider.
	jwks, err := auth.NewJWKS(ctx, auth.JWKSConfig{
		Issuer:   "https://login.example.com",
		Audience: "agents.example.com",
		URL:      "https://login.example.com/.well-known/jwks.json",
	})
	if err != nil {
		return err
	}
	defer func() { _ = jwks.Close(context.WithoutCancel(ctx)) }()
	authn, err := auth.New(auth.Config{Verifiers: []auth.Verifier{jwks}})
	if err != nil {
		return err
	}

	// 7. Rate limits shared by every instance (Redis GCRA), per user when
	// signed in, per client IP otherwise.
	store, err := rlredis.New(rdb)
	if err != nil {
		return err
	}
	limiter, err := ratelimit.New(ratelimit.Config{
		Store:     ratelimit.RemoteStore(store),
		Instances: 3, // replicas; each enforces 1/3 of a rule while Redis is down
		Default:   &ratelimit.Policy{Limits: ratelimit.DefaultLimits()},
	})
	if err != nil {
		return err
	}

	// 8. The client IP is only taken from X-Forwarded-For set by these.
	proxies, err := xcontext.NewTrustedProxies("10.0.0.0/8")
	if err != nil {
		return err
	}

	// 9. The server: gRPC on :10000, REST gateway and probes on :8080,
	// admin port :9090, readiness gated on Postgres and Redis.
	srv, err := server.New(
		server.WithServices( /* your services */ ),
		server.WithErrors(eh),
		server.WithAuth(authn),
		server.WithRateLimit(limiter),
		server.WithTrustedProxies(proxies),
		server.WithReadiness(
			server.Check{Name: "postgres", Func: db.Ping},
			server.Check{Name: "redis", Func: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
		),
		server.WithAdmin(":9090"),
		server.WithCompression(),
		server.WithShutdownDelay(5*time.Second),
	)
	if err != nil {
		return err
	}
	// Run returns after a graceful drain once ctx is cancelled.
	return srv.Run(ctx)
}
```

How each piece plugs in (workers without a server use `server.NewAdmin`):

- **log**: the server, its errs handler and its access log use
  `log.Default()`. Records logged with a request context carry `request_id`
  and `user_id`. See [log](../log/USAGE.md).
- **errs and i18n**: return `errs` errors from handlers. The handler renders
  them on every transport and localizes `LocalizedMessage` from
  `Accept-Language`. See [errs](../errs/USAGE.md) and [i18n](../i18n/USAGE.md).
- **auth**: `WithAuth` checks gRPC, the REST gateway and `RegisterHTTP`
  routes. It is fail-closed: a route the authenticator does not list as public
  needs a valid token. It also sets the user id for logs and rate limits.
  `WithUserIDFrom` does it for your own middleware.
- **ratelimit**: `WithRateLimit` runs after auth, so signed-in callers are
  limited per user and anonymous ones per client IP. See
  [ratelimit](../ratelimit/USAGE.md).
- **context**: `xcontext.FromContext(ctx)` gives the request id, client IP,
  language and headers in any handler ([context](../context/USAGE.md)).
- **otelx**: spans and metrics use the `otelx/sdk` providers. For Prometheus,
  pass `server.WithAdminHandler("GET /metrics", h)` to `WithAdmin`.
- **pgx, redisx, mongox**: a failing `Ping` check turns `/readyz` and
  `grpc.health.v1` to `NOT_SERVING` (cached 1s, bounded by a timeout).

## Advanced

### HTTP-only services

An MCP endpoint or a webhook receiver needs no public gRPC port. It keeps
probes, drain, logs and tracing:

```go
srv, err := server.New(
	server.WithGRPCAddr(""),
	server.WithServices(files{}),
)
```

### Browser clients

```go
srv, err := server.New(
	server.WithServices(files{}),
	server.WithCORS(server.CORSConfig{
		AllowedOrigins: []string{"https://app.example.com"},
		AllowedHeaders: []string{"Authorization", "Content-Type"},
		MaxAge:         10 * time.Minute,
	}),
	server.WithCompression(),         // when no CDN or ingress compresses
	server.WithStreamingMarshalers(), // server streams as SSE or NDJSON
)
```

List exact origins; with `AllowCredentials`, only origins you control.

### Long-lived streams and shutdown

`Run` shuts down in order: mark not ready, wait the shutdown delay, close
`server.Draining`, drain HTTP and gRPC, then call each `Close`. Streams must
end themselves when draining starts, or they are cut at the timeout:

```go
watch := func(ctx context.Context, send func(string) error) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-server.Draining(ctx):
			return errs.ErrServiceDraining // UNAVAILABLE: retry elsewhere
		case <-tick.C:
			if err := send("tick"); err != nil {
				return err
			}
		}
	}
}
```

REST streams end with the same AIP-193 envelope as every other error: SSE
sends `event: error` with `data: {"error": {...}}`, NDJSON and the default
JSON stream send a `{"error": {...}}` line (results stay bare messages in
NDJSON and `{"result": ...}` in JSON). The HTTP status of the response is
already sent by then; read `error.code` and `error.status`.

### Load balancing gRPC connections

gRPC connections live forever by default, so after a scale-out the new pods
get no traffic from clients that are already connected (Kubernetes Service,
L4 load balancers). Rotate connections with a maximum age longer than your
longest stream, plus a grace that lets in-flight streams finish:

```go
server.WithKeepalive(
	keepalive.ServerParameters{
		Time: 60 * time.Second, Timeout: 20 * time.Second, // keep the defaults
		MaxConnectionAge: 30 * time.Minute, MaxConnectionAgeGrace: 5 * time.Minute,
	},
	keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true},
)
```

It is off by default because a stream longer than age + grace is cut
(UNAVAILABLE, the client retries).

### TLS without a mesh

```go
server.WithGRPCServerOptions(grpc.Creds(credentials.NewServerTLSFromCert(&cert))),
server.WithHTTPServerConfig(func(s *http.Server) {
	s.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
}),
```

The REST gateway keeps reaching gRPC in process, without TLS.

### High load and large payloads

Raise `WithMaxMessageSize` and `WithMaxConcurrentStreams` (see
Configuration), and use `WithHTTPServerConfig` for `http.Server` timeouts.
Add a `ReadTimeout` only when no route streams: REST gateway bodies are
already bounded by `WithGatewayBodyTimeout`, and the public gRPC port waits
at most 20s for a new connection's handshake. The compiled example is
`Example_tuning` in `server/usage_example_test.go`.

### Service-to-service identity and other safety switches

Identity metadata (`user-id` and the other `xcontext.IdentityHeaders`) is
kept only from `WithTrustedIdentityPeers`; from anyone else it is stripped and
a warning is logged at most once a minute. A trusted peer's
`X-Kitchen-Client-Ip` is the client IP on both ports.

> **Warning:** `WithTrustAllIdentityHeaders` lets any client claim to be any
> user; use it only when neither port is reachable by untrusted clients.
> `WithoutValidation` lets malformed requests reach your handlers.
> `WithPayloadLogging` writes request bodies, which may hold secrets and
> personal data, to the log. Keep all three out of production.

## Testing

Run the real server on ephemeral ports. Every interceptor then runs exactly
as in production:

```go
ctx, cancel := context.WithCancel(context.Background())
grpcLis, _ := net.Listen("tcp", "127.0.0.1:0")
httpLis, _ := net.Listen("tcp", "127.0.0.1:0")
srv, err := server.New(
	server.WithServices(files{}),
	server.WithGRPCListener(grpcLis),
	server.WithHTTPListener(httpLis),
	server.WithLogger(log.Discard()),
)
if err != nil {
	panic(err)
}
done := make(chan error, 1)
go func() { done <- srv.Run(ctx) }()

base := "http://" + httpLis.Addr().String()
for _, path := range []string{"/v1/files/42", "/v1/files/7", "/readyz"} {
	resp, err := http.Get(base + path)
	if err != nil {
		panic(err)
	}
	_ = resp.Body.Close()
	fmt.Println(path, resp.StatusCode)
}
cancel()
fmt.Println("run:", <-done)
// Output:
// /v1/files/42 200
// /v1/files/7 404
// /readyz 200
// run: <nil>
```

- For gRPC, dial `grpcLis.Addr()` with `grpc.NewClient`. Assert errors with
  `errors.Is` or `errs.Code`; decode HTTP bodies as in
  [errs](../errs/USAGE.md#testing).
- Check for leaked goroutines with `goleak` after `Run` returns.

## Observability

- **Spans:** otelgrpc on the public port and otelhttp on the HTTP port, via
  `otelx`. HTTP spans are named after the method and carry `http.route`.
  Health and reflection RPCs are not traced.
- **Metrics:** otelgrpc and otelhttp server metrics (`rpc.server.*`,
  `http.server.*`). Rate limits add their own (see ratelimit).
- **Access log:** one record per RPC (`rpc.method`,
  `rpc.grpc.status_code`, `duration`, `error`) and per HTTP request
  (`http.request.method`, `http.response.status_code`,
  `http.response.body.size`, `client.address`). The level follows the code:
  OK at Debug, caller mistakes (`INVALID_ARGUMENT`, `NOT_FOUND`, ...) at Info,
  `UNAVAILABLE`, `PERMISSION_DENIED` and similar at Warn, the rest at Error.
- **Probes:** `/livez` and `/readyz` on the HTTP port (no details), and with
  failure details on the admin port. `/version` is on the admin port.
- **errs reasons it returns:** `SERVICE_DRAINING` during shutdown,
  `PAYLOAD_TOO_LARGE` (413), `UNSUPPORTED_MEDIA_TYPE` (415),
  `MALFORMED_REQUEST` for bodies the gateway cannot parse, `NOT_FOUND` for
  unknown routes, and `INTERNAL` for panics.

## Production checklist

- `log.Setup` and `sdk.Setup` before `server.New`; `signal.NotifyContext`
  for SIGTERM (the server does not handle signals). Call its `stop` once
  `ctx` is done, so a second Ctrl-C kills a shutdown that hangs.
- If `main` fails between `server.New` and `Run`, call `srv.Close()` (it
  releases the gateway connection; a running server needs no Close).
- `WithShutdownDelay` about 5s; `WithShutdownTimeout` below
  `terminationGracePeriodSeconds`; long streams select on `server.Draining`.
- `WithAdmin` on a private address, with Kubernetes probes pointed at it.
- `WithTrustedProxies` listing every hop, or client IPs (and per-IP rate
  limits) are the load balancer's.
- A readiness check per dependency; an errs `Domain` and a localizer.
- No `WithTrustAllIdentityHeaders`, `WithoutValidation` or
  `WithPayloadLogging`.

## Migrating from the old kitchen

The old `server`, `grpc`, `nap` and `healthcheck` packages map to this one.
See [docs/migration/transport.md](../docs/migration/transport.md).
