# Design: transport (`server`, `httpx`, `reqctx`)

Status: implemented on branch `claude/feat-transport-e29ef5`. The owner
approved every decision below on 2026-11-20 ("Đồng ý với tất cả Recommended").
Old packages replaced: `server`, `grpc/gatewayopt`, `grpc/protojson`, `nap`,
`kresty`, `healthcheck`, `context`, and the health part of `pb`/`proto`.
Review of the old code: [docs/review/02-transport-server-grpc-http-context.md](../review/02-transport-server-grpc-http-context.md).
Migration: [docs/migration/transport.md](../migration/transport.md).

## 1. Packages

| Package | Replaces | Purpose |
|---|---|---|
| `server` | `server`, `grpc/gatewayopt`, `grpc/protojson`, `healthcheck`, `pb.HealthService` | gRPC server + grpc-gateway REST + health + admin port, ordered shutdown |
| `reqctx` | `context` | Typed per-request client info, authenticated user id, outgoing propagation, trusted-proxy client IP |
| `httpx` | `nap`, `kresty` | Immutable, concurrency-safe JSON HTTP client with otelhttp and opt-in idempotency-aware retry |

All three live in the core module. No health proto is kept, so there is no
`proto/` directory for this area.

## 2. Downstream facts that drove the design (survey of ~70 consumers)

- 65 repos use `kitchen/server`; 326 calls to conn-based
  `RegisterXxxHandler(ctx, mux, conn)`; 0 use connect or grpc-web.
- ms-note-taker exposes 3 server-streaming RPCs over REST with hand-written
  SSE/NDJSON marshalers.
- 14 repos register plain HTTP handlers (SSE, uploads, a reverse proxy) with
  `HandlePath`. 74 of 88 `RegisterWithCustomHandler` implementations are
  `return nil`.
- 65 repos copy-paste the same `pb.HealthService` implementation. 0 use
  `grpc.health.v1`. Docs say probes hit `/api/status`. The k8s manifests are
  not in the local checkout.
- 26 repos copy the same `CustomJSONMarshaler` (proto names, enum names, emit
  unpopulated, discard unknown). 48 repos use the default error body
  (`google.rpc.Status`). web-admin-portal reads `{code, message, details}`.
- No central gateway strips identity headers: services authenticate
  themselves and write `user-id` into incoming metadata. `User-Id` is in the
  default forwarded-header allowlist.
- nap: JSON and `Receive` everywhere, multipart in 2 repos, XML/form/breaker
  never used. 14 call sites chain on a shared `*nap.Nap` without `Clone`
  (data race).
- `context`: `GetUserID` (115 uses), `AttachUserID` (49), `GetUser` (37),
  platform detection (85). `Attach*ToOutgoing` is never used.
- PGV validation is used in 34 repos; protovalidate in 1 or 2.

## 3. Owner decisions (asked one by one)

| # | Topic | Decision |
|---|---|---|
| 1 | Metrics | OTel only (otelgrpc/otelhttp stats handlers → MeterProvider). No Prometheus client in kitchen; expose an OTel Prometheus exporter on the admin port if scraping is needed. |
| 2 | pprof / metrics exposure | Separate admin port, off by default (`WithAdmin(":9090", ...)`). |
| 3 | Defaults | Payload logging off (opt-in). The library does not handle signals; `Run(ctx)` stops when ctx is cancelled. |
| 4 | Package names | `server`, `httpx`, `reqctx`. |
| 5 | REST ↔ gRPC | Keep grpc-gateway and the conn-based registration, dialled over an **in-process** connection (no TCP loopback). Streaming keeps working; gRPC interceptors run once for both REST and gRPC. |
| 6 | JSON contract | Default marshaler: snake_case (proto names), enum **names**, emit unpopulated, discard unknown. |
| 7 | Error body | A hook plus the default `google.rpc.Status` body. `WithHTTPStatus(func(*status.Status) int)` lets `errs/` map statuses to HTTP codes; `WithHTTPErrorHandler` replaces the handler. |
| 8 | REST streaming | Built-in SSE and NDJSON marshalers, opt-in (`WithStreamingMarshalers`). |
| 9 | Health | Built in: `grpc.health.v1` always on; `/livez` and `/readyz` on the admin port; legacy `/health`, `/ready`, `/api/status`, `/version` on the HTTP port (opt-out). `pb.HealthService` is deleted. |
| 10 | Istio `JOB` hack | Deleted. Use native sidecars. |
| 11 | Header trust | Identity headers are stripped by default on **both** ports. Only trusted peers or an explicit opt-in may set them. |
| 12 | Ports | gRPC (:10000) + HTTP (:8080, h1 + h2c) + optional admin. |
| 13 | Validation | protovalidate first, then the PGV `ValidateAll`/`Validate` fallback; violations are returned as `InvalidArgument` + `BadRequest`. |
| 14 | Service API | One required method (`RegisterGRPC`). Optional `GatewayRegistrar`, `HTTPRegistrar` (stdlib `ServeMux` patterns), `Closer`. |
| 15 | Shutdown | One budget (`WithShutdownTimeout`, default 25s). Order: drain, then Close. |
| 16 | CORS | Off by default; `WithCORS(CORSConfig)` (rs/cors). |
| 17 | HTTP client | Immutable `Client` + a `Request` per call; JSON + multipart only. |
| 18 | Retry | Own small retry policy (idempotent only, full jitter, capped Retry-After); no circuit breaker. |
| 19 | reqctx shape | `Info` struct + `WithUserID`/`UserID` in reqctx; header constants in reqctx; explicit outgoing allowlist; trusted-proxy client IP. |
| 20 | Workers / options | `server.NewAdmin` standalone replaces `healthcheck`; functional options everywhere. |

Decisions 17-20 were accepted as "Recommended" through the owner's blanket
approval rather than an individual answer. They are also listed in §8.

## 4. `server`

```go
srv, err := server.New(
    server.WithServices(svcA, svcB),            // Service: RegisterGRPC(grpc.ServiceRegistrar)
    server.WithAdmin(":9090", server.WithPprof(), server.WithAdminHandler("GET /metrics", promHandler)),
    server.WithTrustedProxies(lbs),              // client IP from X-Forwarded-For / CF-Connecting-IP
    server.WithTrustedIdentityPeers(internal),   // who may send user-id metadata
    server.WithUnaryInterceptors(auth),          // auth calls reqctx.WithUserID
    server.WithHTTPStatus(errs.HTTPStatus),      // provided by the errors-auth area
    server.WithStreamingMarshalers(),
    server.WithShutdownDelay(5*time.Second),
)
err = srv.Run(ctx) // ctx from signal.NotifyContext in main
```

### Topology

```
:10000  public grpc.Server ─┐ (MaxConcurrentStreams, keepalive)
                            ├─ same services (fan-out registrar), same interceptors, same health
in-proc internal grpc.Server┘ (no stream limit) ◄── bufconn ◄── grpc-gateway mux
:8080   http.Server (h1+h2c): legacy probes │ otelhttp → recovery → drain → sanitize → access log → CORS → user mw → ServeMux{HTTPRegistrar…, "/" → body limit → gateway}
:9090   admin (optional): /livez /readyz /api/status /version [/debug/pprof] [extra handlers]
```

The gateway gets its own in-process `grpc.Server` so that all REST traffic,
which shares one connection, is not capped by the per-connection
`MaxConcurrentStreams` that protects the public port.

### gRPC interceptor chain (outermost first)

recovery → drain → request info (strip untrusted identity, build
`reqctx.Info`, generate a request id, propagate a trusted user id) → logging →
user interceptors → validation. The OTel stats handler is always installed and
skips `grpc.health.v1`.

### Identity trust

- HTTP: `Grpc-Metadata-*` headers are always deleted. `User-Id`, `X-User-Id`
  and `X-Kitchen-Client-Ip` are deleted unless the connection peer is in
  `WithTrustedIdentityPeers` or `WithTrustAllIdentityHeaders` is set. Only
  allowlisted headers (`reqctx.ForwardHeaders()` + `WithForwardHeaders`)
  become metadata.
- gRPC: the same keys are removed from incoming metadata unless the peer is
  the in-process gateway (which has already applied the HTTP rule), a trusted
  identity peer, or trust-all is set.
- `reqctx.UserID(ctx)` is set from metadata only for trusted peers. Otherwise
  only the auth interceptor sets it, with `reqctx.WithUserID`.

### Shutdown (one budget)

1. Mark not ready (`/readyz`, `/ready` return 503; grpc health goes
   NOT_SERVING).
2. Wait `WithShutdownDelay`.
3. Close the `Draining(ctx)` channel.
4. Drain HTTP and then the in-process gRPC server; drain the public gRPC
   server concurrently. Force-close at the deadline.
5. Call `Closer.Close(ctx)` on each service with the remaining budget.
6. Stop the admin port.

`Run` returns nil after a ctx-triggered shutdown, or the listener error.

## 5. `reqctx`

- `Info{RequestID, AnonymousUserID, DeviceID, DeviceType, Platform, OSVersion,
  AppVersion, ApplicationID, Localization, Locale, Timezone, Country, ClientIP
  netip.Addr, UserAgent}`. Read it with `FromContext`.
- `WithUserID` / `UserID`. `Get(ctx, key)` returns the first metadata value
  and never joins repeated values. `BearerToken(ctx)`.
- `TrustedProxies.ClientIP` walks X-Forwarded-For from the right, skipping
  trusted hops. It honours CF-Connecting-IP only from a trusted hop.
- `AppendToOutgoing` and the client interceptors send an explicit allowlist:
  user id, request id, client info and client IP. Incoming `authorization` is
  never copied.

## 6. `httpx`

- `New(baseURL, opts...)` returns an immutable `*Client`. Defaults: 30s
  timeout, otelhttp transport, 100 idle connections per host.
- Per-call builder: `c.Post(path).Header().Bearer().Query().JSON()/Body()/Multipart().Do(ctx, &out)`,
  or `.Send(ctx)` for a raw response.
- Any non-2xx response returns `*StatusError` with a bounded `Body` and a
  `Decode` method. `httpx.StatusCode(err)` returns the code.
- `RetryPolicy`: retries only idempotent methods or requests that carry
  `Idempotency-Key`, and only with a rewindable body. It retries transport
  errors and 429/502/503/504, uses exponential backoff with full jitter, caps
  Retry-After, and never sleeps past the ctx deadline.

## 7. Cross-area interfaces

- **errors-auth (`errs/`, `auth/`)**: `server` imports neither. `errs`
  provides an `HTTPStatusFunc` (for example one that reads its ErrorInfo
  detail) and an interceptor that converts errors to statuses. `auth`
  provides an interceptor that calls `reqctx.WithUserID`.
- **observability (`log/`, `otel/`)**: `server` takes a `*slog.Logger` and
  OTel providers or propagators; it does not import those packages.
- **sse (`sse/`)**: SSE handlers that need drain notification should use
  `server.Draining(r.Context())` through `HTTPRegistrar`.

## 8. Decisions taken by default (to review)

The owner told this area to take the recommended option, or the option best
for system design and security, without asking. Each one below can be
revisited.

| Decision | Chosen | Alternatives (pros / cons) |
|---|---|---|
| HTTP client API (#17) | Immutable Client + per-call Request | Generic `PostJSON[T]` helpers: type-safe, but API sprawl for multipart and query. resty v3: feature-rich, but still rc. |
| Retry (#18) | Own ~150-line policy, no breaker | failsafe-go: retry + breaker + hedge, but pre-1.0 and a core dependency. go-retryablehttp: MPL-2.0, retries POST, buffers bodies. |
| reqctx shape (#19) | Info + UserID in reqctx | Principal in `auth/`: cleaner boundary, but 115 call sites would depend on an unmerged area and transport could not propagate. Keep `User` struct: least churn, but keeps re-parsing and trusting client headers. |
| Workers / options (#20) | `NewAdmin` + functional options | Config struct: easy env loading, but hooks are funcs anyway. Keep `healthcheck`: less churn, duplicate logic. |
| Default max message size | 4 MiB on both gRPC servers, the gateway conn and the gateway body limit | Old: client 50 MB, server 4 MB (mismatch → uploads > 4 MB failed). A bigger default costs memory per request at 30K connections; raise it with `WithMaxMessageSize`. |
| HTTPRegistrar body limit | None (handlers own it, e.g. `http.MaxBytesReader`) | Applying the gateway limit would break uploads. |
| Separate in-process gRPC server | Yes, without a stream limit | One server: simpler, but every REST request shares one connection capped at `MaxConcurrentStreams` (1000). |
| HTTP server timeouts | ReadHeaderTimeout 10s, IdleTimeout 650s (> GCP LB 600s), no read/write timeout | Old: idle 0 (never reaped), which holds idle connections forever at 30K scale. |
| h2c on the HTTP port | On (`http.Protocols`) | HTTP/1.1 only: old behaviour, no multiplexing for internal callers. |
| Log levels | gRPC: OK=Debug, client codes=Info, Unavailable etc.=Warn, server faults=Error. HTTP: 5xx=Error, 4xx=Info, else Debug | Old: every gRPC call at Info, HTTP ≥300 at Error (304 and 404 too). Success at Info is too noisy at 30K rps. |
| grpc.health.v1 vs readiness checks | gRPC health reflects lifecycle only; checks run on `/readyz` and `/ready` | Running checks per gRPC probe: no caching, and probes would hammer dependencies. |
| `/api/status` | Liveness (always 200 while serving) | Readiness. The old Status RPC returned the liveness payload. |
| Public `/ready` | No failure details | Same body as admin `/readyz`, which would leak dependency errors publicly. |
| `Info.Platform` | X-Platform when it is a known value, else detected from User-Agent | Two fields. Fewer call sites need to pick. |
| Dropped from Info | Lat/long (0 readers), secrets (API key, App Check, refresh token, CSRF, nonce) | Keep them in Info. Secrets in a struct get logged by accident; read them with `reqctx.Get`. |
| Removed `mileusna/useragent` | Device type comes from the X-Device-Type header only | UA-based `DetectDeviceType` had 0 users. |
| CF-Connecting-IP | Honoured only when the immediate peer is a trusted proxy | Old: always honoured, so it was spoofable. |
| Identity keys | `user-id`, `x-user-id`, `x-kitchen-client-ip` | Could add `anonymous-user-id`, but it is client-generated by design. |
| `httpx` idle connections | 100 per host | Stdlib default 2 causes connection churn under load. |
| Multipart | Streamed through a pipe, never retried | Buffered: retryable, but holds whole uploads in memory. |
| Shutdown delay default | 0 (use ~5s on Kubernetes) | 5s default slows tests and non-k8s binaries. |
| protovalidate in core | Yes (pulls cel-go) | Submodule: lighter core, but one more wiring step that is easy to forget. |
| pprof import | `net/http/pprof` handlers mounted on the admin mux | Its `init` also registers on `http.DefaultServeMux`; kitchen never serves that mux. |
| REST span name | `METHOD /route/pattern` from the gateway route | Method only for HTTPRegistrar routes, which can set it themselves. |

## 9. Not done / follow-ups

- golangci-lint v2 is not installed locally, so `make lint` was not run.
  CI runs it.
- Health proto and buf v2 config: none needed now that `pb.HealthService` is
  deleted. The errors proto is owned by errors-auth.
- Prometheus-name compatibility for dashboards: the metric names change
  (`rpc.server.*`, `http.server.*`). Dashboards and alerts must be updated by
  their owners.
