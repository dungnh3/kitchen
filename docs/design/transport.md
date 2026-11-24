# Design: transport (`server`, `httpx`, `context`)

Status: implemented on branch `claude/feat-transport-e29ef5`. The owner
approved every decision below on 2026-11-20 ("Đồng ý với tất cả Recommended").
Old packages replaced: `server`, `grpc/gatewayopt`, `grpc/protojson`, `nap`,
`kresty`, `healthcheck`, `context`, and the health part of `pb`/`proto`.
Migration: [docs/migration/transport.md](../migration/transport.md).

## 1. Packages

| Package | Replaces | Purpose |
|---|---|---|
| `server` | `server`, `grpc/gatewayopt`, `grpc/protojson`, `healthcheck`, `pb.HealthService` | gRPC server + grpc-gateway REST + health + admin port, ordered shutdown |
| `context` | `context` | Typed per-request client info, authenticated user id, outgoing propagation, trusted-proxy client IP |
| `httpx` | `nap`, `kresty` | Own client type over resty v3 (hidden): safe defaults, errs mapping, hooks, retry, breaker, otelx tracing |

All three live in the core module. No health proto is kept, so there is no
`proto/` directory for this area.

## 2. Downstream facts that drove the design (survey of ~70 consumers)

- 65 repos use `kitchen/server`; 326 calls to conn-based
  `RegisterXxxHandler(ctx, mux, conn)`; 0 use connect or grpc-web.
- One service exposes 3 server-streaming RPCs over REST with hand-written
  SSE/NDJSON marshalers.
- 14 repos register plain HTTP handlers (SSE, uploads, a reverse proxy) with
  `HandlePath`. 74 of 88 `RegisterWithCustomHandler` implementations are
  `return nil`.
- 65 repos copy-paste the same `pb.HealthService` implementation. 0 use
  `grpc.health.v1`. Docs say probes hit `/api/status`. The k8s manifests are
  not in the local checkout.
- 26 repos copy the same `CustomJSONMarshaler` (proto names, enum names, emit
  unpopulated, discard unknown). 48 repos use the default error body
  (`google.rpc.Status`). An admin web client reads `{code, message, details}`.
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
| 4 | Package names | `server`, `httpx`, `context`. |
| 5 | REST ↔ gRPC | Keep grpc-gateway and the conn-based registration, dialled over an **in-process** connection (no TCP loopback). Streaming keeps working; gRPC interceptors run once for both REST and gRPC. |
| 6 | JSON contract | Default marshaler: snake_case (proto names), enum **names**, emit unpopulated, discard unknown. |
| 7 | Error body | `server` uses `errs` natively (owner, 2026-11-20): one `errs.Handler` (`WithErrors`, default `errs.NewHandler` with the server logger) converts gRPC errors, REST gateway errors, unknown routes, panics and oversized bodies into the AIP-193 envelope. `WithHTTPStatus` was removed (HTTP status comes from the code, errs rule E2; oversized bodies are 413); `WithHTTPErrorHandler` still replaces the gateway handler. |
| 8 | REST streaming | Built-in SSE and NDJSON marshalers, opt-in (`WithStreamingMarshalers`). |
| 9 | Health | Built in: `grpc.health.v1` always on; `/livez`, `/readyz` (details) and `/version` on the admin port; `/livez`, `/readyz` (no details) on the HTTP port (`WithoutPublicProbes` to drop). The old library's paths (`/health`, `/ready`, `/api/status`, public `/version`) are gone (owner, 2026-11-20). Readiness checks are cached for 1s and shared by concurrent probes. |
| 10 | Istio `JOB` hack | Deleted. Use native sidecars. |
| 11 | Header trust | Identity headers are stripped by default on **both** ports. Only trusted peers or an explicit opt-in may set them. |
| 12 | Ports | gRPC (:10000) + HTTP (:8080, h1 + h2c) + optional admin. |
| 13 | Validation | protovalidate first, then the PGV `ValidateAll`/`Validate` fallback; violations are returned as `InvalidArgument` + `BadRequest`. |
| 14 | Service API | One required method (`RegisterGRPC`). Optional `GatewayRegistrar`, `HTTPRegistrar` (stdlib `ServeMux` patterns), `Closer`. |
| 15 | Shutdown | One budget (`WithShutdownTimeout`, default 25s). Order: drain, then Close. |
| 16 | CORS | Off by default; `WithCORS(CORSConfig)` (rs/cors). |
| 17 | HTTP client | Immutable `Client` + a `Request` per call; JSON + multipart only. |
| 18 | Retry | Own small retry policy (idempotent only, full jitter, capped Retry-After); no circuit breaker. |
| 19 | context shape | `Info` struct + `WithUserID`/`UserID` in `context`; header constants in `context`; explicit outgoing allowlist; trusted-proxy client IP. |
| 20 | Workers / options | `server.NewAdmin` standalone replaces `healthcheck`; functional options everywhere. |
| 21 | Compression | Off by default; `WithCompression()` (owner, 2026-11-21) gzips HTTP responses ≥ 1 KiB for clients that accept gzip, with `klauspost/compress/gzhttp` (Vary, flush, Content-Encoding pass-through). SSE, NDJSON, raster images, fonts, audio, video and archives are sent as is. Prefer the CDN/LB/ingress when it compresses (AWS ALB and ingress-nginx's defaults do not). gRPC keeps its per-call compression. |
| 22 | Kitchen packages by default | The server is wired to kitchen's own packages (owner, 2026-11-21): without `WithLogger` it logs with `kitchen/log` from the environment (`log.OptionsFromEnv`, zap, platform schema, stdout) with `xcontext.LogAttrs`, and `New` fails on an invalid `LOG_*` value; `WithI18n(*i18n.Bundle)` localizes the default `errs.Handler` (exclusive with `WithErrors`). `WithAuth(Authenticator)` (an interface `*auth.Authenticator` implements, so jwx stays out of core) adds the auth interceptors after the errs interceptor, keeps the gRPC health service public and authenticates `HTTPRegistrar` routes by their matched pattern (fail-closed); see errors-auth A8. `WithRateLimit(*ratelimit.Limiter)` adds the limiter after auth (gRPC, REST gateway, `HTTPRegistrar` routes by pattern; health exempt) and passes its headers through the gateway; see errors-auth R4/R5. |
| 23 | Reuse of kitchen packages | (owner, 2026-11-21) The server's OpenTelemetry instrumentation is `otelx.GRPCServerHandler` / `otelx.HTTPHandler` (one filter for kitchen: health and reflection are not traced). `redisx.Open` enables `ContextTimeoutEnabled`, so its clients work with `ratelimit/redis` and honor the short deadlines of `ratelimit` and `cache`. |
| 24 | Build vs reuse | (owner, 2026-11-21: check mature libraries before writing code) `context.TrustedProxies` wraps realclientip-go's `RightmostTrustedRangeStrategy` (reference implementation, MIT, no deps, last release 2022 but complete) behind kitchen's netip API; `NewTrustedProxiesHeader` adds RFC 7239 `Forwarded`. When every hop is trusted or the deciding entry is malformed, the peer address is the client (was: a guessed hop). `ratelimit`'s RateLimit headers are encoded with dunglas/httpsfv (RFC 9651). Kept custom, with reasons: the GCRA Lua store and `LocalStore` (throttled/v2 needs ≥ 2 Redis round trips with CAS retries, its memstore resets active buckets, and it pulls old Redis clients), and `PlatformFromUserAgent` (3 platforms; UA libraries are unmaintained or ship a large regex database). |
| 25 | Review fixes (2026-11-21) | grpc.health.v1 `Watch` streams end with UNAVAILABLE when draining starts (one open Watch used to hold `GracefulStop` until the deadline and leave `Close` an expired ctx). Draining uses at most 80% of `WithShutdownTimeout`, so `Closer.Close` always gets at least 20%. A panicking readiness check is a failure (not a process crash, since `/readyz` is public), and a check that ignores its ctx is reported as timed out without holding readiness. The REST gateway forwards the HTTP side's `xcontext.UserID` to the in-process call (and no longer forwards a raw `User-Id` header), so `WithUserIDFrom`/auth on HTTP reaches gateway handlers. The public gRPC port accepts at most 64 KiB of metadata (`grpc.MaxHeaderListSize`, grpc-go default 16 MiB). An oversized chunked REST body is 413 `PAYLOAD_TOO_LARGE` like a declared one. Kept: plain HTTP routes are matched twice (`mux.Handler` then `ServeHTTP`) because only `ServeHTTP` sets `r.Pattern`/`PathValue`. |
| 26 | HTTP-only server | (owner, 2026-11-21, for mcpkit) `WithGRPCAddr("")` disables the public gRPC port, like `WithHTTPAddr("")` disables HTTP; the in-process gRPC server behind the REST gateway still runs. `New` fails when both ports are disabled. An HTTP-only server keeps probes, drain, access log, auth/rate limit on `HTTPRegistrar` routes and tracing. |

Decisions 17-20 were accepted as "Recommended" through the owner's blanket
approval rather than an individual answer. They are also listed in §8.

## 4. `server`

```go
srv, err := server.New(
    server.WithServices(svcA, svcB),            // Service: RegisterGRPC(grpc.ServiceRegistrar)
    server.WithAdmin(":9090", server.WithPprof(), server.WithAdminHandler("GET /metrics", promHandler)),
    server.WithTrustedProxies(lbs),              // client IP from X-Forwarded-For
    server.WithTrustedIdentityPeers(internal),   // who may send user-id metadata
    server.WithErrors(eh),                       // errs.Handler: one error format everywhere
    server.WithUnaryInterceptors(authn.UnaryServerInterceptor()),
    server.WithUserIDFrom(auth.SubjectFromContext), // copies the subject into xcontext.UserID
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
:8080   http.Server (h1+h2c): /livez /readyz │ otelhttp → recovery → drain → sanitize → access log → compression → CORS → user mw → user id → ServeMux{HTTPRegistrar…, "/" → body limit → gateway}
:9090   admin (optional): /livez /readyz /version [/debug/pprof] [extra handlers]
```

The gateway gets its own in-process `grpc.Server` so that all REST traffic,
which shares one connection, is not capped by the per-connection
`MaxConcurrentStreams` that protects the public port.

### gRPC interceptor chain (outermost first)

recovery → request info (strip untrusted identity, build `xcontext.Info`,
generate a request id, propagate a trusted user id) → drain → logging →
errors (`errs.Handler`) → auth (`WithAuth`, health exempt) → rate limit
(`WithRateLimit`, health exempt) → user interceptors → user id
(`WithUserIDFrom`) → validation (protovalidate via `errs/validation`, PGV). Logging sits outside the
error conversion so it records the final code; a panic is converted by the
recovery interceptor itself. The OTel stats handler is always installed and
skips `grpc.health.v1`.

### Identity trust

- HTTP: `Grpc-Metadata-*` headers are always deleted. `User-Id`
  and `X-Kitchen-Client-Ip` are deleted unless the connection peer is in
  `WithTrustedIdentityPeers` or `WithTrustAllIdentityHeaders` is set. Only
  allowlisted headers (`xcontext.DefaultForwardHeaders()` + `WithForwardHeaders`)
  become metadata.
- gRPC: the same keys are removed from incoming metadata unless the peer is
  the in-process gateway (which has already applied the HTTP rule), a trusted
  identity peer, or trust-all is set.
- `xcontext.UserID(ctx)` is set from metadata only for trusted peers. Otherwise
  only the auth interceptor sets it, with `xcontext.WithUserID`.

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

## 5. `context`

Reviewed and slimmed with the owner (2026-11-20): kitchen owns only generic
request facts; client conventions of a particular app are captured headers.

- `Info{RequestID, ClientIP netip.Addr, UserAgent, AcceptLanguage, Platform,
  AppVersion, DeviceID, Timezone, Headers map[string]string}`. `Headers`
  holds the first value of every allowlisted header (`DefaultForwardHeaders()`
  plus `server.WithForwardHeaders`), never identity headers or credentials
  (Authorization, cookies); values over 4 KiB are dropped.
- Build: `FromHTTP(r, Options)` / `FromMetadata(md, Options)` with
  `Options{TrustedProxies, Headers}`. Read: `FromContext`, `Info.Header(name)`,
  `Header(ctx, name)` (same answer on gRPC, gateway and plain HTTP).
- `WithUserID` / `UserID` is the transport-level identity (trusted peers,
  propagation, logs). auth keeps its own `Principal` and stores the
  subject with `WithUserID` itself.
- `TrustedProxies.ClientIP` walks X-Forwarded-For (or `Forwarded` with
  `NewTrustedProxiesHeader`) from the right, skipping trusted hops, with
  realclientip-go (decision 24). CF-Connecting-IP is not used (list the Cloudflare ranges as
  trusted proxies instead). `AddrFromHostPort` parses peer addresses.
- `Platform` is `ios`, `android` or `web`: X-Platform when known, else
  `PlatformFromUserAgent`.
- `AppendToOutgoing` and the client interceptors send an explicit allowlist:
  user id, request id, client IP, Accept-Language, platform, app version,
  device id, time zone. Incoming `authorization` is never copied.
- The package is named `context` like the old one; import it as `xcontext`.
- Removed (no generic use): `DeviceType`, `ApplicationID`, `AnonymousUserID`,
  `OSVersion`, `Localization`, `Locale` (now the `X-Locale` captured header),
  `Country` (client-controlled), `PlatformHuawei`, `PlatformOf`,
  `BearerToken` (auth owns credentials), the `X-User-Id` alias, and the
  app-specific header constants (API key, vendor key, App Check, CSRF, nonce,
  refresh token, lat/long).

## 6. `httpx`

Revised 2026-11-21 (group-3 review, owner): `httpx` is built on **resty v3**
(`resty.dev/v3`, pinned at `v3.0.0-rc.4`, move to `v3.0.0` when released),
hidden behind httpx's own types: no resty type appears in the API, so the
engine can change without breaking callers. 19 services use resty v2
directly and 16 use `nap`; both migrate to this one client.

- `httpx.New(opts...) (*httpx.Client, error)`; the client is immutable and
  shared. Per call: `c.Get/Post/Put/Patch/Delete/Head(path)` →
  `Header`, `Bearer`, `BasicAuth`, `Query`, `QueryValues`, `PathParam`
  (escaped), `JSON`, `Body`, `Form`, `FormField`, `File`, `MultipartField`,
  `IdempotencyKey`, `Timeout`, `MaxResponseBody`, `ErrorInto` →
  `Do(ctx, &out) error` (JSON or `*[]byte`) or `Send(ctx) (*Response, error)`
  for streams.
- Extension points (owner): `WithRequestHook(func(*Request) error)`,
  `WithResponseHook(func(*Response) error)`,
  `WithTransportMiddleware(func(http.RoundTripper) http.RoundTripper)`,
  and policy overrides below.

| Rule | Default | Override |
|---|---|---|
| `OriginGuard` (RequestHook) | on with a base URL: an absolute URL on another origin fails with `ErrDestinationNotAllowed` | `WithoutOriginGuard()` |
| `SafeRedirectPolicy(10)` | ≤10 hops, no https→http, cross-origin hops keep only Accept*, Content-Type/Length, User-Agent | `WithRedirectPolicy(p)`, `NoRedirects` |
| `SSRFControl` / `PublicAddr` | off; `WithSSRFGuard(SSRFConfig{})` refuses non-public IPs (after DNS) and ports other than 80/443, ignores proxy env | `SSRFConfig.Allow`, `.Ports` |
| error mapping | non-2xx → `errs` error wrapping `*StatusError` (64 KiB body): 404→NotFound, 408/504→`ErrUpstreamTimeout`, 429/502/503→`ErrDependencyUnavailable` (+RetryInfo), else (401/403 too)→Internal | `WithErrorMapper(fn)`, `nil` = bare `*StatusError` |
| transport errors | timeout→`ErrUpstreamTimeout`, network→`ErrDependencyUnavailable`; cancel and TLS errors unchanged | — |
| `RetryPolicy` | off; when on: `timex.Backoff` delays, idempotent methods or `IdempotencyKey`, network/timeout/429/502/503/504, Retry-After capped at 30s, one-shot readers never replayed | `RetryPolicy.Retryable` |
| `BreakerConfig` | off; when on: count (5 failures / 30s) or ratio mode, 5xx and transport errors count; open → `ErrDependencyUnavailable` wrapping `ErrCircuitOpen` | `IsFailure`, `OnStateChange` |
| response body limit | 32 MiB | `WithMaxResponseBody`, `Request.MaxResponseBody(0)` |
| cookie jar | none (a shared client must not replay one caller's cookies) | — |
| redaction | query values and passwords redacted in errors | `RedactURL` |
| telemetry, logs | `otelx.HTTPTransport`; logs (including resty's) to `*log.Logger` from kitchen/log; User-Agent `kitchen-httpx` | `WithTracerProvider`, `WithLogger`, `WithHeader` |

- Per-attempt timeout 30s (`WithTimeout`), 100 idle connections per host
  (`WithTransportSettings`).
- Upstream errors reach our own callers through `errs`, so `errs.Handler`
  answers with a sensible code and never leaks the upstream body; a vendor
  401 is our Internal.
- Known limit: otelhttp's `url.full` span attribute only redacts the query
  keys the OTel semantic conventions list.

### Gateway parse errors (group-3 review, 2026-11-21)

A body, query or path parameter the gateway cannot parse reaches the error
handler as a bare InvalidArgument status (generated code wraps the parser
error). Services always return an ErrorInfo (errs interceptor), so a bare one
is the gateway's own: `gatewayError` renders it as `errs.ErrMalformedRequest`
(400, localized) with the parser message (≤200 bytes) in metadata `detail`.
This replaces the old `grpc/protojson` `Gea*` prefix wrappers.

## 7. Cross-area interfaces

- **errors-auth (`errs/`, `auth/`)**: `server` imports `errs` (a core leaf)
  and uses one `errs.Handler` for every transport. `auth` is a separate module
  that core cannot import: its interceptor goes in `WithUnaryInterceptors`
  and `WithUserIDFrom(auth.SubjectFromContext)` bridges the subject into
  `xcontext.UserID`.
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
| context shape (#19) | Info + UserID in `context` | Principal in `auth/`: cleaner boundary, but 115 call sites would depend on an unmerged area and transport could not propagate. Keep `User` struct: least churn, but keeps re-parsing and trusting client headers. |
| Workers / options (#20) | `NewAdmin` + functional options | Config struct: easy env loading, but hooks are funcs anyway. Keep `healthcheck`: less churn, duplicate logic. |
| Default max message size | 4 MiB on both gRPC servers, the gateway conn and the gateway body limit | Old: client 50 MB, server 4 MB (mismatch → uploads > 4 MB failed). A bigger default costs memory per request at 30K connections; raise it with `WithMaxMessageSize`. |
| HTTPRegistrar body limit | None (handlers own it, e.g. `http.MaxBytesReader`) | Applying the gateway limit would break uploads. |
| Separate in-process gRPC server | Yes, without a stream limit | One server: simpler, but every REST request shares one connection capped at `MaxConcurrentStreams` (1000). |
| HTTP server timeouts | ReadHeaderTimeout 10s, IdleTimeout 650s (> GCP LB 600s), no read/write timeout | Old: idle 0 (never reaped), which holds idle connections forever at 30K scale. |
| h2c on the HTTP port | On (`http.Protocols`) | HTTP/1.1 only: old behaviour, no multiplexing for internal callers. |
| Log levels | gRPC: OK=Debug, client codes=Info, Unavailable etc.=Warn, server faults=Error. HTTP: 5xx=Error, 4xx=Info, else Debug | Old: every gRPC call at Info, HTTP ≥300 at Error (304 and 404 too). Success at Info is too noisy at 30K rps. |
| grpc.health.v1 vs readiness checks | gRPC health reflects lifecycle only; checks run on `/readyz` and `/ready` | Running checks per gRPC probe: no caching, and probes would hammer dependencies. |
| Public probe paths | `/livez`, `/readyz` (owner, 2026-11-20) | The old library's `/health`, `/ready`, `/api/status`, `/version`: compatibility with the old library, and a public version leak. |
| Public `/readyz` | No failure details; checks cached 1s and shared by concurrent probes (owner, 2026-11-20) | Same body as admin `/readyz`, which would leak dependency errors; running checks per probe let anyone multiply the load on dependencies (measured: 200 probes = 200 DB checks). |
| `Info.Platform` | X-Platform when it is a known value, else detected from User-Agent | Two fields. Fewer call sites need to pick. |
| Info scope | Generic fields + captured allowlisted headers (owner, 2026-11-20); app conventions and secrets are not fields | Keep the old mobile-client fields: one app's conventions in a general library. |
| Removed `mileusna/useragent` and device type | No device-type field; platform from X-Platform or the User-Agent | A UA parser library for one enum. |
| CF-Connecting-IP | Not used; Cloudflare ranges go in the trusted-proxy list (owner, 2026-11-20) | Honour it from a trusted hop: spoofable by calling the LB directly. Old: always honoured. |
| Untrusted identity metadata | Dropped, with a Warn log at most once a minute (owner, 2026-11-20) | Silent drop: S2S callers lose the user id unnoticed. Trust all: impersonation. |
| Identity keys | `user-id`, `x-kitchen-client-ip` (the `x-user-id` alias was dropped, owner 2026-11-20) | An alias doubles the keys to strip and to trust. |
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

## Review of 2026-11-21 (group 2, `healthcheck` → `server`)

| # | Finding | Decision (owner) |
|---|---|---|
| H1 | `grpc.health.v1` stayed SERVING while `WithReadiness` checks failed, so gRPC probes (Kubernetes `grpc:` probes, Envoy, GCP load balancers) kept routing to an instance `/readyz` reported unready. | While running, the server status ("") follows the readiness checks every second (cached results, no extra load on dependencies); shutdown still switches it to NOT_SERVING first. |

## Deep review of 2026-11-24 (`server`)

Every line of the package was read; each finding has a regression test that
failed before the fix (`server/hardening_test.go`).

| # | Finding | Severity | Decision |
|---|---|---|---|
| S1 | `WithGRPCServerOptions` also configured the in-process server; `grpc.Creds` made every REST call 503. | High | Fixed (owner): the options apply to the public port only. |
| S2 | `WithHTTPServerConfig` documented TLS, but `Run` called `Serve`, so a `TLSConfig` was ignored and the port served plaintext. | High | Fixed (owner): a `TLSConfig` serves TLS (h1 + h2 by ALPN); one without a certificate fails in `New`. |
| S3 | Duplicate gRPC service, or an implementation of the wrong type: grpc-go `logger.Fatalf` exited the process inside `New`. A conflicting `HTTPRegistrar` pattern (or `"/"`) panicked. | Medium | Fixed: `New` returns an error. |
| S4 | A panic after the response started ended it cleanly: the client took a truncated body (or SSE stream) as complete. | Medium | Fixed: re-panic `http.ErrAbortHandler` (connection or stream aborted), as net/http and httputil do. |
| S5 | REST stream errors were a bare `google.rpc.Status` (gRPC code, no `status`), indistinguishable from a result in NDJSON; gateway-made stream errors were not sanitized. | Medium | Fixed (owner): AIP-193 envelope in SSE, NDJSON and JSON streams; non-errs errors sanitized. Wire change, see migration. |
| S6 | Wrong method on a gateway path: 501, and an ERROR log with stack trace per request (scanner noise). | Medium | Fixed (owner): 405 UNIMPLEMENTED, no ERROR log. 404 and 400 routing errors are kitchen sentinels too. Reason `METHOD_NOT_ALLOWED` needs an errs/i18n catalog entry first (follow-up). |
| S7 | Slow-body DoS: no read deadline (streams forbid `ReadTimeout`), so a REST body sent one byte a minute held a handler forever; gRPC handshakes waited 120s. | Medium | Fixed (owner): `WithGatewayBodyTimeout` (30s, 408) on gateway routes only, via a connection read deadline cleared at body EOF (so longer RPCs are not cancelled by net/http's background read); `grpc.ConnectionTimeout(20s)` on the public port. |
| S8 | `Run` with a ctx cancelled before the serve goroutines reached `Serve` returned "grpc: the server has been stopped". | Low | Fixed: `grpc.ErrServerStopped` is ignored. |
| S9 | `New` without `Run` leaked 3 grpc-go goroutines (gateway connection) with no way to release them. | Low | Fixed (owner): `Server.Close()`; `Run` after it returns `ErrClosed`. |
| S10 | With readiness checks, `grpc.health.v1` said SERVING from `Run` until the first check run finished (up to 5s with a hanging dependency). | Low | Fixed: NOT_SERVING until the checks pass. |
| S11 | A trusted peer's `X-Kitchen-Client-Ip` was used on gRPC but ignored on HTTP. | Low | Fixed (owner): same rule on both ports. |
| S12 | Hot path: 24 allocations per unary call in the interceptor chain (peer address formatted then parsed, metadata keys lower-cased per call). | Perf | Fixed: 15 allocations, 1176 B (`BenchmarkUnaryChain`). |
| S13 | No `MaxConnectionAge`: connections never rebalance after scale-out. | Note | Kept off (owner): long streams would be cut; USAGE shows how to enable. |
| S14 | A readiness run in progress when shutdown starts is detached from cancellation, so `Run` may return up to 5s (check timeout) after shutdown ends; a check that ignores its ctx outlives `Run`. | Note | Deferred: bounded and documented in `Check`. |
| S15 | A stream error before the first message gets grpc-gateway's code→HTTP status (not errs' 413/415 exceptions). Errors net/http writes itself (bad request line, 431) are not AIP-193. | Note | Deferred: outside the server's control. |

Comparison with mature servers: grpc-go's keepalive example sets
`MaxConnectionAge` and an enforcement policy (kitchen: enforcement on, age
opt-in because of long streams); go-kratos runs one interceptor/middleware
chain per transport (kitchen runs one chain for both through the in-process
connection, so auth, rate limit and validation cannot drift); connect-go
serves gRPC and JSON from one `http.Handler` with a read-size limit
(`ReadMaxBytes`; kitchen: `WithMaxMessageSize` plus the body deadline);
grpc-gateway's defaults (501 for a wrong method, bare `google.rpc.Status`
stream errors, `DefaultHeaderMatcher` forwarding every `Grpc-Metadata-*`
header) are all overridden; Google's API design guide (AIP-193) is the
error body everywhere, streams included.
