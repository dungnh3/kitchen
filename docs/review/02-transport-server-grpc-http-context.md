# Review: server / grpc / nap / kresty / healthcheck / context / pb+proto

Base path: `/Users/dungnh/works/sources/github.com/vulcan/kitchen`. Nothing was modified. Latest versions below come from proxy.golang.org `@latest` and the GitHub API, checked 2026-11-20.

## 1. server (gRPC + grpc-gateway framework)

**API.** `New(opts...)`, then `Serve(ctx)` and `Stop(ctx)` (server.go:37,61,114). About 30 `With*` options (options.go). `ServiceServer` is a 4-method interface (service_server.go:11). Also `Draining(ctx)` (drain.go:31), `IsServerShuttingDown()` (server.go:181), and the exported `DefaultErrorHandler`, `CustomErrorHandler` and `CustomHeaderMatcher` (gateway.go:202,227,303).

**Global state and side effects:**
- A package logger.
- A process-wide `shuttingDown` atomic (server.go:23).
- An `init()` Istio hack: when `JOB=true` and `ENABLE_ISTIO=true` it POSTs to `0.0.0.0:5555/quit` on SIGTERM (server.go:185-216). This is obsolete with native sidecars (KEP-753).
- `createDefaultGrpcConfig` turns on the go-grpc-prometheus global histograms on every `New` (grpc.go:61-62). The *client* histogram is enabled even though no client interceptor is ever installed.

**Lifecycle (good, well thought out).**
- Both ports are bound up front (server.go:64-72).
- The order is: health goes NOT_SERVING, then a delay, then Close (or drain, depending on `WithDrainBeforeClose`), then the drain channel closes, then the HTTP and gRPC servers stop concurrently with a forced stop on timeout (server.go:118-176, grpc.go:211-224).
- `stopOnce` makes Stop safe to call more than once; `WithoutCancel` gives shutdown its own budget even when ctx is already cancelled.
- Weak points:
  - The library owns signal handling inside `Serve` (server.go:74), which is opinionated for a library.
  - Two time budgets (`shutdownTimeout` + `closeTimeout`) add together and are easy to misconfigure against `terminationGracePeriodSeconds`.

**Interceptor chain** (grpc.go:101-136): recovery → drain → logging → prometheus → user interceptors → validator.
- Payload logging is **on by default** (grpc.go:81), so PII and tokens land in logs and span events.
- The validator is go-grpc-middleware's `validator`, which works with PGV-style `Validate()` methods. 26 repos still import envoyproxy PGV; only 3 use protovalidate.
- OTel is opt-in via `WithStatHandler(otelgrpc.NewServerHandler())`, the correct stats-handler style. `WithTracerProvider` is a no-op kept only for compatibility (options.go:244).

**Gateway** (gateway.go:82-176).
- It is an in-process **loopback gRPC dial** (`grpc.NewClient`, not the deprecated `Dial`), so every REST call pays a full extra gRPC hop.
- Message size limits don't match: the client allows 50 MB receive (gateway.go:29,86) but the server keeps grpc-go's default 4 MB receive. Upload-style requests over 4 MB fail unless `WithGrpcServerOptions` is set.
- CORS is hardcoded to `*`, all headers, all methods, and is not configurable (gateway.go:101-114).
- `/metrics` and **pprof are on by default on the public gateway port** (gateway.go:70-73,151). `WithoutPprof` is used by only 3 callers.
- There is no cmux; HTTP and gRPC use two separate ports. HTTP is HTTP/1.1 only (no h2c).
- `IdleTimeout` and `ReadTimeout` are 0 by design (gateway.go:159-164).

**Health.** Two systems coexist:
- the standard `grpc.health.v1`, opt-in through `WithHealthServer` (grpc.go:185);
- a custom `pb.HealthService` (Liveness, Readiness, Version, Status) that each service implements itself.

The logging and metrics selector excludes both (grpc.go:29-37).

**Tests.** server_test.go has 15 solid lifecycle tests (drain, IPv6, panic, concurrent Stop). There are no tests for CORS, the header matcher or the interceptor order.

## 2. grpc/gatewayopt + grpc/protojson

**API.** `HTTPServerConfig.ApplyTo`, the marshalers (`DefaultMarshaler` with `UseEnumNumbers:true`, `ProtoJSONMarshaler`, `FormURLEncodedMarshaler`), `Redirect`, `WrapHttp{Get}Handler`, `CreatePassingHeaderMiddleware` and `DefaultPassedHeaderDecider`, `LoggingMiddleware`, `StaffAuthMiddleware`, `MaxBodyBytes`, `Compress`, and the Prometheus/pprof/CORS mux handlers.

**Findings:**
- **Header spoofing.** Stripping of client-sent `Grpc-Metadata-*` headers only happens inside `CreatePassingHeaderMiddleware` (gateway_middleware.go:38). Without `WithPassedHeader`, `runtime.DefaultHeaderMatcher` forwards `Grpc-Metadata-User-Id` straight through (gateway.go:310). Separately, `User-Id` itself is in the default allowlist (gateway_middleware.go:75), so services trust a client-supplied user id unless something upstream strips it.
- **Staff auth gaps.** `StaffAuthMiddleware` uses fixed path prefixes (staff_auth_middleware.go:64-97) and protects HTTP only; calls to the gRPC port bypass it. It also returns a hand-written 401 body that differs from the gateway's error format.
- **Inconsistent defaults.** There are two "default" JSON marshalers with opposite enum handling: gateway_opt.go:72 uses numbers, protojson/body_marshaler.go:19 uses names. The `Gea*` wrappers only add an error prefix.
- **Logging middleware** (gateway_logging.go:116): every status ≥300, including 304 and 404, is logged at Error. Duration is whole milliseconds converted to float. Health paths are hardcoded (line 88).
- **`Compress`** (compress.go:46) sets `Content-Encoding: gzip` with no minimum size or content-type check. It is not enabled by default and no consumer uses it.
- **Tests:** 10 hardening tests; protojson has none.

## 3. nap (fluent HTTP client, a sling fork plus a hashicorp go-retryablehttp copy)

**API.** A mutable `*Nap` builder: `New`, `NewOtel`, the method/Path/Body*/Query* setters, `Receive`, `Do`, `AutoRetry`, `RetryDoer`, `WithCircuitBreaker`, and `Raw`. Downstream uses `nap.New` 44 times and `nap.Raw` 103 times.

**Bugs and risks:**
- **No timeout.** `defaultClient` has no timeout (nap.go:69-71), so a request without a ctx deadline can hang forever.
- **Not safe to share across goroutines.** The mutex only guards headers (nap.go:248); method, rawURL, ctx and the body providers race. `Request()` writes Content-Type back into the shared builder (nap.go:471).
- **`Clone()` is broken in two ways:**
  - it panics when `baseURL` is nil (nap.go:114);
  - it silently drops the circuit breaker and the multipart provider (nap.go:115-130).
- **`Base()` panics** on a bad URL (nap.go:294).
- **The circuit breaker only trips on transport errors**, because a 5xx comes back as `(resp, nil)` (nap.go:568). It is also a single breaker per Nap regardless of host, with the constant name "Nap CB" (config.go:81-94). It is never used downstream.
- **Metric labels are high-cardinality.** The counter uses the full `rawURL` as the "path" label (nap.go:606) and only counts when decoding happens. It is not registered automatically.
- **Response bodies are logged** at Info level (nap.go:616-636).
- **Retries are unsafe for writes.** The retry doer retries non-idempotent POSTs, buffers the whole body (retry.go:346), honours `Retry-After` without a cap, and the default backoff has no jitter (retry.go:237).
- **Stale module.** go.mod is `go 1.22.5`, otelhttp v0.53 (latest v0.72.0), prometheus v1.19.1 (latest v1.25.0), gobreaker v2.0.0 (latest v2.4.0), go-querystring v1.1.0 (latest v1.2.0). It uses the separate `kitchen/l` logger, not `kitchen/log`.
- **Tests:** 32.

## 4. kresty

kresty/resty.go is a 28-line wrapper that embeds `*resty.Client`, pinned to `resty.dev/v3 v3.0.0-beta.6` (latest is v3.0.0-rc.4, still pre-release). There are 0 import hits downstream. It overlaps entirely with nap.

## 5. healthcheck

- It registers `/api/status` on **`http.DefaultServeMux`** inside `NewServer` (healthcheck.go:27). A second call panics on the duplicate pattern, and anything else registered on the default mux (such as a blank `net/http/pprof` import) gets exposed too.
- There are no server timeouts, and the `port` field and `Run`'s ctx are unused.
- No tests. Used by 2 repos.

## 6. context (metadata and user extraction; most-used package)

**API.** About 25 header constants, the `User` struct, `GetUser`, `GetUserFromHttp`, `GetUserFromHeader`, `Attach*`, per-field getters, `GetClientIP` (with a good trust caveat comment), and the UA detection functions.

**Bugs:**
- **Panic in `GetUserFromHttp`.** It checks `r.Header` for latitude/longitude but then indexes `md.Get(...)[0]`, which panics when the value is in the header but not in metadata (context.go:221-228).
- **Possible panic in `GetMethod`.** It reads `methodParts[1]` while the length guard checks `== 0` (context.go:495-499).
- **Duplicate headers are concatenated.** `strings.Join(md.Get(k), "")` joins repeated values with no separator (context.go:117+), so a direct gRPC caller can send `user-id` twice.
- **`AttachUserToOutgoing` forwards every incoming key.** It copies the *entire incoming* metadata to outgoing (context.go:288-293), including `authorization`, `:authority` and `grpcgateway-*`. Meanwhile `AttachUserToMD` drops RequestID, AppVersion, IP and UserAgent (context.go:297-315), so request ids are lost across hops.
- **Spoofable client IP.** `GetUser` and `GetIPAddress` take the leftmost X-Forwarded-For entry.
- **Legacy duplicate.** `DetectDevicePlatform` (context.go:533) still matches "ios" inside "axios"; it sits next to the fixed version.
- **Untyped constants.** The `DeviceType` constants are untyped (context.go:54).
- `mileusna/useragent` v1.3.5 is current but the repo hasn't been pushed since 2024-09.
- **Tests:** a single client_ip_test.go.

## 7. pb / proto / buf

- **Config is outdated.** `proto/buf.yaml` is **v1beta1**, `buf.gen.yaml` and `buf.work.yaml` are v1 (v2 config replaces workspaces).
- **Deprecated validation.** It depends on `envoyproxy/protoc-gen-validate` and generates `*.pb.validate.go` with `bufbuild/validate-go`. PGV is archived with the note "replaced by protovalidate" (github.com/bufbuild/protoc-gen-validate). The managed-mode `except` already lists protovalidate, so the migration was started but not finished.
- **Old generated code.** It was generated with protoc-gen-go v1.34.1; the latest protobuf-go is v1.36.12.
- **Spec drift.** `svc_health.proto` declares `ToggleReadiness*` messages without an RPC, and `docs/` holds swagger for a `svc_tokenbroker` that is no longer present.

## 8. Dependency freshness

Sources: proxy.golang.org `@latest` and the GitHub API.

| Dependency | Used | Latest | Note |
|---|---|---|---|
| google.golang.org/grpc | v1.84.0 | v1.85.0 | minor bump |
| grpc-gateway/v2 | v2.30.0 | v2.34.0 | update |
| go-grpc-middleware/v2 | v2.3.4 | v2.3.4 | current |
| **go-grpc-prometheus** | v1.2.0 (2018) | — | **archived**; replace with `go-grpc-middleware/providers/prometheus` v1.1.1 |
| otelgrpc / otelhttp | v0.71.0 (server), v0.70 (grpc, indirect), v0.53 (nap) | v0.72.0 | version skew across modules |
| otel | v1.46.0 | v1.47.0 | minor bump |
| protobuf | v1.36.12 | v1.36.12 | current |
| prometheus client_golang | v1.24.1 (grpc), v1.19.1 (nap) | v1.25.0 | nap is far behind |
| rs/cors | v1.11.1 | v1.11.1 | current |
| sony/gobreaker/v2 | v2.0.0 | v2.4.0 | update |
| resty.dev/v3 | beta.6 | rc.4 | still not stable |
| protoc-gen-validate | — | v1.3.3 | **archived** → protovalidate (`buf.build/go/protovalidate` v1.4.1) |
| connectrpc.com/connect | not used | v1.21.1 | — |

Go directives are inconsistent across modules: 1.26.0 (server, grpc, context), 1.22.5 (nap, healthcheck), 1.24.6 (kresty).

## 9. Modern-trend assessment and recommendations

**server: REDESIGN (keep the lifecycle core).**
- Keep: the ordered shutdown, `Draining`, port pre-binding, and stats-handler OTel.
- Change:
  - Make OTel server and client handlers on by default.
  - Swap to `providers/prometheus` with an injectable registry.
  - Switch validation to protovalidate.
  - Turn payload logging off by default.
  - Make CORS configurable.
  - Move pprof and `/metrics` to a separate admin port.
  - Drop the Istio `init()`.
  - Make signal handling optional.
- Gateway: either keep grpc-gateway but register the handler with `RegisterXxxHandlerServer` (no loopback hop, but no streaming), or move to **connectrpc**. Connect serves gRPC, gRPC-Web and Connect-JSON on one `net/http` port, works with Go 1.22+ `ServeMux` method/path patterns, and comes with `grpchealth`, `grpcreflect`, `otelconnect` and protovalidate interceptors. The cost is that the `google.api.http` REST mappings 70 services rely on would need Vanguard to keep REST shapes.
- Target sketch: `kitchen.New(kitchen.WithServices(svc...), kitchen.WithAdmin(":9090"), kitchen.WithCORS(cfg))` then `.Run(ctx)`. A service would be a plain `Register(*Mux)` instead of the current 4-method interface.

**grpc/gatewayopt: MERGE into server.**
- Default-on header sanitising: always strip client `Grpc-Metadata-*` and never trust `User-Id` from the edge.
- One marshaler, one error format.
- Drop `Gea*`, `Compress` and `WrapHttp*` unless they turn out to be needed.

**nap + kresty: MERGE then DROP both.**
- Replacement: a thin wrapper over `http.Client` that adds timeout, `otelhttp.NewTransport`, and retry/breaker through failsafe-go (v0.9.8) or go-retryablehttp (v0.7.8). The retry policy must be idempotency-aware, with jitter and a capped `Retry-After`; the breaker should key on host and count 5xx as failures.
- Builder API: immutable, or one `Request` per call.
- Alternative: standardise on resty v3 once it is GA.
- kresty can be deleted (0 users).

**healthcheck: DROP.** Fold it into the server's admin port, which serves `/livez` and `/readyz` plus grpc.health.v1.

**context: REDESIGN.**
- Use a typed `Principal`/`ClientInfo` built **once** by an interceptor from trusted metadata and stored in ctx under a private key. The per-call getters re-parse metadata today.
- Explicit propagation allowlist (request id included).
- Trusted-proxy-aware client IP.
- Fix the two panics.
- Keep the header constants as a separate tiny package.

**pb/proto: UPDATE.** Move to buf v2 config, protovalidate, and current plugins. Either delete the custom HealthService in favour of grpc.health.v1 with plain HTTP probes, or keep only `/version`.

## 10. Open questions for the owner

1. Is any `ms-*` gateway port reachable without the central `../gateway`? If yes, `User-Id` and `Grpc-Metadata-*` spoofing is a live authz hole; if no, it is defence in depth.
2. REST contract: do clients depend on the exact grpc-gateway JSON shapes (`UseProtoNames`, enum numbers, error body)? This decides whether connectrpc plus Vanguard is acceptable.
3. Are server-streaming RPCs exposed over REST (SSE)? That rules out `RegisterXxxHandlerServer` and affects the connect choice.
4. Which probes do the k8s manifests use: `/health`, `/ready`, `/api/status` or grpc.health? Is `JOB=true` + Istio still deployed?
5. Should the new repo allow breaking API changes across the 70 services? Consumers are already pinned to more than 10 different server pseudo-versions.
6. Prometheus vs OTel metrics: is the target OTLP-only, which would drop the Prometheus interceptors entirely?
7. Is the nap XML/multipart/form support needed by its 17 users, or would a smaller JSON-only client be enough?
