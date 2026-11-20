# Migration: transport

From `github.com/vulcanlabsvn/kitchen/{server,grpc/gatewayopt,grpc/protojson,nap,kresty,healthcheck,context,pb (HealthService)}`
to `github.com/dungnh3/kitchen/{server,reqctx,httpx}`. Design: [docs/design/transport.md](../design/transport.md).

## 1. server

### Wiring

Before:

```go
srv, err := server.New(
    server.WithGrpcAddrListen(cfg.Grpc), server.WithGatewayAddrListen(cfg.HTTP),
    server.WithServiceServer(svc),
    server.WithGatewayMuxOptions(CustomJSONMarshaler(), runtime.WithErrorHandler(server.CustomErrorHandler)),
    server.WithPassedHeader(gatewayopt.DefaultPassedHeaderDecider),
    server.WithStatHandler(otelgrpc.NewServerHandler()), server.WithGatewayTracing(),
    server.WithReflection(), server.WithPayloadLogging(false),
)
err = srv.Serve(ctx)
```

After:

```go
ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM) // the library no longer traps signals
defer stop()
srv, err := server.New(
    server.WithGRPCAddr(":10000"), server.WithHTTPAddr(":8080"),
    server.WithServices(svc),
    server.WithAdmin(":9090"),          // optional: /livez /readyz, pprof, metrics
    server.WithReflection(),
)
err = srv.Run(ctx)
```

### Symbol map

| Old | New |
|---|---|
| `New(...)` + `Serve(ctx)` + `Stop(ctx)` | `New(...)` + `Run(ctx)`; cancel ctx to stop |
| `ServiceServer` (4 methods) | `Service{RegisterGRPC(grpc.ServiceRegistrar)}` + optional `GatewayRegistrar`, `HTTPRegistrar`, `Closer` |
| `RegisterWithGrpcServer(*grpc.Server)` | `RegisterGRPC(grpc.ServiceRegistrar)`; the generated `pb.RegisterXxxServer(r, impl)` accepts it unchanged |
| `RegisterWithMuxServer(ctx, mux, conn)` | `RegisterGateway(ctx, mux, conn) error`, same body |
| `RegisterWithCustomHandler(ctx, mux)` + `mux.HandlePath("GET", "/x/{id}", h)` | `RegisterHTTP(mux *http.ServeMux)` + `mux.HandleFunc("GET /x/{id}", h)`; `pathParams["id"]` → `r.PathValue("id")`. Delete the method when it only returns nil. |
| `Close(ctx)` | `Close(ctx) error` (optional `Closer`) |
| `WithGrpcAddr(h, p)` / `WithGrpcAddrListen(l)` | `WithGRPCAddr("h:p")` |
| `WithGatewayAddr` / `WithGatewayAddrListen` | `WithHTTPAddr("h:p")` |
| `WithGatewayMuxOptions(...)` | `WithGatewayOptions(...)` |
| `CustomJSONMarshaler()` copies, `gatewayopt.DefaultMarshaler()` | delete them: the new default is the same shape, with enum names (see §5) |
| `WithGatewayServerMiddlewares(...)` | `WithHTTPMiddlewares(...)` |
| `WithGatewayServerHandler(...)` | `HTTPRegistrar`, or `WithAdmin(addr, WithAdminHandler(...))` for ops endpoints |
| `WithGatewayServerConfig(&HTTPServerConfig{...})` | `WithHTTPServerConfig(func(*http.Server))` |
| `WithPassedHeader(decider)` / `DefaultPassedHeaderDecider` | delete them: an allowlist is the default (`reqctx.ForwardHeaders()`); add more with `WithForwardHeaders(...)` |
| `WithGrpcServerUnaryInterceptors` / `...Stream...` | `WithUnaryInterceptors` / `WithStreamInterceptors` |
| `WithGrpcServerOptions` | `WithGRPCServerOptions` |
| `WithStatHandler(otelgrpc.NewServerHandler())`, `WithGatewayTracing()`, `WithTracerProvider` | delete them: OTel is always on. Use `WithTracerProvider` / `WithMeterProvider` / `WithPropagator` only to override the globals. |
| `WithHealthServer()` | delete it: grpc.health.v1 is always registered |
| `WithReflection()` | unchanged |
| `WithPayloadLogging(false)` | delete it (off by default); `WithPayloadLogging()` turns it on |
| `WithoutPprof()` | delete it: pprof is only served on the admin port, with `WithPprof()` |
| `WithShutdownTimeout`, `WithShutdownDelay` | unchanged names; the timeout now also covers Close |
| `WithCloseTimeout` | delete it (single budget) |
| `WithDrainBeforeClose()` | delete it (always drain, then Close) |
| `WithMaxConcurrentStreams`, `WithKeepalive`, `WithRecoveryHandler` | `WithMaxConcurrentStreams`, `WithKeepalive`, `WithRecovery` |
| `WithGatewayMaxBodyBytes(n)` | `WithMaxMessageSize(n)` (gRPC message size and gateway body limit together) |
| `WithValidateCallback` | removed; violations are returned as `InvalidArgument` + `BadRequest` |
| `WithStaffAuth(fn)` | removed: write a gRPC interceptor so both ports are protected |
| `WithDefaultLogger()` | `WithLogger(*slog.Logger)` |
| `Draining(ctx)` | `server.Draining(ctx)`; it now also works in `HTTPRegistrar` handlers |
| `IsServerShuttingDown()` (global) | `srv.ShuttingDown()`, or a `WithReadiness(Check{...})` |
| `DefaultErrorHandler` / `CustomErrorHandler` | the default is `google.rpc.Status`; `WithHTTPStatus(fn)` maps codes; `WithHTTPErrorHandler` replaces the handler |
| `CustomHeaderMatcher` | built in (allowlist) |
| `gatewayopt.Redirect`, `FormURLEncodedMarshaler`, `ProtoJSONMarshaler`, `Compress`, `WrapHttp*`, `Gea*` | removed (0 or near-0 users); pass your own `runtime.ServeMuxOption` if you need one |
| ms-note-taker `EventStreamMarshaler` / `NDJsonMarshaler` | `WithStreamingMarshalers()` |

### Health

| Old | New |
|---|---|
| your `pb.HealthService` implementation + `pbCore.RegisterHealthServiceServer` + `RegisterHealthServiceHandler` | delete it; the server serves `/health`, `/ready`, `/api/status`, `/version` itself (`WithoutLegacyHealthPaths()` to drop them) |
| Readiness tied to `IsServerShuttingDown()` | built in; add dependency checks with `WithReadiness(server.Check{Name, Func})` |
| Version from your build | `WithVersion(v)` (default: module version from build info) |
| `healthcheck.NewServer(port)` + `Run` / `Close` in workers | `server.NewAdmin(":port", server.WithAdminChecks(...)).Run(ctx)` |
| k8s probes on `/api/status` | keep working. Prefer `httpGet /livez` and `/readyz` on the admin port, or a `grpc:` probe on :10000. |

### Behaviour changes and fixed bugs

1. **Identity headers are no longer trusted.** `User-Id`, `X-User-Id` (HTTP)
   and `user-id`, `x-user-id` (gRPC metadata) from clients are removed before
   any interceptor runs. `Grpc-Metadata-*` headers are always removed.
   Internal callers that propagate `user-id` need
   `WithTrustedIdentityPeers(cidrs)` on the receiving server. **Fixes the
   `User-Id` / `Grpc-Metadata-User-Id` spoofing hole.**
2. The auth interceptor must call `reqctx.WithUserID(ctx, id)` instead of
   writing `user-id` into incoming metadata (`xcontext.AttachUserID`).
3. **pprof and `/metrics` are off the public port.** pprof is available only
   with `WithAdmin(addr, WithPprof())`. There is no Prometheus `/metrics`;
   metrics go through OTel. Dashboards that use `grpc_server_handled_total`
   must move to `rpc.server.*` and `http.server.*`.
4. **CORS is off by default** (it was `*` for everything). Add
   `WithCORS(CORSConfig{AllowedOrigins: ...})` where browsers call the
   service directly.
5. **The library no longer traps SIGINT/SIGTERM**: use
   `signal.NotifyContext`.
6. **The Istio `JOB=true` + `ENABLE_ISTIO=true` init hook is gone.** Use
   native sidecars (Kubernetes ≥ 1.29, Istio `ENABLE_NATIVE_SIDECARS`).
7. **Payload logging is off by default.** It used to log every request body,
   including tokens and personal data.
8. **Shutdown has one budget** (default 25s, was 20s + 30s). Close always
   runs after the drain: services that sent a goodbye on a stream from
   `Close` must select on `server.Draining(ctx)` instead.
9. **REST → gRPC no longer dials TCP loopback.** It uses an in-process
   connection, and its streams are not capped by `MaxConcurrentStreams`.
10. **The message size is consistent**: 4 MiB by default on both gRPC servers,
    the gateway client and the REST body limit (413 when Content-Length is
    larger). Before, the gateway accepted 50 MB but the server rejected
    anything over 4 MB.
11. **Default JSON enums are names, not numbers** (`UseEnumNumbers` is
    gone). Repos that used kitchen's default marshaler see `"STATUS_ACTIVE"`
    instead of `1`. To keep numbers, pass
    `WithGatewayOptions(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{MarshalOptions: protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true, UseEnumNumbers: true}}))`.
12. The error body is `google.rpc.Status` (`{code, message, details}`) for
    everyone. Repos on `CustomErrorHandler` lose the `reason` field until
    errors-auth ships its handler (`WithHTTPErrorHandler`).
13. Validation: protovalidate rules run first, then PGV `ValidateAll` (all
    violations, not just the first). Failures carry a `BadRequest` detail.
    `WithValidateCallback` is gone.
14. Logs are leveled by outcome. A successful RPC or 2xx/3xx request is
    logged at Debug. 4xx is logged at Info. 5xx and server-fault codes are
    logged at Error. Before, every HTTP status ≥ 300, including 304 and 404,
    was logged at Error.
15. HTTP server: IdleTimeout 650s (was unlimited), ReadHeaderTimeout 10s (was
    30s), h2c enabled.
16. `X-Request-Id` is always present and valid, and is echoed in the response
    header.
17. go-grpc-prometheus is no longer called, so the client histogram is no
    longer enabled globally on every `New`.

## 2. context → reqctx

| Old (`xcontext`) | New (`reqctx`) |
|---|---|
| `GetUserID(ctx) (string, error)` | `reqctx.UserID(ctx) string` (set by auth or a trusted peer only) |
| `AttachUserID(md, id)` in auth middleware | `ctx = reqctx.WithUserID(ctx, id)` |
| `GetUser(ctx)` → `*User` | `info, ok := reqctx.FromContext(ctx)` → `Info` (value) |
| `user.ID` | `reqctx.UserID(ctx)` |
| `user.DeviceID`, `Timezone`, `Locale`, `Localization`, `AppVersion`, `OSVersion`, `ApplicationID`, `AnonymousUserID`, `RequestID`, `Country`, `UserAgent` | same field names on `Info` |
| `user.IP`, `GetIPAddress`, `GetClientIP`, `ReadUserIP`, `ReadClientIP` | `info.ClientIP` (`netip.Addr`), resolved with `server.WithTrustedProxies` |
| `user.Platform` (header) / `DetectPlatformFromUserAgent(ctx)` / `DetectDevicePlatform(ctx)` | `info.Platform` (`reqctx.Platform`; header first, then User-Agent), or `reqctx.PlatformFromUserAgent(ua)` |
| `IOS`, `Android`, `Huawei`, `Web` | `reqctx.PlatformIOS`, `PlatformAndroid`, `PlatformHuawei`, `PlatformWeb` (typed) |
| `DeviceType*` (untyped) | `reqctx.DeviceType*` (typed `DeviceType`) |
| `GetUserAgent(ctx)` | `info.UserAgent` |
| `GetCountry(ctx)` | `info.Country` |
| `GetFirebaseAppcheck`, `GetVendorKey`, `GetRefreshToken`, `GetCSRFToken`, `GetNonce`, `user.APIKey` | `reqctx.Get(ctx, reqctx.HeaderFirebaseAppCheck)` etc. |
| `GetAccessToken(ctx)` | `tok, ok := reqctx.BearerToken(ctx)` (case-insensitive "Bearer", no error status) |
| `GetUserFromHttp(r)` / `GetUserFromHeader(r)` | inside `HTTPRegistrar` handlers: `reqctx.FromContext(r.Context())`; elsewhere `reqctx.FromHTTP(r, proxies)` |
| `AttachUserToOutgoing` / `AttachUserToMD` / `AttachUserToIncoming` | `reqctx.AppendToOutgoing(ctx)` or `reqctx.UnaryClientInterceptor()` on the client conn |
| `GetMethod(ctx)` | `grpc.Method(ctx)` |
| `GetPattern(ctx)` | `runtime.HTTPPathPattern(ctx)` |
| `Header*` constants | `reqctx.Header*`. `HeaderUserAgent` is now `User-Agent`, and the gateway key is `HeaderGatewayUserAgent`. `HeaderFirebaseAppcheck` (legacy spelling) is merged into `HeaderFirebaseAppCheck`; metadata keys are case-insensitive. `HeaderRefreshTokenKey` is now `HeaderRefreshToken`. |
| `DetectDeviceType(ua)` | removed (0 users); send `X-Device-Type` |
| Latitude/Longitude fields | removed from Info (0 readers); `reqctx.Get(ctx, reqctx.HeaderLatitude)` |

Bugs fixed:

- `GetUserFromHttp` panicked when lat/long arrived as a header but not as
  metadata.
- `GetMethod` could panic on a method name without a slash.
- Repeated metadata values were concatenated with no separator
  (`strings.Join(v, "")`); `Get` now returns the first value.
- `AttachUserToOutgoing` copied all incoming metadata, `authorization`
  included, to outgoing calls, and dropped the request id.
  `AppendToOutgoing` sends an allowlist that includes the request id.
- The client IP was the leftmost X-Forwarded-For entry and CF-Connecting-IP
  was trusted from anyone, so both were spoofable. Now both are resolved
  through a trusted-proxy list.
- `DetectDevicePlatform` matched "ios" inside "axios"; it is replaced by the
  fixed detection.
- The `DeviceType` constants were untyped.
- The `mileusna/useragent` dependency is gone.

Mechanical rewrites (review each diff):

```bash
gofmt -r 'xcontext.DetectPlatformFromUserAgent(a) -> string(reqctx.PlatformFromUserAgent(reqctx.Get(a, reqctx.HeaderGatewayUserAgent)))' -w .
sed -i '' 's#"github.com/vulcanlabsvn/kitchen/context"#"github.com/dungnh3/kitchen/reqctx"#' $(grep -rl kitchen/context --include='*.go' .)
```

`uid, err := xcontext.GetUserID(ctx)` becomes `uid := reqctx.UserID(ctx)`.
Remove the `err` branch by hand.

## 3. nap / kresty → httpx

| Old (`nap`) | New (`httpx`) |
|---|---|
| `nap.New()` / `nap.NewOtel()` + `.Base(url)`, stored in a struct | `c, err := httpx.New(url, opts...)`, stored and shared (concurrency-safe, OTel always on) |
| `n.Clone().SetContext(ctx).Post(path).BodyJSON(in).Receive(&ok, &raw)` | `err := c.Post(path).JSON(in).Do(ctx, &ok)`; on a non-2xx, `errors.AsType[*httpx.StatusError](err)` and `se.Decode(&raw)` |
| `nap.Raw` as the failure type | `(*httpx.StatusError).Body` / `.Decode` |
| `SetHeader(k, v)` | `.Header(k, v)` per request, or `httpx.WithHeader` per client |
| `SetAuthToken(t)` | `.Bearer(t)` |
| `QueryStruct(s)` | `.Query(k, v)` / `.QueryValues(url.Values)` |
| `BodyMultipart` / `BodyMultipartFile` | `.Multipart(func(w *multipart.Writer) error {...})` (streamed, not retried) |
| `Body(reader)` / `BodyProvider` | `.Body(reader, contentType)` |
| `AutoRetry(WithRetryTimes(n), WithRetryWaitMin(d))` | `httpx.WithRetry(httpx.RetryPolicy{MaxAttempts: n+1, MinBackoff: d})` |
| `RetryDoer`, `WithCircuitBreaker` | removed (0 users) |
| `Do(req, ok, fail)` | `.Send(ctx)` for a raw `*http.Response` |
| `kresty` | removed (0 users) |

Bugs fixed:

- nap had no default timeout; httpx defaults to 30s.
- A shared `*Nap` raced on method, URL, ctx and body. A `Client` is
  immutable and each `Request` is per call, so `Clone` is no longer needed.
- `Clone()` panicked when the base URL was nil and dropped the breaker and
  multipart provider; `Base()` panicked on a bad URL. `New` returns an error.
- The breaker counted only transport errors and was shared across hosts. It
  is removed.
- Metrics used the full URL as a label and logged response bodies at Info.
  Now there is otelhttp only and no body logging.
- Retries repeated POSTs, buffered whole bodies, honoured an uncapped
  Retry-After and had no jitter. Now only idempotent requests are retried,
  with full jitter, Retry-After capped at 30s, and no sleep past the ctx
  deadline.
- Stale dependencies (otelhttp v0.53, prometheus v1.19, gobreaker) and the
  `kitchen/l` logger are gone.
