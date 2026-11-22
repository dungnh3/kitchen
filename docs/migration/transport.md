# Migration: transport

From the old library's `kitchen/{server,grpc/gatewayopt,grpc/protojson,nap,kresty,healthcheck,context,pb (HealthService)}`
to `github.com/dungnh3/kitchen/{server,context,httpx}`. Design: [docs/design/transport.md](../design/transport.md).

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
| `WithPassedHeader(decider)` / `DefaultPassedHeaderDecider` | delete them: an allowlist is the default (`xcontext.DefaultForwardHeaders()`); add more with `WithForwardHeaders(...)` |
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
| `WithDefaultLogger()` | `WithLogger(*log.Logger)` (kitchen/log; nil = `log.Default()`, resty's own logs go through it too) |
| `Draining(ctx)` | `server.Draining(ctx)`; it now also works in `HTTPRegistrar` handlers |
| `IsServerShuttingDown()` (global) | `srv.ShuttingDown()`, or a `WithReadiness(Check{...})` |
| `DefaultErrorHandler` / `CustomErrorHandler` | built in: the server's `errs.Handler` (`WithErrors`) renders every REST error as the AIP-193 envelope; `WithHTTPErrorHandler` replaces it. The HTTP status comes from the gRPC code. |
| `CustomHeaderMatcher` | built in (allowlist) |
| `gatewayopt.Redirect`, `FormURLEncodedMarshaler`, `ProtoJSONMarshaler`, `Compress`, `WrapHttp*`, `Gea*` | removed (0 or near-0 users); pass your own `runtime.ServeMuxOption` if you need one |
| Hand-written `EventStreamMarshaler` / `NDJsonMarshaler` | `WithStreamingMarshalers()` |
| `grpc/protojson.DefaultMarshaler()` | `server.JSONMarshaler()` (same options: proto names, enum names, zero values emitted, unknown fields ignored) |
| `grpc/protojson.NewGeaMarshaller` / `NewGeaQueryParser`, `ErrBodyMarshalerPrefix` / `ErrQueryParserPrefix` (string prefixes to detect bad input) | built in: a body, query or path parameter the gateway cannot parse is **400 `MALFORMED_REQUEST`** (`errs.ErrMalformedRequest`, localized) with the parser message in metadata `detail`; match with `errors.Is(err, errs.ErrMalformedRequest)` instead of a prefix. Blank `tools.go` imports of `grpc/protojson` can be deleted. |

### Health

| Old | New |
|---|---|
| your `pb.HealthService` implementation + `pbCore.RegisterHealthServiceServer` + `RegisterHealthServiceHandler` | delete it; the server serves `/livez` and `/readyz` on the HTTP port (`WithoutPublicProbes()` to drop them) and `/livez`, `/readyz`, `/version` on the admin port |
| Readiness tied to `IsServerShuttingDown()` | built in; add dependency checks with `WithReadiness(server.Check{Name, Func})` |
| Version from your build | `WithVersion(v)` (default: module version from build info) |
| `healthcheck.NewServer(port)` + `Run` / `Close` in workers | `server.NewAdmin(":port", server.WithAdminChecks(...)).Run(ctx)` |
| k8s probes on `/health`, `/ready`, `/api/status` | **change them** to `/livez` and `/readyz` (HTTP or admin port), or a `grpc:` probe on :10000. The old paths now return 404. |

### Behaviour changes and fixed bugs

1. **Identity headers are no longer trusted.** `User-Id` (HTTP)
   and `user-id` (gRPC metadata) from clients are removed before
   any interceptor runs. `Grpc-Metadata-*` headers are always removed.
   Internal callers that propagate `user-id` need
   `WithTrustedIdentityPeers(cidrs)` on the receiving server. **Fixes the
   `User-Id` / `Grpc-Metadata-User-Id` spoofing hole.**
2. The auth interceptor must call `xcontext.WithUserID(ctx, id)` instead of
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
8. **Errors**: every error is the AIP-193 envelope from the `errs.Handler`
   (`{"error":{"code":404,"status":"NOT_FOUND","message",...,"details":[ErrorInfo,...]}}`),
   including unknown routes, panics (500) and oversized bodies
   (`PAYLOAD_TOO_LARGE`, HTTP 413).
   Validation errors carry `google.rpc.BadRequest` and an ErrorInfo with
   reason `INVALID_ARGUMENT`. `WithHTTPStatus` is gone.
9. **User id**: `WithUserIDFrom(auth.SubjectFromContext)` replaces writing
   `user-id` into metadata after authentication.
10. **Shutdown has one budget** (default 25s, was 20s + 30s). Close always
   runs after the drain: services that sent a goodbye on a stream from
   `Close` must select on `server.Draining(ctx)` instead.
11. **REST → gRPC no longer dials TCP loopback.** It uses an in-process
   connection, and its streams are not capped by `MaxConcurrentStreams`.
12. **The message size is consistent**: 4 MiB by default on both gRPC servers,
    the gateway client and the REST body limit (413 when Content-Length is
    larger). Before, the gateway accepted 50 MB but the server rejected
    anything over 4 MB.
13. **Default JSON enums are names, not numbers** (`UseEnumNumbers` is
    gone). Repos that used kitchen's default marshaler see `"STATUS_ACTIVE"`
    instead of `1`. To keep numbers, pass
    `WithGatewayOptions(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{MarshalOptions: protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true, UseEnumNumbers: true}}))`.
14. The error body is the AIP-193 envelope for everyone (item 8); the old
    `reason` field is `details[].reason` of the ErrorInfo.
15. Validation: protovalidate rules run first, then PGV `ValidateAll` (all
    violations, not just the first). Failures carry a `BadRequest` detail.
    `WithValidateCallback` is gone.
16. Logs are leveled by outcome. A successful RPC or 2xx/3xx request is
    logged at Debug. 4xx is logged at Info. 5xx and server-fault codes are
    logged at Error. Before, every HTTP status ≥ 300, including 304 and 404,
    was logged at Error.
17. HTTP server: IdleTimeout 650s (was unlimited), ReadHeaderTimeout 10s (was
    30s), h2c enabled.
18. `X-Request-Id` is always present and valid, and is echoed in the response
    header.
19. go-grpc-prometheus is no longer called, so the client histogram is no
    longer enabled globally on every `New`.
20. Response compression (`gatewayopt.Compress`, commented out in the old
    defaults) is `WithCompression()`: off by default, gzip from 1 KiB,
    never for SSE/NDJSON streams.

## 2. context → context (new import path)

The package keeps its name. Import it as
`xcontext "github.com/dungnh3/kitchen/context"` (the alias most services
already use), so only the import path changes in those files.

| Old (`xcontext`) | New (`xcontext`) |
|---|---|
| `GetUserID(ctx) (string, error)` | `xcontext.UserID(ctx) string` (set by auth wiring or a trusted peer only) |
| `AttachUserID(md, id)` in auth middleware | `ctx = xcontext.WithUserID(ctx, id)` |
| `GetUser(ctx)` → `*User` | `info, ok := xcontext.FromContext(ctx)` → `Info` (value) |
| `user.ID` | `xcontext.UserID(ctx)` |
| `user.DeviceID`, `AppVersion`, `Timezone`, `RequestID`, `UserAgent` | same field names on `Info` |
| `user.Locale` / `GetLocale` | `info.AcceptLanguage`, or `xcontext.Header(ctx, xcontext.HeaderLocale)` |
| `user.IP`, `GetIPAddress`, `GetClientIP`, `ReadUserIP`, `ReadClientIP` | `info.ClientIP` (`netip.Addr`), resolved with `server.WithTrustedProxies` |
| `user.Platform` / `DetectPlatformFromUserAgent(ctx)` / `DetectDevicePlatform(ctx)` | `info.Platform` (X-Platform first, then User-Agent); `xcontext.PlatformFromUserAgent(ua)` for a bare string |
| `IOS`, `Android`, `Web` | `xcontext.PlatformIOS`, `PlatformAndroid`, `PlatformWeb` (typed). `Huawei` is gone: Huawei devices report `android`. |
| `user.OSVersion`, `ApplicationID`, `AnonymousUserID`, `Localization`, `Country`, `APIKey`, `GetVendorKey`, `GetFirebaseAppcheck`, `GetRefreshToken`, `GetCSRFToken`, `GetNonce` | not modelled by kitchen: add the header with `server.WithForwardHeaders(...)` and read `xcontext.Header(ctx, "X-Api-Key")`. Credentials (Authorization, cookies) are never captured. |
| `GetAccessToken(ctx)` | package `auth` (it verifies the token); there is no raw bearer accessor |
| `GetUserFromHttp(r)` / `GetUserFromHeader(r)` | inside `HTTPRegistrar` handlers: `xcontext.FromContext(r.Context())`; elsewhere `xcontext.FromHTTP(r, xcontext.Options{...})` |
| `AttachUserToOutgoing` / `AttachUserToMD` / `AttachUserToIncoming` | `xcontext.AppendToOutgoing(ctx)` or `xcontext.UnaryClientInterceptor()` on the client conn |
| `GetMethod(ctx)` | `grpc.Method(ctx)` |
| `GetPattern(ctx)` | `runtime.HTTPPathPattern(ctx)` |
| `Header*` constants | kept: `HeaderRequestID`, `HeaderUserID`, `HeaderForwardedFor`, `HeaderUserAgent`, `HeaderAcceptLanguage`, `HeaderDeviceID`, `HeaderPlatform`, `HeaderAppVersion`, `HeaderTimezone`, `HeaderLocale`, `HeaderClientIP`. `HeaderUserAgent` is now `User-Agent` (it was the gateway key). Others are gone; use string literals with `Header(ctx, ...)`. |
| `DeviceType*`, `DetectDeviceType(ua)` | removed |
| Latitude/Longitude | removed; forward and read them as headers if needed |

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
  was trusted from anyone, so both were spoofable. Now the client IP comes
  only from walking X-Forwarded-For from the right through a trusted-proxy
  list. CF-Connecting-IP is no longer used at all (a client can reach the
  load balancer directly and set it): list the Cloudflare ranges, the load
  balancer IP and the GFE ranges (35.191.0.0/16, 130.211.0.0/22) in
  `server.WithTrustedProxies`. A missing hop makes `ClientIP` that hop's
  address, so per-IP rate limits would see one shared IP.
- `DetectDevicePlatform` matched "ios" inside "axios"; it is replaced by the
  fixed detection.
- The `mileusna/useragent` dependency is gone.

Service-to-service user id (check every service):

- `user-id` metadata from a peer that is not in
  `server.WithTrustedIdentityPeers` is dropped, and `xcontext.UserID(ctx)`
  is `""`. The server logs a Warn ("dropped identity metadata from untrusted
  peer", at most once a minute) when this happens.
- A caller that writes `user-id` into outgoing metadata by hand should use
  `xcontext.UnaryClientInterceptor()` instead, and the callee must list the
  caller's network in `WithTrustedIdentityPeers`.

Mechanical rewrites (review each diff; set `OLD` to the old library's
module path):

```bash
sed -i '' "s#xcontext \"$OLD/context\"#xcontext \"github.com/dungnh3/kitchen/context\"#" $(grep -rl "$OLD/context" --include='*.go' .)
```

Files that imported the package under another alias, or with none, need
the alias set by hand. `HeaderUserAgent` changed meaning (see the table):
fix its call sites by hand, not by rename.

`uid, err := xcontext.GetUserID(ctx)` becomes `uid := xcontext.UserID(ctx)`.
Remove the `err` branch by hand.

## 3. nap / kresty / resty v2 → httpx

`httpx.New` returns an `*httpx.Client` (resty v3 inside, not exposed).
Build it once per upstream and share it.

| Old | New |
|---|---|
| `nap.New()` / `nap.NewOtel()` + `.Base(url)` | `c, err := httpx.New(httpx.WithBaseURL(url))` (OTel always on) |
| resty v2 `resty.New().SetBaseURL(url)` | `httpx.New(httpx.WithBaseURL(url))` |
| `n.Clone().SetContext(ctx).Post(p).BodyJSON(in).Receive(&ok, &raw)` | `err := c.Post(p).JSON(in).Do(ctx, &ok)` |
| resty v2 `c.R().SetContext(ctx).SetBody(in).SetResult(&ok).Post(p)` | `err := c.Post(p).JSON(in).Do(ctx, &ok)` |
| `nap.Raw` failure body / resty `SetError(&e)` | `se, _ := errors.AsType[*httpx.StatusError](err)` (`se.Body`, `se.Decode(&e)`), or `.ErrorInto(&e)` |
| `if failure != nil {...}` / `if res.IsError() {...}` | `if err != nil {...}`: **a non-2xx is now an error** (an `errs` error, see below) |
| client `SetAuthToken(t)` / `SetHeader(k, v)` | `httpx.WithBearer(t)` / `httpx.WithHeader(k, v)`; per call `.Bearer(t)` / `.Header(k, v)` |
| `QueryStruct` / `QueryParams` / `SetQueryParam` | `.Query(k, v)` / `.QueryValues(url.Values)` |
| `"/users/" + id` / `SetPathParam` | `.PathParam("id", id)` on `"/users/{id}"` (escaped) |
| `BodyMultipartFile(payload, files)` / `SetFileReader` | `.FormField(k, v)`, `.File(field, name, r)`, `.MultipartField(...)` |
| `SetFormData` | `.Form(url.Values)` |
| `AutoRetry(WithRetryTimes(n), WithRetryWaitMin(d))` | `httpx.WithRetry(httpx.RetryPolicy{MaxAttempts: n+1, MinBackoff: d})` |
| resty v2 `SetRetryCount` + `AddRetryCondition` | `httpx.WithRetry(httpx.RetryPolicy{Retryable: fn})` |
| POST retry | `.IdempotencyKey(key)` |
| `OnBeforeRequest` / `OnAfterResponse` | `httpx.WithRequestHook` / `httpx.WithResponseHook` |
| `SetTransport(rt)` | `httpx.WithTransport(rt)` or `httpx.WithTransportMiddleware(mw)` |
| `c.Do(httpReq)` / `SetDoNotParseResponse(true)` streaming | `res, err := c.Get(p).Send(ctx)`; read and close `res.Body()` |
| `WithCircuitBreaker()` (nap, unused) | `httpx.WithCircuitBreaker(httpx.BreakerConfig{})`, per client |
| `RetryDoer`, `kresty` | removed (0 users) |

Behaviour changes:

- **Non-2xx returns an error.** With the default mapper: 404 →
  `errs.ErrNotFound`; 408/504 → `errs.ErrUpstreamTimeout`; 429/502/503 →
  `errs.ErrDependencyUnavailable` (with the upstream Retry-After); anything
  else, including a vendor 401/403, → `errs.ErrInternal`. Network failures
  map to `ErrDependencyUnavailable`, attempt timeouts to
  `ErrUpstreamTimeout`. `httpx.WithErrorMapper(nil)` returns the bare
  `*httpx.StatusError` instead. `Send` returns a nil response on error.
- **Absolute URLs on another origin fail** (`httpx.ErrDestinationNotAllowed`)
  when the client has a base URL. Code that fetches arbitrary URLs
  (e.g. `GetContentFromUrl(url)`) needs its own client without a base URL
  and, for user-supplied URLs, `httpx.WithSSRFGuard(httpx.SSRFConfig{})`.
- Redirects: at most 10, never https→http, and a cross-origin hop drops
  every header except Accept*, Content-Type/Length and User-Agent.
- Responses over 32 MiB fail; lift per call with `.MaxResponseBody(0)`.
- No cookie jar (resty v2 enabled one by default).
- Default 30s timeout per attempt (nap had none).
- Query values are redacted (`key=REDACTED`) in error strings.
- `Do` decodes JSON whatever the response Content-Type.

Bugs fixed versus nap: shared `*Nap` data races, `Clone`/`Base` panics,
retries of POSTs and uncapped Retry-After, a breaker shared across hosts,
full-URL metric labels and body logging at Info, stale otelhttp/prometheus
dependencies, credential leaks on absolute URLs and redirects.

## Revision 2026-11-21 (group-2 review)

- `healthcheck` (old standalone `/api/status` server, always OK) → `server`:
  `/livez` and `/readyz` on the HTTP and admin ports, or `server.NewAdmin`
  in worker binaries without a gRPC server.
- `grpc.health.v1` now reports NOT_SERVING while a `WithReadiness` check
  fails (it used to stay SERVING until shutdown).

