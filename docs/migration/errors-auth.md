# Migration: errors, i18n, auth, rate limiting

This guide maps the legacy kitchen packages `ecode`, `errors`, `localization`,
`guard` and `rlimit` to the new `errs`, `i18n`, `auth` and `ratelimit`. It is a
clean break: there are no compatibility shims and no codemod, because most
changes are about behavior, not names. The design is in
[../design/errors-auth.md](../design/errors-auth.md).

## Package map

| Legacy | New | Module |
|---|---|---|
| `ecode` | `errs` | core |
| `errors` (sentinels + `langs/en.json`) | `errs` generic sentinels + `i18n` built-in en/vi catalogs | core |
| `localization` | `i18n` + `errs.UnaryServerInterceptor` | core |
| `guard` | `auth` | `github.com/dungnh3/kitchen/auth` |
| `rlimit` | `ratelimit` | core |
| `rlimit` Redis store | `ratelimit/redis` | `github.com/dungnh3/kitchen/ratelimit/redis` |
| `rlimit/metrics` (Prometheus) | built-in OTel metrics | — |
| root `pb.ErrorInfo`, `pb.DetailError`, `proto/*.proto` errors | `google.rpc.ErrorInfo` & co. (genproto) | — |

## errs (from ecode / errors)

| Legacy | New |
|---|---|
| `ecode.Error(codes.NotFound, "X")` | `errs.New(codes.NotFound, "X", errs.Domain("svc.example.com"))` |
| `ecode.Errorf(code, fmt, ...)` | `errs.New(code, reason, errs.Message(fmt.Sprintf(...)))`. A reason is now always required. |
| `.SetMessage(m)` | `.With(errs.Message(m))` |
| `.SetMetadata(map)` / `.AddMetadata(map)` | `.With(errs.Meta(k, v), ...)` (string values only) |
| `.SetDetailError(err)` | `errs.Wrap(err, sentinel)`: the cause is logged, **never sent to clients** |
| `.SetHttpStatus(n)` | removed: HTTP status comes from the gRPC code (`errs.HTTPStatusFromCode`) |
| `.SetCode(s)` | `errs.New(code, s)` |
| `.HttpStatus()` | `errs.HTTPStatus(err)` |
| `.CodeString()` | `.Reason()` |
| `ecode.WrapError(err)` | `errs.ToStatus(err)` (unknown errors become `INTERNAL`, not `InvalidArgument`) |
| `errors.InternalServer`, `NotFound`, `BadRequest`, `InvalidArgument`, `Unauthenticated`, `PermissionDenied`, `RateLimitExceeded`, `ServiceUnavailable`, `Timeout` | `errs.ErrInternal`, `ErrNotFound`, `ErrInvalidArgument`, `ErrInvalidArgument`, `ErrUnauthenticated`, `ErrPermissionDenied`, `ratelimit.ErrRateLimited`, `ErrUnavailable`, `ErrDeadlineExceeded` |
| business sentinels (orders, payments, products, ...) | not in kitchen: define them in the owning service with `errs.New` |
| `errors.ParseDefaultLangEn()` | `i18n.New(language.English, i18n.WithDefaults())` |

Behavior changes:

- `errors.Is(err, sentinel)` now works after `Wrap`/`With`. It matches by
  reason plus domain; a sentinel with an empty domain matches the reason in
  any domain.
- The wire detail is `google.rpc.ErrorInfo{reason, domain, metadata}`
  (type URL `type.googleapis.com/google.rpc.ErrorInfo`). The legacy
  `pb.ErrorInfo` with `http_status_code` and `pb.DetailError` are gone.
- HTTP codes follow the gRPC code. Legacy overrides that change:

  | Legacy sentinel | Legacy | New |
  |---|---|---|
  | `PermissionDenied`, `Unauthorized`, `AccountLocked` | 401 | 403 (`PermissionDenied`) |
  | `RecordNotFound` (InvalidArgument + 404) | 404 | use `codes.NotFound` → 404 |
  | `DuplicatedRecord` (InvalidArgument + 409) | 409 | use `codes.AlreadyExists` → 409 |
  | `QuotaExceeded` | 402 | 429 |
  | `StorageLimitExceeded` | 500 | 429 |
  | `ThirdParty*`, `ExternalAPIError` | 424 | from the code (503/500/504/...) |
  | domain errors without an override (default 400) | 400 | from the code (e.g. `Internal` → 500) |

- The HTTP error body is the AIP-193 envelope. It replaces the
  `{code, message, reason, details}` shape of the legacy gateway handler:

  ```json
  {"error": {"code": 404, "message": "agent not found", "status": "NOT_FOUND",
    "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                 "reason": "AGENT_NOT_FOUND", "domain": "agents.example.com",
                 "metadata": {"agent_id": "a1"}}]}}
  ```

  `code` is now the HTTP status (it was the gRPC number). `reason` moved into
  `details[].reason`.

## i18n (from localization)

| Legacy | New |
|---|---|
| `localization.New()` + `Load(paths...)` | `i18n.New(def, i18n.WithFS(fsys, "langs/*.json"))`: errors are returned, never only logged |
| `LoadMessages(map)` | `i18n.WithMessages(tag, map)` (any language, not only English) |
| `Localize(key, lang)` | `b.Message(id, acceptLanguage...)` / `b.Localize(ctx, id)` |
| `ErrorWrapperUnaryServerInterceptor(WithLocalizer(l))` | `errs.UnaryServerInterceptor(errs.ServerConfig{Localizer: b})` (plus `StreamServerInterceptor`) |

Behavior changes:

- The language comes from `Accept-Language` (gRPC metadata `accept-language`
  or `grpcgateway-accept-language`), not from a custom `locale` key.
- The translation is **appended** as `google.rpc.LocalizedMessage`. The status
  message stays the developer message, and the other details (`RetryInfo`,
  `BadRequest`) are kept. Legacy code replaced the message and dropped all details.
- Message IDs are `ErrorInfo.reason` values.
- Unknown errors become `INTERNAL` / "internal error" and are logged; their
  text no longer reaches the client.
- The bundle is immutable after `New` (no runtime reload race). A missing
  localizer no longer panics.

## auth (from guard)

| Legacy | New |
|---|---|
| `NewTokenInterceptor(authGuard, ecoGuard, staffGuard, whitelist)` | `auth.New(auth.Config{Verifiers: [...], Public: [...]})` |
| `TokenValidatorV2` | the same `auth.New` (one issuer is just one verifier) |
| `Guard` interface (`Issuer`, `Validate(ctx, token) (ctx, error)`) | `auth.Verifier` (`Issuer`, `Verify(ctx, token) (*Principal, error)`) |
| `RegisterAuthValidator` / `RegisterUserValidator` / `RegisterStaffValidator` (remote introspection over gRPC) | `auth.NewIntrospection(auth.IntrospectionConfig{Introspect: yourFunc})`, or `auth.RFC7662(...)` for a standard endpoint |
| whitelist methods | `Config.Public` (exact method or `/pkg.Svc/` prefix) |
| `Internal*Service` regex bypass | removed; list those methods in `Public` explicitly, or serve them on a separate listener |
| `OpsService`/`Staff*Service` "staff issuer only" regex | a dedicated `Authenticator` for that listener, or an `Authorizer` |
| `PermissionRegistry` (method → resource/action) | `auth.RequireScopes(method → scopes)` or your own `auth.Authorizer` |
| `ctx.Value(IntrospectAuthKey).(IntrospectAuthInfo)` | `auth.FromContext(ctx)` → `*auth.Principal` |
| `pkg/errors` | stdlib errors |

Behavior changes (security):

- **Fail closed.** The legacy guard parsed tokens without checking the
  signature and let a token through when its issuer had a nil guard. Now every
  token is verified (signature, `exp` required, `nbf`, `iss`, `aud`, algorithm
  allow-list). An unknown issuer, `alg: none`, an unsigned token or an
  algorithm/key mismatch → `Unauthenticated`.
- Introspection results are cached (TTL 30s, never past `exp`; inactive 5s)
  and concurrent lookups are coalesced. An unreachable authority → `Unavailable`.
- Rejection details are logged at Debug level, not sent to clients.
- A stream interceptor and an HTTP middleware are provided.
- No dependency on business protos: the caller supplies the introspection
  function.

## ratelimit (from rlimit)

| Legacy | New |
|---|---|
| `rlimit.New(store, policy, opts...)` | `ratelimit.New(ratelimit.Config{...})` |
| `Rule{Rate, Burst, Period}`, `PerSecond(n)` | `ratelimit.Rule{Rate, Per, Burst}` |
| `Policy{Default, Endpoints, Keys}` per key type | `Config.Default` + `Config.Policies` by method/route; per-key overrides dropped |
| identity ladder (user → anon → device+IP → IP) | `Config.Subject` (e.g. `auth.SubjectFromContext`) → client IP |
| `WithDeviceHeaders`, `WithIdentity` | custom key: call `Limiter.Allow(ctx, policy, key)` yourself |
| `WithWhitelist`, `WithSkip` | omit the method from `Policies` (and set no `Default`) |
| `WithDryRun` | `Policy.DryRun` |
| `NewRedisStore` / `NewDefaultRedisStore` | `ratelimit/redis.New(client)` + `Config.Store`; breaker and fallback now live in the Limiter |
| `WithLocalFallback` / `WithoutLocalFallback` | `Policy.OnStoreError`: `FallbackLocal` (default), `Allow`, `Deny` |
| `WithRedisTimeout`, `WithBreakerCooldown` | `Config.StoreTimeout`, `Config.BreakerCooldown` |
| `metrics` (Prometheus hooks) | OTel counters `kitchen.ratelimit.decisions{policy,result}`, `kitchen.ratelimit.store.errors` |

Behavior changes:

- The local fallback divides each rule by `Config.Instances`, instead of
  letting every pod admit the full rate.
- `ratelimit/redis` requires `ContextTimeoutEnabled` on the go-redis client.
- The GCRA Lua script is embedded; there is no `redis_rate` dependency. Bucket
  keys change (`rl:<policy>:<sub|ip>:<value>`), so existing Redis state is not reused.
- Denials carry reason `RATE_LIMITED` (domain `ratelimit.kitchen`) plus `RetryInfo`.
