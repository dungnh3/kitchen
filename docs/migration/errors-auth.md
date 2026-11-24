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
| `errors` (sentinels + `langs/en.json`) | `errs` code sentinels + standard catalog (`errs.Catalog()`) + `i18n` built-in en/vi catalogs | core |
| `localization` | `i18n` + `errs.Handler` | core |
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
| `.SetHttpStatus(n)` | removed: HTTP status comes from the gRPC code (`errs.HTTPStatusFromStatus`; only `PAYLOAD_TOO_LARGE` → 413 and `UNSUPPORTED_MEDIA_TYPE` → 415 differ) |
| `.SetCode(s)` | `errs.New(code, s)` |
| `.HttpStatus()` | `errs.HTTPStatus(err)` |
| `.CodeString()` | `.Reason()` |
| `ecode.WrapError(err)` | `eh.Status(ctx, err)` with an `errs.Handler` (or `errs.ToStatus(err)` without logging); unknown errors become `INTERNAL`, not `InvalidArgument` |
| `errors.InternalServer`, `NotFound`, `BadRequest`, `InvalidArgument`, `Unauthenticated`, `PermissionDenied`, `RateLimitExceeded`, `QuotaExceeded`, `ServiceUnavailable`, `Timeout` | `errs.ErrInternal`, `ErrNotFound`, `ErrMalformedRequest`/`ErrInvalidArgument`, `ErrInvalidArgument`, `ErrUnauthenticated` (or `ErrTokenMissing`/`ErrTokenInvalid`/`ErrTokenExpired`), `ErrForbidden`, `ErrRateLimited`, `ErrQuotaExceeded`, `ErrUnavailable`, `ErrDeadlineExceeded` |
| business sentinels (orders, payments, products, ...) | not in kitchen: define them in the owning service with `errs.New` |
| `errors.ParseDefaultLangEn()` | `i18n.New(language.English, i18n.WithDefaults())` |

Behavior changes:

- `errors.Is(err, sentinel)` now works after `Wrap`/`With`. Generic
  sentinels (reason = code name, no domain: `errs.ErrNotFound`, ...) match
  any error with that gRPC code; other
  sentinels match by reason plus domain, and an empty sentinel domain matches
  the reason in any domain.
- Every client-visible error has an `ErrorInfo` with a domain: an empty
  domain becomes `errs.Config.Domain`, and a bare status gets the code name as
  reason.
- A status error wrapped with `fmt.Errorf` keeps its own message; the wrap
  text no longer reaches the client. A bare (no ErrorInfo) Unauthenticated or
  PermissionDenied status, typically from a failed outgoing call, becomes
  `INTERNAL`: return `errs.ErrTokenInvalid`, `errs.ErrForbidden`, ... for real
  client auth failures.
- The cancelled code's name is `CANCELLED` (google.rpc.Code spelling).
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
| `Localize(key, lang)` | `b.Message(id, data, acceptLanguage...)` / `b.Localize(ctx, errorInfo)` (metadata is template data) |
| `ErrorWrapperUnaryServerInterceptor(WithLocalizer(l))` | `eh := errs.NewHandler(errs.Config{Localizer: b, Domain: ...})`; `eh.UnaryServerInterceptor()` (plus stream), `eh.GatewayErrorHandler`, `eh.WriteHTTP` |

Behavior changes:

- The language comes from `Accept-Language` (gRPC metadata `accept-language`
  or `grpcgateway-accept-language`), then `X-Locale` / `X-Localization`; the
  legacy custom `locale` metadata key is not read.
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
| `kerr.*` returned by the guard | `auth.ErrTokenMissing/Invalid/Expired`, `auth.ErrForbidden`, `auth.ErrInsufficientScope`, `auth.ErrAuthUnavailable` (the `errs` catalog sentinels; `errors.Is(err, errs.ErrTokenExpired)` works) |

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

### auth: reuse of kitchen packages (2026-11-21)

- `auth.NewIntrospection` caches with `kitchen/cache`; set `IntrospectionConfig.Redis` (a `redisx.Open` client) to share results across instances, and call `Introspection.Close` on shutdown.
- `RFC7662Config.HTTPClient *http.Client` is now `Client *httpx.Client` (default: `httpx.New(httpx.WithBaseURL(endpoint), httpx.WithTimeout(5*time.Second))`).
- `JWKSConfig.HTTPClient *http.Client` is now `Client *httpx.Client` (D7; default: `httpx.New(httpx.WithBaseURL(URL), httpx.WithTimeout(10*time.Second))`). The JWKS response is capped at 1 MiB.

## ratelimit (from rlimit)

Every rlimit feature has an equivalent (owner goal 2026-11-21: the new package
is better in every aspect):

| Legacy | New |
|---|---|
| `rlimit.New(store, policy, opts...)` | `ratelimit.New(ratelimit.Config{...})` |
| `Rule{Rate, Burst, Period}`, `PerSecond(n)` | `ratelimit.Rule{Rate, Per, Burst}` |
| identity ladder user → anon → device (+ IP) → IP, `Identity{UserID, AnonID, DeviceID, IP}` | `Limit.By`: `ByUser`, `ByAnon`, `ByDevice`, `ByIP` (unauthenticated only), plus `ByCaller` (most trusted identity, one rule), `ByAnyIP`, `ByGlobal`, and `Limit.Key` for custom keys (tenant, API key). Identity comes from `Config.Subject` (default `xcontext.UserID`), `Config.Anonymous`, `Config.Device` (default `xcontext.Info.DeviceID`), and the client IP from `xcontext.Info` |
| `DefaultLimits()` (per key type) | `ratelimit.DefaultLimits()` (same values) → `Default: &ratelimit.Policy{Limits: ratelimit.DefaultLimits()}` |
| `Policy{Default, Endpoints}` per key type | `Config.Default` / `Config.Policies[method]` with `Limits []Limit`; a request must pass every limit that applies |
| `Policy.Keys["user:123"]` | `Config.Overrides["user:123"]` (also `anon:`, `device:`, `ip:`; IPv6 matched on its /64) |
| `WithWhitelist("ip:10.0.0.0/8", "user:1")` | `Config.Exempt` (same syntax, CIDR for `ip:`) |
| `WithSkip(endpoints...)` | `Config.Skip` |
| `WithEnabled(false)` | `Config.Disabled` |
| `WithDryRun` | `Config.DryRun` (all policies) or `Policy.DryRun` |
| `WithHooks(Hooks{OnDecision, OnStoreError})` | OTel metrics (`kitchen.ratelimit.decisions{policy,limit,result}`, `kitchen.ratelimit.store.errors`) plus `Config.OnDecision(ctx, Event)` |
| `WithDeviceHeaders` | `xcontext.Options.Headers` / `server.WithForwardHeaders`, or `Config.Device` |
| `WithIdentity(fn)` | `Config.Subject` / `Config.Anonymous` / `Config.Device` |
| `Limiter.Allow(ctx, endpoint, Identity)` in business logic | `Limiter.Check(ctx, policy)` (identity from ctx) or `Limiter.Allow(ctx, policy, key)` |
| `WithHTTPEndpoint` (default `METHOD /path`, one policy per id) | the route pattern (`GET /files/{id}`) |
| `WithHTTPDeniedHandler` | `Config.Errors` (`errs.Handler`: domain, localization); denials are always the AIP-193 envelope |
| `NewRedisStore` / `NewDefaultRedisStore` | `Config.Store: ratelimit.RemoteStore(rlredis.New(client))`; breaker and fallback now live in the Limiter |
| `WithLocalFallback` / `WithoutLocalFallback` / fail open | `Policy.OnStoreError`: `FallbackLocal` (default), `Allow`, `Deny` |
| `WithRedisTimeout`, `WithBreakerCooldown` | `Config.StoreTimeout`, `Config.BreakerCooldown` |
| `metrics` (Prometheus hooks) | OTel counters |
| retry-after gRPC metadata | `RetryInfo` detail plus `retry-after`, `ratelimit`, `ratelimit-policy`, `x-ratelimit-*` response metadata |

Behavior changes:

- The local fallback divides each rule by `Config.Instances`, instead of
  letting every pod admit the full rate.
- `ratelimit/redis` requires `ContextTimeoutEnabled` on the go-redis client.
- The GCRA Lua script is embedded; there is no `redis_rate` dependency. Bucket
  keys change (`rl:<policy>:<sub|ip>:<value>`), so existing Redis state is not reused.
- Denials carry reason `RATE_LIMITED` (the service's domain) plus `RetryInfo`.
- Bucket keys percent-encode `{`, `}` and `%` (and `:` in policy names):
  policies whose route has a path variable (`GET /files/{id}`) or a `:` get
  new keys, so their buckets start full once after the upgrade. Redis bucket
  values are now integer microseconds; values written by earlier builds read
  as empty buckets once (keys expire within one window).
- `RateLimit-Policy` scales sub-second or fractional windows: `Rule{Rate: 1,
  Per: 100ms}` is now `q=10;w=1` (was `q=1;w=1`). The format stays the one
  of draft revisions 09–11 (`r`, `t`), not revision 12 (`a`, `w`).
- `RATE_LIMITER_UNAVAILABLE` (503, `OnStoreError: Deny`) now carries a
  `RetryInfo` detail and, over HTTP, `Retry-After`.
- `New` rejects rules over one request per microsecond per bucket (the old
  in-memory store silently allowed everything for rules over 1/ns).
- `X-RateLimit-Remaining` from the Redis store was one too low on about half
  of the allowed requests; it is now exact.
- Store failures are logged once when the breaker opens (and per failed
  probe), not per request.
- `Config.TrustedProxies` is gone: the client IP comes from `xcontext.Info` (resolved by `server.WithTrustedProxies`). `Key(ctx, remote, xff)` is `Key(ctx, remote)`. `Config.Subject` defaults to `xcontext.UserID`. Install it with `server.WithRateLimit(l)`.

## Legacy bugs fixed (behavior changes)

Each bug found in the review of the legacy packages is fixed in the new code and is a behavior change:

| # | Legacy bug | New behavior |
|---|---|---|
| B1 | A nil issuer guard let an **unverified, unsigned JWT** through (`guard/interceptors.go:93-98`). | Fail closed: no verifier for the issuer → `Unauthenticated`. |
| B2 | Tokens were parsed with `ParseUnverified`; the signature was never checked locally. | The signature, `exp` (required), `nbf`, `iss` and `aud` are always verified, against an algorithm allow-list. |
| B3 | V2 interceptor dereferenced `authGuard.Issuer()` before its nil check (panic). | One implementation; `auth.New` rejects nil verifiers. |
| B4 | `*Internal*Service` methods skipped auth via a name regex that also matched package paths. | No name-based bypass; only the explicit `Config.Public` allow-list. |
| B5 | Unchecked `ctx.Value(MethodKey).(string)` in the staff validator (panic). | No untyped context assertions; `auth.FromContext` uses comma-ok. |
| B6 | A synchronous introspection RPC on every request, with no cache. | JWTs verify locally; introspection is cached (TTL, negative TTL, LRU) and coalesced. |
| B7 | `insecure` gRPC connections were created and never closed; the whitelist argument was ignored. | auth opens no connections of its own; `JWKS.Close` stops refreshing; the allow-list is enforced. |
| B8 | `Bearer` was matched case-sensitively with `strings.Split`. | Case-insensitive scheme; exactly one header value; malformed values rejected. |
| B9 | Introspection/gRPC error text was put into client-visible metadata (`SetDetailError`). | Causes are logged only; clients get a reason code. |
| B10 | The regex was recompiled on every call (V2); V1 and V2 duplicated each other. | One code path, no regexes. |
| B11 | Custom `pb.ErrorInfo` re-invented `google.rpc.ErrorInfo` without `domain`, under the collision-prone proto package `pb`. | Standard `google.rpc.ErrorInfo`/`LocalizedMessage`/`BadRequest`/`RetryInfo`. |
| B12 | HTTP status travelled on the wire, and many code/HTTP pairs were wrong (403→401, 402, 424, `Internal`→400 default, ...). | HTTP is derived from the gRPC code only (see the table above). |
| B13 | `ecode.Error(...)` had no `ErrorInfo` until a `Set*` call; `Errorf` had no reason. | `GRPCStatus()` always includes `ErrorInfo`; a reason is mandatory. |
| B14 | No `Is()`, and `Set*` returned clones, so `errors.Is(err, kerr.RecordNotFound)` silently failed. | `errors.Is` matches by reason + domain across `Wrap`/`With`. |
| B15 | `Unwrap()` allocated a new `HTTPStatusError` and dropped the cause chain. | `Unwrap()` returns the cause; the chain is preserved. |
| B16 | `sync.Map` metadata on an immutable value. | Plain maps, copied on write. |
| B17 | The gateway read only `Details()[0]` as ErrorInfo, so other details hid the HTTP override. | Every detail is scanned (`errs.Info`) and rendered in the HTTP body. |
| B18 | Circular module dependency: root ↔ `ecode`, which pinned a 2024 pseudo-version on consumers. | `errs` is a leaf package in core; no cycles. |
| B19 | Message text lived in three places (ecode message, `langs/en.json`, localizer) with nothing linking them. | One key: the `ErrorInfo.reason` is the i18n message ID. |
| B20 | The localization interceptor rebuilt status errors with `ecode.Error`, dropping every detail (including the rate limiter's `RetryInfo`). | Details are kept; `LocalizedMessage` is appended. |
| B21 | Plain errors became `Unknown` with `err.Error()` sent to the client. | Sanitized to `INTERNAL` / "internal error", with the cause logged. |
| B22 | A missing localizer panicked on the first error. | The localizer is optional; nothing panics. |
| B23 | `Load`/`LoadMessages` swapped the bundle without a lock (data race); `LoadMessages` was English-only. | The bundle is immutable after `New`; any language. |
| B24 | `Localize` built a new go-i18n localizer on every call. | One localizer per supported language, built at startup. |
| B25 | A custom `locale` metadata key; no stream interceptor. | Standard `Accept-Language`; unary and stream interceptors. |
| B26 | `WWW-Authenticate` echoed the error message. | `WWW-Authenticate: Bearer`. |
| B27 | `rlimit/metrics` made Prometheus a hard dependency. | OTel metric API; exporters are the observability area's job. |
| B28 | When the LRU was full it evicted the oldest bucket even if partly used, resetting that key. | A full (idle) bucket is evicted first; flooding cannot reset an active key while idle buckets exist. |
| B29 | During a Redis outage each pod enforced the full rule (N× over-admission). | The local fallback divides the rule by `Config.Instances`; per policy, `Allow` or `Deny` instead. |
| B30 | Depended on `redis_rate` (unmaintained since 2023). | Embedded GCRA Lua script. |
| B31 | `buf` config was `v1beta1`/`v1`, with inconsistent validate deps and stale plugins; `DetailError` was unused. | No kitchen error protos at all; validation is converted with `errs/validation.FromError`. |
| B32 | `pkg/errors` (archived). | stdlib `errors` / `%w`. |
| B33 | The `ecode` tests failed (wrong expectations; type assertion on a wrapped error). | Table tests with `-race` in every package. |

Reviewed and kept on purpose: in dry-run mode, allowed requests still
consume tokens (legacy "dry-run writes state"). That keeps dry-run numbers
equal to what enforcement would do; requests that would be limited write
nothing. See design X11.

## Revision 2026-11-21 (group-2 review)

- `auth.IntrospectionConfig.CacheKey` is required when `Redis` is set
  (entries are HMAC-authenticated); entries written by earlier builds are
  ignored once and replaced.
- `auth.Config.Optional` (token optional, but validated when present),
  `MaxTokenBytes` (default 8192), and gRPC streams now end with
  `TOKEN_EXPIRED` when the token expires (`StreamOutlivesToken` to keep the
  old behaviour). The old `guard` had none of these.- `auth.IntrospectionConfig.Prefix` is required when `Redis` is set (one
  prefix per verifier).

## Revision 2026-11-24 (auth deep review)

Breaking:

- `auth.New` fails unless `Config.Audience` is set, or `Config.AnyAudience`
  is set explicitly (not both). Before, an empty audience accepted tokens
  minted for any API of the issuer.
- RFC 7518 minimums at start-up: HMAC secrets of at least the hash size
  (HS384 48 bytes, HS512 64; HS256 stays 32), RSA keys of at least 2048
  bits. `StaticConfig.Leeway` / `JWKSConfig.Leeway` above 5m fail.
- JWTs with a `crit` (or `b64`) protected header are rejected
  (`TOKEN_INVALID`), as RFC 7515 section 4.1.11 requires.
- gRPC streams: after the token expired, `RecvMsg`, `SendMsg` and
  `SendHeader` on the handler's stream return `TOKEN_EXPIRED`.

Behaviour (no API change unless noted):

- JWKS no longer uses jwx's `jwk.Cache`/httprc. During a key rotation every
  request with the new `kid` waits for one shared refresh instead of being
  rejected (before: all but one concurrent request got `TOKEN_INVALID`).
- An unknown `kid` while the JWKS endpoint fails is `AUTH_UNAVAILABLE`
  (503), no longer `TOKEN_INVALID` (401).
- `Verify` after `JWKS.Close` fails fast with `AUTH_UNAVAILABLE` (before: it
  blocked until the caller's deadline, forever without one). `Close` is
  idempotent.
- JWKS keys that must not verify are ignored: `oct`, `use` other than
  `sig`, `key_ops` without `verify`, RSA under 2048 bits. A set with one
  unparseable key keeps the others. Without `Cache-Control: max-age` the set
  is refreshed every `MinRefresh` (unchanged); a failed background refresh
  is retried with backoff (1s doubling, capped at `MinRefresh`; before: next
  try after `MinRefresh`).
- New `JWKSConfig.Logger`; refresh failures and ignored keys are logged at
  Warn (before: silently dropped).
- New `RFC7662Config.Audience`. RFC 7662 transport failures wrap
  `AUTH_UNAVAILABLE` explicitly, and an authority failure is classified
  before any token reason found deeper in its cause, in both the
  introspection cache and the authenticator (always 503, never cached).
