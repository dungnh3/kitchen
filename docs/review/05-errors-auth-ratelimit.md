# Review: ecode / errors / localization / guard / rlimit / root pb+proto

Scope note: everything was read-only. I ran tests in a scratch copy, not in the repo. Root = `/Users/dungnh/works/sources/github.com/vulcan/kitchen`.

## 1. Purpose and public API
- **ecode** (`ecode/status.go`, `wrapper.go`): `ErrorCode{gStatus, httpStatus, code, metadata *sync.Map}`. Constructors are `Error(codes.Code, code)` and `Errorf`. Builder methods (`SetHttpStatus/SetCode/SetMessage/SetMetadata/AddMetadata/SetDetailError`) each clone and then re-attach `pb.ErrorInfo{reason, http_status_code, metadata}` as a gRPC status detail. It implements `GRPCStatus()`, `HttpStatus()`, and `Unwrap()` returning `runtime.HTTPStatusError`. Also `WrapError/WrapErrorf`.
- **errors** (root module, `errors/errors.go`): about 120 package-level `ecode` sentinels (general, auth, rate-limit, DB, 3rd-party, AI, and business domains like Orders, Payment, Subscription, Product, Store). `errors/load.go` embeds `langs/en.json` and exposes `ParseDefaultLangEn()`.
- **localization**: a `Localizer` interface over go-i18n (`Load`, `LoadMessages`, `Localize(key, lang)`), plus `ErrorWrapperUnaryServerInterceptor`, which rewrites the `ecode` message using metadata `locale`.
- **guard**: a gRPC unary interceptor (`TokenValidator` v1 with 3 issuers, `TokenValidatorV2` with 1). It parses the JWT **without verifying** it, then sends it to remote introspection (`AuthValidator`, `UserValidator`, `StaffValidator`, each a gRPC client to central-proto services). Also `PermissionRegistry` (method → resource/action).
- **rlimit**: the well-built one. `Limiter` + `Policy` (Default/Endpoints/Keys) + identity ladder (user → anon → device+IP → IP). `RedisStore` (redis_rate GCRA, 100ms timeout, circuit breaker with single-probe half-open, local fallback). `LocalStore` (x/time/rate + size-bounded LRU + sweeper). gRPC unary/stream interceptors return `ResourceExhausted` + standard `errdetails.RetryInfo`. HTTP middleware sets X-RateLimit-* and Retry-After. `metrics` subpackage adds Prometheus hooks.
- **pb/proto**: `proto/error_details.proto` → `pb.ErrorInfo`. `proto/common_error.proto` → `pb.DetailError`, which I found no non-generated user of in kitchen. Both use proto `package pb`.

## 2. Design review

### Error model
- `pb.ErrorInfo` (`proto/error_details.proto:6-10`) re-invents `google.rpc.ErrorInfo` (reason/domain/metadata) and only adds `http_status_code`. It has no `domain` field, and metadata is `map<string,Value>` instead of `map<string,string>`.
- Proto package `pb` gives the type URL `type.googleapis.com/pb.ErrorInfo`. That generic name can collide with any other `package pb` proto in the same binary (registry conflict).
- Embedding the HTTP status in the wire error couples gRPC to REST. AIP-193 derives HTTP from the gRPC code. The custom code exists only so the gateway can override, e.g. `RecordNotFound` = InvalidArgument + 404 (`errors/errors.go:14`).
- Code/HTTP mismatches in `errors/errors.go`:
  - `PermissionDenied` → 401 (l.44; should be 403)
  - `Unauthorized` is PermissionDenied + 401 (l.36)
  - `StorageLimitExceeded` → 500 (l.67)
  - `QuotaExceeded` → 402 (l.53)
  - all ThirdParty* → 424
  - `InvalidSignature` is InvalidArgument + 401 (l.26)
  - domain errors without `SetHttpStatus` (l.96-166) default to **400 even for `codes.Internal`** (`status.go:27`).
- `Error()` does not call `convert()`, so a bare `ecode.Error(...)` has **no ErrorInfo detail** until some `Set*` call. `Errorf` leaves `code` empty (`status.go:35-41`). The message defaults to the reason code string, and localization relies on that as its i18n key.
- `Unwrap()` (`status.go:169`) allocates a new `HTTPStatusError` on every call. It drops the original error chain and is not an identity-preserving wrap.
- **There is no `Is()` method.** `Set*` returns clones, so `errors.Is(err, kerr.RecordNotFound)` (downstream uses this, e.g. 3 hits) silently fails once anyone calls `SetDetailError`.
- `sync.Map` for metadata is pointless because the value is immutable (copy-on-write).
- `SetDetailError(err)` puts `err.Error()` into client-visible metadata (`status.go:86`). Guard does this with introspection/gRPC errors (`interceptors.go:103`), so internal errors leak to clients.
- **`ecode` test fails:** `status_test.go:23` expects 502 but the default is 400, and l.42 type-asserts `GRPCStatus` on a `fmt.wrapError`. `go test` in-repo fails at setup (missing go.sum entries for `pb`).

### HTTP mapping
`server/gateway.go:202-217` and `:226-288` (`CustomErrorHandler`) read only `Details()[0]` as `kitchenpb.ErrorInfo`. If any other detail comes first, the `http_status_code` override is ignored.

### Overlap and cycle
- **Circular module dependency, confirmed.** Root `go.mod:7` requires `kitchen/ecode`, and `ecode/go.mod:5,9` requires root `kitchen` (for `pb`) with `replace => ../`. The replace is ignored for consumers, so they get root pinned at a 2024-09-30 pseudo-version via ecode.
- `errors` lives in root only because of this tangle. That is why "errors=0 direct module": 7 repos import `kitchen/errors` through root.
- Three places define message text: ecode message = code, `errors/langs/en.json`, and the localization bundle. They are not linked by type.

### localization
- `ConvertGrpcError` (`error_wrapper.go:39-42`) rebuilds non-ecode status errors with `ecode.Error(code, msg)`. That **drops all details**, including rlimit `RetryInfo` and validation `BadRequest`. Plain errors become `Unknown` with `inErr.Error()` exposed to the client.
- If `WithLocalizer` is omitted, `ew.localizer` is nil and the first error panics.
- `Load/LoadMessages` swap `s.bundle` with no lock, a race if called at runtime. `LoadMessages` is English-only. `Localize` builds a `NewLocalizer` on every call.
- It uses a custom `locale` metadata key instead of Accept-Language (grpc-gateway forwards `grpcgateway-accept-language`). There is no stream interceptor.

### guard (security-critical)
- **Auth bypass:** in `interceptors.go:93-98`, `selectGuard` returns a nil guard and `handler` runs with an **unverified** token. Signature is never checked (`ParseUnverified`, l.145).
  - Downstream does pass nils: `NewTokenInterceptor(authGuard, nil, whitelist)` and `NewTokenInterceptor(nil, ecoGuard, nil, ...)`.
  - So a self-made unsigned JWT with `iss` set to the nil issuer and a future `exp` is accepted.
- **V2 nil-pointer panic:** `interceptors_v2.go:131` dereferences `s.authGuard.Issuer()` before the nil check at l.65.
- **Internal-service bypass:** `isInternalCheck` (`interceptors.go:18,120`) skips auth for any `*.Internal*Service`. It relies solely on network isolation, and the regex also matches "Internal" anywhere in the package path.
- **Panic risk:** `staff_validator.go:55` does an unchecked type assertion `ctx.Value(MethodKey).(string)`.
- Every request makes a synchronous introspection RPC with no cache. That is a latency and availability coupling at 30K CCU.
- All connections are `insecure` and are never closed. The `RegisterAuthValidator` whitelist is computed and discarded (l.45-48).
- There is no local verification, no JWKS, and no key rotation.
- V1 and V2 are near-duplicate code; V2 recompiles its regex on every call (l.85).
- Header check `Split("Bearer ")` is case-sensitive.
- Uses `pkg/errors` (`guard.go:6`, `staff_validator.go:6`, `user_validator.go:7`).
- The module depends on `central-proto`, which couples a shared library to business protos. It also runs stale versions: go 1.23.4 directive, grpc v1.71.0, a root kitchen from 2025-03, while the root module needs go ≥1.26, so `go vet` in guard fails.
- Tests cover only the regexes.

### rlimit (good)
- The design is sound. `go test -race` passes in both packages.
- Breaker uses atomic CAS for the half-open probe (`redis.go:126-147`). `detached()` handles clients without `ContextTimeoutEnabled`. The sweeper is batched, and `extendFullAt` is monotonic CAS (`local.go:237`). IPv6 is collapsed to /64 and device IDs are hashed.
- Minor issues:
  - `metrics` is not its own module, so Prometheus is a hard dependency of rlimit.
  - miniredis is listed as a direct require.
  - Dry-run still writes Redis state.
  - LRU eviction under pressure resets the evicted keys.
  - No `RetryInfo`-aware HTTP mapping is needed, but localization strips that detail anyway (see above).
- 5 users (fulfillment-orchestrator, drama-short-iap, auth-validator, demo).

## 3. Dependency freshness (proxy.golang.org `@latest`, 2026-11-20)

| dep | used | latest | note |
|---|---|---|---|
| golang-jwt/jwt/v5 | v5.2.1 (guard) | v5.3.1 | update |
| pkg/errors | v0.9.1 | v0.9.1 (2020, archived) | **drop** |
| go-i18n/v2 | v2.5.1 | v2.7.0 | update |
| redis_rate/v10 | v10.0.1 | v10.0.1 (last 2023-04) | stale but current; consider owning the GCRA Lua |
| golang-lru/v2 | v2.0.7 | v2.0.7 (2023) | fine |
| x/time | v0.16.0 | v0.16.0 | ok |
| go-redis/v9 | v9.22.0 | v9.23.0 | minor |
| grpc | v1.84.0 (guard v1.71.0) | v1.85.0 | guard is very stale |
| grpc-gateway/v2 | v2.30.0 | v2.34.0 | update |
| protobuf | v1.36.12 | v1.36.12 | ok; buf plugin pinned `protocolbuffers/go:v1.34.1` (buf.gen.yaml) is stale |
| protoc-gen-validate | v1.2.1 (indirect) / `bufbuild/validate-go` plugin | PGV v1.3.3; **protovalidate v1.4.1** (`buf.build/go/protovalidate`) | migrate |
| lestrrat-go/jwx/v3 | — | v3.4.0 | candidate for JWKS |
| prometheus client | v1.24.1 | v1.25.0 | minor |
| miniredis/v2 | v2.33.0 | v2.40.0 | minor |

- Sources: https://proxy.golang.org/<module>/@latest for each row.
- Buf config: `proto/buf.yaml` is `version: v1beta1`, and `buf.gen.yaml`/`buf.work.yaml` are v1. Buf v2 config merges work + module config; I did not web-verify this, it is from prior knowledge.
- `buf.gen.yaml` excludes `bufbuild/protovalidate` while `buf.yaml` depends on `envoyproxy/protoc-gen-validate`. That is inconsistent.

## 4. Modern-trend assessment and recommendations

- **ecode + errors + pb ErrorInfo → MERGE + REDESIGN** into one leaf module `kitchen/errs` with no root dependency, which breaks the cycle.
  - Use standard `google.rpc.ErrorInfo{reason, domain, metadata}`, `LocalizedMessage`, `BadRequest`, `RetryInfo` from `genproto/googleapis/rpc/errdetails`, following AIP-193.
  - Derive HTTP from `codes.Code` (fix the mismatches). If an override is truly needed, keep it server-side in a reason→HTTP table at the gateway, not on the wire.
  - API sketch:
    `var ErrNotFound = errs.New(codes.NotFound, "NOT_FOUND", errs.Domain("kitchen"))`
    `return errs.Wrap(err, ErrNotFound, errs.Meta("id", id))`
  - Implement `Is()` by reason+domain and `Unwrap()` returning the cause (stdlib `errors.Join/%w`). Keep cause and internal text out of the wire; log them instead.
  - Business-domain sentinels (Orders, Payment, Store…) do **not** belong in kitchen. Move them to the owning services or central-proto.
- **localization → REDESIGN (small).**
  - Interceptor (unary + stream) reads Accept-Language / `grpcgateway-accept-language` and **appends** `errdetails.LocalizedMessage` while keeping existing details. Message catalogs are keyed by `ErrorInfo.reason`.
  - Build an immutable bundle at startup (`embed.FS` + `bundle.LoadMessageFileFS`) and use go-i18n v2.7 `MatchLanguage`. Panic-free when no localizer is given.
- **guard → REDESIGN.**
  - Local JWT verification with jwx v3 `jwk.Cache` (JWKS auto-refresh = key rotation), or golang-jwt v5.3 + keyfunc. Enforce `iss/aud/exp/nbf`, alg allow-list, clock skew.
  - Introspection becomes optional (revocation), behind a TTL cache with singleflight.
  - **Fail closed** on unknown or unconfigured issuers. Use a single configurable interceptor (issuer → verifier map) for unary + stream.
  - Remove central-proto from kitchen; callers inject a `Verifier` interface. Replace pkg/errors with `%w`. Use typed context accessors (`guard.UserFrom(ctx)`).
  - Treat "Internal" services via an explicit allow-list or mTLS/SPIFFE identity, not name regexes.
- **rlimit → KEEP / UPDATE.** Bump deps and split `metrics` into its own module (or use OTel metrics). Optionally vendor the GCRA Lua script, since redis_rate has been unmaintained since 2023. Make sure the error-localization layer preserves `RetryInfo`.
- **proto/buf → UPDATE.**
  - buf v2 config; rename the proto package (e.g. `kitchen.errors.v1`) or delete it in favor of `google.rpc`; drop unused `DetailError`.
  - Replace the `validate-go` plugin with protovalidate (runtime library + interceptor; no codegen).
  - Bump managed plugin versions.

## 5. Open questions for the owner
1. Is the `nil` guard → pass-through (`interceptors.go:97`) intentional, e.g. because an upstream gateway already verified the JWT? Are Internal*Service methods ever reachable from the public gateway?
2. Do the auth/staff/eco issuers publish JWKS or public keys, or only HMAC/opaque introspection? This decides whether local verification is possible.
3. Do clients (mobile/web) depend on the current JSON error shape (`code/message/reason/details[0]` from `CustomErrorHandler`) and the non-standard HTTP codes (401-for-forbidden, 424, 402)? Is a breaking change allowed, or is a compat shim needed?
4. Which languages are actually shipped, and do clients send `locale` metadata or Accept-Language?
5. Should business-domain sentinels (Payment, Orders, Store) move out of kitchen, and who owns them?
6. Is `pb.DetailError` used by any client or older service? It has no user in kitchen.
7. For the new repo, will services consume one root module or per-package modules? This determines how the ecode↔root cycle is broken.
8. rlimit: acceptable fail-open semantics and N× over-admission during a Redis outage at 30K CCU?