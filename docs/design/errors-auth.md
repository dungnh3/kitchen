# Design: errors, i18n, auth, rate limiting

Area: **errors-auth**. Status: approved by the owner on 2026-11-20.
Cross-cutting rules are in [../decisions.md](../decisions.md).

kitchen is a personal, general-purpose library. Nothing here is shaped by the
contracts of services that used the legacy library (old error JSON, business
error codes, existing token issuers).

## 1. Decisions (agreed with the owner)

| # | Topic | Decision |
|---|---|---|
| E1 | Wire error details | Standard `google.rpc` types only (AIP-193): `ErrorInfo{reason, domain, metadata}`, `LocalizedMessage`, `BadRequest`, `RetryInfo` from `genproto/googleapis/rpc/errdetails`. No kitchen error proto, so no `proto/` dir or buf config in this area. |
| E2 | HTTP status | Derived from the gRPC code with grpc-gateway's `runtime.HTTPStatusFromCode`. Nothing about HTTP on the wire. Exceptions: a status the REST gateway itself picks (`runtime.HTTPStatusError`, e.g. 405), and two INVALID_ARGUMENT reasons with a dedicated RFC 9110 status that clients, proxies and upload libraries act on: `PAYLOAD_TOO_LARGE` → 413 and `UNSUPPORTED_MEDIA_TYPE` → 415 (owner, 2026-11-21; fixed table, any domain, `HTTPStatusFromStatus`). The gRPC code stays INVALID_ARGUMENT; the envelope's `code` is the HTTP status. No per-error HTTP status option. |
| E3 | HTTP error body | AIP-193 envelope `{"error":{"code","message","status","details"}}`. `errs` renders it; transport calls it. |
| E4 | `errs` API | Immutable sentinels; the cause is for logs only. A generic target (reason = its code name, empty domain: `ErrNotFound` and its copies) matches any error with that gRPC code in `errors.Is`; every other target matches by reason + domain (also after `Wrap`/`With`). No hidden flag: the rule follows from the fields. Ships the code sentinels plus a catalog of 27 reasons common to API servers (owner review, 2026-11-20); no business errors. |
| E5 | Validation | No proto. `errs.BadRequest(...FieldViolation)`; protovalidate errors convert with `errs/validation.FromError` (a separate package so `errs` does not pull in CEL), which uses `protovalidate.FieldPathString`. |
| E6 | Unknown and foreign errors | A non-`errs`, non-status error is sanitized to `INTERNAL` / `"internal error"` and logged. A local `*Error` is trusted (a nil `*Error` or code OK becomes INTERNAL). A **foreign status** (from a downstream call; owner 2026-11-21, after a confirmed leak of `dial tcp 10.0.3.7:5432` and `pq: password authentication failed`): client errors (InvalidArgument, NotFound, AlreadyExists, FailedPrecondition, OutOfRange, ResourceExhausted, Aborted, Canceled) keep code, message and details; Unauthenticated/PermissionDenied become INTERNAL (they are this service's credentials); every other code keeps its code with the generic message and only RetryInfo. The original is logged. The REST gateway trusts statuses that carry an ErrorInfo (completed by the server's own `errs.Handler` on the in-process call) and sanitizes the others the same way. Every response carries an ErrorInfo; an empty domain becomes `Config.Domain`. |
| I1 | Catalogs | `i18n` embeds en + vi for every reason in `errs.Catalog()` plus the auth and ratelimit reasons (a test enforces coverage). Message ID = `ErrorInfo.reason`; messages are go-i18n templates with ErrorInfo metadata as data. The bundle is immutable once built. |
| I2 | Locale source | `Accept-Language` first (ctx value, `xcontext.Info.AcceptLanguage`, raw `accept-language` / `grpcgateway-accept-language` metadata), then the `X-Locale` header captured in `xcontext.Info`. Localized text goes in a `LocalizedMessage` detail; the top-level message stays developer English (AIP-193). |
| A1 | Fail-closed | Every token is verified (signature, `exp`/`nbf`/`iss`/`aud`, algorithm allow-list). Unknown issuer, no verifier, unsigned, `alg=none`, expired → `Unauthenticated`. There is no "trust upstream" mode. |
| A2 | Verifiers | Small `Verifier` interface plus built-ins: JWKS (auto-refresh), static keys (HMAC / public keys, several kids for rotation), introspection (RFC 7662 client or a custom func) with a cache. |
| A3 | JWT library | `github.com/lestrrat-go/jwx/v3` for JWS, JWT validation and JWK parsing. The JWKS refresher is kitchen's own (2026-11-24 deep review, owner decision): `jwk.Cache`/httprc serialised every lookup through one controller goroutine, rejected concurrent new-kid requests during a rotation, and blocked `Verify` after `Close`. |
| A4 | Public methods | Deny by default. `Config.Public` is an explicit allow-list: an exact full method, or a `/pkg.Service/` prefix. No name regexes (the old `*Internal*Service` bypass is gone). |
| A5 | Introspection cache | Built on `kitchen/cache` (owner, 2026-11-21, reuse): in-process L1 (otter) plus optional Redis L2 (`IntrospectionConfig.Redis`, prefix `auth:introspection:`) so a fleet asks the authority once per token and TTL. Keys are SHA-256 hashes of the token. Active results live 30s by default, never past the token `exp` (validity is stored with the result and checked on every read); inactive results live 5s. Concurrent lookups are coalesced by the cache's singleflight, detached from the first caller and bounded by `Timeout`; an authority error is never cached and fails closed (`Unavailable`). `RFC7662` sends its requests with `kitchen/httpx` (traced, bounded error bodies). |
| A6 | Authorization | `Authorizer` hook, plus a built-in `RequireScopes(method → scopes)` that reads the `scope`/`scp` claim. A missing scope → 403 `INSUFFICIENT_SCOPE` with metadata `missing_scope` and `WWW-Authenticate: Bearer error="insufficient_scope", scope="…"` (RFC 6750, owner 2026-11-21). Any other Authorizer error is 403 `FORBIDDEN`, never 401. Rejected tokens get `error="invalid_token"`. |
| A7 | Explicit typing | `StaticConfig.Types` / `JWKSConfig.Types` (owner, 2026-11-21): when set, the JWT `typ` header must be one of them (RFC 8725 §3.11, RFC 9068 `at+jwt`; case-insensitive, `application/` optional), so an ID token cannot be replayed as an access token. Empty: not checked, because many IdPs send `typ: JWT`. |
| A8 | Server integration | `server.WithAuth(a)` (owner, 2026-11-21): the auth interceptors run in the server chain after the errs interceptor, the gRPC health service is always public, and plain HTTP routes (`HTTPRegistrar`) are authenticated too (fail-closed), by their matched route pattern. The subject is stored with `xcontext.WithUserID`. |
| R1 | Store outage | Per-policy `OnStoreError`: `FallbackLocal` (default) / `Allow` / `Deny`. The circuit breaker lives in the limiter. The local fallback divides the rule by `Config.Instances`. |
| R2 | Metrics | OTel metric API (`MeterProvider` option, default global). No Prometheus package. |
| R3 | GCRA | Own embedded Lua script (algorithm after go-redis/redis_rate, BSD-2), run with `EVALSHA`. No redis_rate dependency. |
| R4 | Rate-limit key | Pluggable. Default (owner, 2026-11-21): the subject from `Config.Subject`, by default `xcontext.UserID` (set by `auth`), else the client IP that `server` resolved with its trusted proxies (`xcontext.Info.ClientIP`), else the transport peer (IPv6 collapsed to /64). `ratelimit` has no proxy/`X-Forwarded-For` logic of its own (`Config.TrustedProxies` removed), which also fixes REST calls through the in-process gateway, which have no peer IP and used to go unlimited. |
| R5 | Response headers | `RateLimit-Policy` / `RateLimit` (IETF httpapi draft, structured fields `"<policy>";q=;w=` and `"<policy>";r=;t=`) plus `X-RateLimit-Limit/Remaining/Reset` (owner, 2026-11-21), on plain HTTP routes and, through response metadata, on REST gateway calls (`server` passes them unprefixed). Errors carry no fixed domain: `RATE_LIMITED` and `RATE_LIMITER_UNAVAILABLE` get the service's domain from its `errs.Handler`. |
| R6 | Identities and limits | (owner, 2026-11-21: better than rlimit in every aspect) A `Policy` holds `Limits`, each counting one identity kind (`By`): `ByCaller` (most trusted: user, anon, device, IP), `ByUser`, `ByAnon`, `ByDevice`, `ByIP` (unauthenticated only), `ByAnyIP`, `ByGlobal`, or a custom `Limit.Key`. Limits are checked from the most specific to the most shared and stop at the first denial, so a denied request does not use up shared buckets (rlimit did this only for its fixed device-then-IP pair). `Exempt` (identities and CIDRs), `Overrides` (rule per identity, bucket unchanged), `Skip`, `Disabled`, global `DryRun`, `OnDecision` hook, `DefaultLimits()` and `Check(ctx, policy)` for business logic cover every rlimit option. Device ids are hashed in keys; ids over 256 bytes are ignored. |
| R7 | Review fixes (2026-11-21) | `ByCaller` also counts the client IP when the caller only has an unverified device id (rotating `X-Device-Id` gave a fresh bucket per request). `Exempt` rejects `device:` (client-controlled) and matches a single IP exactly (an IPv6 exemption no longer covers its /64; Overrides still apply to the /64 bucket). DryRun never blocks, also during a store outage with `Deny`. `Allow(ctx, policy, key)` keys live under `key:` (cannot drain `user:`/`ip:` buckets) and are hashed over 256 bytes. Metric attribute sets are precomputed per limit and result. A request with no identity at all (no subject, device, or IP; only possible with in-process transports) is counted by `ByGlobal` limits only. |
| M1 | Modules | Core: `errs`, `i18n`, `ratelimit`. Submodules: `auth` (jwx), `ratelimit/redis` (go-redis). |
| G1 | Naming | No legacy organisation names anywhere in code, docs or tests. |

## 1b. Decisions taken by default (to review)

The owner asked (2026-11-20) that open questions take the option that is
best for system design and security, without asking, and be listed here for
a later review. Security-relevant defaults are fail-closed.

| # | Decision taken | Alternatives | Why / trade-offs |
|---|---|---|---|
| X1 | JWTs must carry `exp`; a token without it is rejected. | Accept tokens without `exp`. | Pro: no immortal tokens. Con: issuers that omit `exp` cannot be used. |
| X2 | HMAC secrets must be ≥ 32 bytes; the algorithm family must match the key type (HS* ↔ `[]byte`, RS/PS ↔ RSA, ES ↔ ECDSA, EdDSA ↔ Ed25519); `none` is never accepted. | Trust jwx to reject mismatches at verify time. | Pro: misconfiguration fails at startup; alg confusion is impossible by construction. Con: short legacy secrets must be rotated. |
| X3 | `NewJWKS` refuses HS* algorithms and fails if the first fetch fails. | Allow HS* in a JWKS; start lazily and return `Unavailable` until the keys arrive. | Pro: a JWKS is public, so HS* keys there would be a vulnerability; fail-fast startup surfaces bad URLs. Con: the service cannot start while the IdP is down. |
| X4 | A refresh caused by an unknown `kid` happens at most once per 30s (`MissRefresh`). | Refresh on every miss; never refresh outside the schedule. | Pro: rotation works without a DoS lever against the IdP. Con: a just-rotated key can be rejected for up to 30s after a junk-kid refresh. Requests arriving while a refresh runs wait for it instead of being rejected (2026-11-24). |
| X16 | (2026-11-24, owner) `Config.Audience` is required; `AnyAudience` is the explicit opt-out. | Optional audience. | Pro: RFC 8725 section 3.9, fails closed. Con: one more config line. |
| X17 | (2026-11-24, owner) RFC 7518 minimums: HMAC secret ≥ hash size, RSA ≥ 2048 bits (static and JWKS keys); leeway ≤ 5m. | 32-byte HMAC for all; no RSA or leeway bound. | Pro: weak keys fail at start-up. Con: short HS384/HS512 secrets must be rotated. |
| X18 | (2026-11-24) Tokens with `crit` (or `b64`) are rejected: no critical extension is supported. | Allow-list extensions. | Pro: RFC 7515 section 4.1.11 MUST. Con: none known for access tokens. |
| X19 | (2026-11-24) JWKS outage: keep the last good key set indefinitely; unknown kid → 503; a cleanly parsed empty set replaces the old one. | Expire keys after a maximum staleness. | Pro: no outage when the IdP is briefly down. Con: a key withdrawn while the endpoint is unreachable stays usable until it answers again. |
| X5 | Several `Authorization` values, non-Bearer schemes and tokens containing whitespace are rejected. | Take the first value. | Pro: no header-smuggling ambiguity. Con: none in practice. |
| X6 | A token without `kid` is accepted only when the verifier has exactly one key. | Try every key. | Pro: a constant amount of work per token, and no key oracle. Con: issuers with several keys must send `kid`. |
| X7 | One introspection call is shared by concurrent callers, detached from the first caller's cancellation, and bounded by `Timeout` (2s). | Use each caller's own context. | Pro: one cancelled request cannot fail its siblings. Con: the call may outlive a cancelled request by up to `Timeout`. |
| X8 | `Config.Audience` is checked on every principal in the Authenticator, whichever verifier produced it; verifiers may also check their own. | Check audience only in the verifiers. | Pro: introspection and custom verifiers get the same check. Con: the audience is configured in two places when both are used. |
| X9 | Rate limiter: `OnStoreError: Deny` answers 503 `RATE_LIMITER_UNAVAILABLE`, not 429. | 429. | Pro: clients and alerts can tell "you are too fast" from "the limiter is broken". Con: one more code to handle. |
| X10 | A request with neither a subject nor a parseable peer IP is not rate-limited. That only happens with in-process transports. | Put all such requests in one shared bucket. | Pro: no global bucket that one caller can exhaust for everyone. Con: an in-process caller is unlimited; real TCP connections always have a peer address. |
| X11 | Dry-run counts exactly like enforcement: allowed requests consume tokens, would-be-limited requests write nothing (GCRA never writes on a deny). | Separate dry-run buckets; no writes at all. | Pro: dry-run metrics predict real enforcement, and switching a policy to enforce starts from accurate state. Con: dry-run traffic shares buckets with any enforced policy on the same name. |
| X12 | When the in-memory store is full, it evicts a full bucket among the 8 least recently used; it drops the tail only if none is full. | Plain LRU. | Pro: a flood of new keys cannot reset an active key's partly used bucket. Con: up to 8 extra checks per eviction. |
| X13 | i18n builds one localizer per supported language at startup, and ignores `Accept-Language` values longer than 256 bytes. | Build a localizer per request; parse any length. | Pro: no per-request allocation on the error path, and oversized headers cannot cost parse time. Con: unusual huge headers fall back to the default language. |
| X15 | ~~`auth` does not import core~~ **Superseded (owner, 2026-11-21):** `auth` (still a submodule, so jwx stays out of core) imports core `errs` and `context`. Its errors are the `errs` catalog sentinels (`TOKEN_MISSING`, `TOKEN_INVALID`, `TOKEN_EXPIRED`, `FORBIDDEN`, `INSUFFICIENT_SCOPE`) plus `AUTH_UNAVAILABLE`, so `errors.Is(authErr, errs.ErrTokenExpired)` holds and the domain comes from the service's `errs.Handler`. `ratelimit/redis` still does not import core. | Keep auth independent; merge auth into core. | Pro: one error model, one HTTP renderer, i18n for free, the authenticated subject is written to `xcontext.UserID`. Con: core must be tagged before auth. |
| X14 | `errs` sentinels without `Message` use the reason as the client message. | Require a message. | Pro: a terse but safe default; localization covers user-facing text. Con: the reason string becomes visible as the message. |

## 2. Package `errs` (core)

Rewritten after the owner review (2026-11-20). Dependencies: grpc,
`genproto/googleapis/rpc` (errdetails, code), protobuf, grpc-gateway `runtime`
(HTTP status table, gateway error handler). No other kitchen package. The
legacy mistakes it avoids are listed in the review (custom ErrorInfo proto,
HTTP status on the wire, wrong 401/403 mapping, no `Is`, `err.Error()` sent
to clients, details dropped by localization, three sources of message text).

```go
type Error struct{ /* code, reason, domain, message, metadata, details, cause: all immutable */ }

func New(code codes.Code, reason string, opts ...Option) *Error
func Wrap(cause error, sentinel *Error, opts ...Option) *Error
func (e *Error) With(opts ...Option) *Error

func Domain(d string) Option; Message(m string) Option; Meta(k, v string) Option
func Details(ds ...proto.Message) Option; RetryAfter(d time.Duration) Option

func (e *Error) Error() string; Unwrap() error
func (e *Error) Is(target error) bool // generic target (reason == CodeName(code), no domain): by code; else reason + domain
func (e *Error) GRPCStatus() *status.Status // never the cause
func (e *Error) Code() codes.Code; Reason(); Domain(); Message(); Metadata(); Details()

func Code(err error) codes.Code
func CodeName(c codes.Code) string          // google.rpc.Code name: "NOT_FOUND", "CANCELLED"
func HTTPStatus(err error) int
func HTTPStatusFromCode(c codes.Code) int   // runtime.HTTPStatusFromCode
func HTTPStatusFromStatus(st *status.Status) int // + 413/415 reason exceptions
func Info(err error) (*errdetails.ErrorInfo, bool)
func FromStatus(st *status.Status) *Error   // client side
func FromError(err error) *Error            // client side
func ToStatus(err error) (st *status.Status, sanitized bool) // E6, no logging
func MarshalHTTP(st *status.Status) (body []byte, httpStatus int)

type FieldViolation struct{ Field, Description, Reason string }
func BadRequest(vs ...FieldViolation) *Error

// One Handler per service, shared by every transport.
type Localizer interface {
    Localize(ctx context.Context, info *errdetails.ErrorInfo) (locale, message string, ok bool)
}
type Config struct{ Logger *slog.Logger; Localizer Localizer; Domain string }
func NewHandler(cfg Config) *Handler
func (h *Handler) Status(ctx context.Context, err error) *status.Status
func (h *Handler) UnaryServerInterceptor() grpc.UnaryServerInterceptor
func (h *Handler) StreamServerInterceptor() grpc.StreamServerInterceptor
func (h *Handler) WriteHTTP(w http.ResponseWriter, r *http.Request, err error)
func (h *Handler) GatewayErrorHandler(ctx, *runtime.ServeMux, runtime.Marshaler, http.ResponseWriter, *http.Request, error)
func WriteHTTP(w http.ResponseWriter, r *http.Request, err error) // unconfigured Handler

func Catalog() []*Error // every sentinel, for catalog coverage checks
```

Package `errs/validation`: `FromError(err) (*errs.Error, bool)` for
`*protovalidate.ValidationError`.

### Sentinels

Code sentinels, reason = google.rpc.Code name: `ErrCanceled` (`CANCELLED`),
`ErrUnknown`, `ErrInvalidArgument`, `ErrDeadlineExceeded`, `ErrNotFound`,
`ErrAlreadyExists`, `ErrPermissionDenied`, `ErrResourceExhausted`,
`ErrFailedPrecondition`, `ErrAborted`, `ErrOutOfRange`, `ErrUnimplemented`,
`ErrInternal`, `ErrUnavailable`, `ErrDataLoss`, `ErrUnauthenticated`.

Catalog (domain empty; the Handler fills `Config.Domain`). `errs.Domain(...)` is only for a service that reports errors for several domains:

| Group | Reason | Code (HTTP) |
|---|---|---|
| Request | `MALFORMED_REQUEST`, `UNSUPPORTED_MEDIA_TYPE`, `PAYLOAD_TOO_LARGE`, `INVALID_PAGE_TOKEN`, `INVALID_PAGE_SIZE` | InvalidArgument (400; HTTP 415 for `UNSUPPORTED_MEDIA_TYPE`, 413 for `PAYLOAD_TOO_LARGE`) |
| Authentication | `TOKEN_MISSING`, `TOKEN_INVALID`, `TOKEN_EXPIRED`, `API_KEY_INVALID` | Unauthenticated (401) |
| Authorization | `FORBIDDEN`, `INSUFFICIENT_SCOPE`, `ACCOUNT_DISABLED` | PermissionDenied (403) |
| State | `RESOURCE_CONFLICT`, `VERSION_MISMATCH` | Aborted (409) |
| State | `PRECONDITION_FAILED`, `INVALID_STATE`, `CLIENT_VERSION_UNSUPPORTED`, `FEATURE_DISABLED` | FailedPrecondition (400) |
| Idempotency | `IDEMPOTENCY_KEY_REUSED` (FailedPrecondition, 400), `REQUEST_IN_PROGRESS` (Aborted, 409) | |
| Limits | `RATE_LIMITED`, `QUOTA_EXCEEDED`, `CONCURRENCY_LIMITED` | ResourceExhausted (429) |
| Server | `SERVICE_DRAINING`, `MAINTENANCE`, `DEPENDENCY_UNAVAILABLE` | Unavailable (503) |
| Server | `UPSTREAM_TIMEOUT` | DeadlineExceeded (504) |

The Handler logs at Error level, with the cause, sanitized errors and
`Internal`/`Unknown`/`DataLoss` errors. It adds a `LocalizedMessage` when the
Localizer knows the reason and the status has none. `GatewayErrorHandler` does
not sanitize statuses from the in-process gRPC call again (the interceptor
already did); it completes them (domain, localization) and keeps a gateway
HTTP status such as 405.

HTTP sample (`Handler.WriteHTTP`):

```json
HTTP/1.1 404 Not Found
Cache-Control: no-store
{"error": {"code": 404, "message": "agent not found", "status": "NOT_FOUND",
  "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
               "reason": "AGENT_NOT_FOUND", "domain": "agents.example.com",
               "metadata": {"agent_id": "a1"}},
              {"@type": "type.googleapis.com/google.rpc.LocalizedMessage",
               "locale": "vi", "message": "Không tìm thấy agent a1."}]}}
```

## 3. Package `i18n` (core)

Dependencies: `github.com/nicksnyder/go-i18n/v2`, `golang.org/x/text/language`,
`kitchen/context` (request Info).

```go
func New(def language.Tag, opts ...Option) (*Bundle, error)
func WithDefaults() Option                                 // embedded en + vi for every errs reason
func WithFS(fsys fs.FS, patterns ...string) Option
func WithMessages(tag language.Tag, msgs map[string]string) Option

func (b *Bundle) Message(id string, data map[string]string, acceptLanguage ...string) (msg string, tag language.Tag, ok bool)
func (b *Bundle) Match(acceptLanguage ...string) language.Tag
func (b *Bundle) Localize(ctx context.Context, info *errdetails.ErrorInfo) (locale, msg string, ok bool) // errs.Localizer

func WithAcceptLanguage(ctx context.Context, v string) context.Context
func AcceptLanguage(ctx context.Context) []string // see I2 for the order
```

The bundle is immutable after `New` and safe for concurrent use. A missing
message falls back to the default language and then reports `ok=false`.
A template key missing from the metadata renders as `<no value>`.
Nothing panics.

## 4. Module `auth` (submodule `github.com/dungnh3/kitchen/auth`)

Dependencies: core (`errs`, `context`), `lestrrat-go/jwx/v3`, `golang.org/x/sync`, grpc, OTel metric (see X15). The text below predates X15's revision: errors are now `errs` sentinels and `Config.WriteError` defaults to `errs.WriteHTTP`. Formerly its `*auth.Error` used the same wire model as `errs` (gRPC code + `google.rpc.ErrorInfo`), so `errs.Code/Info/ToStatus/WriteHTTP` handle it via `GRPCStatus`. `Config.WriteError` (default: AIP-193 envelope) lets apps plug in `errs.WriteHTTP`.

```go
type Principal struct {
    Subject, Issuer, TokenID string
    Audience, Scopes         []string
    ExpiresAt, IssuedAt      time.Time
    Claims                   map[string]any
}

type Verifier interface {
    Issuer() string // routing key; "" only for opaque-token verifiers
    Verify(ctx context.Context, token string) (*Principal, error)
}

func NewStatic(cfg StaticConfig) (*Static, error)              // Keys: []Key{ID, Alg, Material ([]byte ≥32B | public key)}; Leeway (default 30s)
func NewJWKS(ctx context.Context, cfg JWKSConfig) (*JWKS, error) // URL, Algs allow-list (no HS*), refresh bounds, rate-limited refresh on unknown kid; Close(ctx)
func NewIntrospection(cfg IntrospectionConfig) (*Introspection, error) // Introspect func + cache (A5)
func RFC7662(cfg RFC7662Config) IntrospectFunc                  // HTTP client for RFC 7662 endpoints
func All(vs ...Verifier) Verifier                                // all must pass (e.g. JWKS + revocation check)

type Config struct {
    Verifiers  []Verifier // routed by the unverified `iss`; duplicate issuers are a config error
    Opaque     Verifier   // non-JWT tokens; nil → rejected
    Audience   string     // required (or AnyAudience); checked on every principal
    Public     []string   // A4
    Authorizer Authorizer // A6, optional
    Logger     *slog.Logger
    MeterProvider metric.MeterProvider
}
func New(cfg Config) (*Authenticator, error)
func (a *Authenticator) Authenticate(ctx context.Context, token string) (*Principal, error)
func (a *Authenticator) UnaryServerInterceptor() grpc.UnaryServerInterceptor
func (a *Authenticator) StreamServerInterceptor() grpc.StreamServerInterceptor
func (a *Authenticator) Middleware(next http.Handler) http.Handler // every request must authenticate

func FromContext(ctx context.Context) (*Principal, bool)
func NewContext(ctx context.Context, p *Principal) context.Context
func SubjectFromContext(ctx context.Context) (string, bool) // plugs into ratelimit.Config.Subject

type Authorizer interface{ Authorize(ctx context.Context, method string, p *Principal) error }
func RequireScopes(m map[string][]string) Authorizer // methods not listed need authentication only
```

Errors (`errs` catalog sentinels; the domain comes from the service's `errs.Handler`): `ErrTokenMissing`, `ErrTokenInvalid`, `ErrInsufficientScope`,
`ErrTokenExpired` (all `Unauthenticated`), `ErrForbidden` (`PermissionDenied`),
and `ErrAuthUnavailable` (`Unavailable`, when introspection or JWKS fails).
Why a token was rejected goes to the logs at Debug level, never to the client.

The routing peek reads `iss` from the unverified payload only to choose a
verifier. The chosen verifier then verifies the signature and checks `iss` again.

Security tests: unsigned token, `alg=none`, HS256 signed with an RSA public key
(alg confusion), wrong `iss`, unknown `iss`, wrong `aud`, expired, `nbf` in the
future, tampered payload, unknown `kid`, missing or malformed header, and
`Bearer` matched case-insensitively.

## 5. Package `ratelimit` (core) and module `ratelimit/redis`

Core dependencies: grpc, OTel metric, `errs`, httpsfv.

```go
type Rule struct{ Rate int; Per time.Duration; Burst int } // Burst 0 → Rate
type By int // ByCaller | ByUser | ByAnon | ByDevice | ByIP | ByAnyIP | ByGlobal (R6)
type Limit struct {
    Rule
    By  By
    Key func(context.Context) (string, bool) // custom key, replaces By
}
func DefaultLimits() []Limit // 20/s user, anon, device; 300/s unauthenticated IP
type Policy struct {
    Rule                        // shorthand for Limit{Rule, ByCaller}
    Limits       []Limit        // all must pass; most specific first, stop at first denial
    OnStoreError StoreErrorMode // FallbackLocal (zero value) | Allow | Deny
    DryRun       bool           // count and report, never block
}
type Decision struct {
    Allowed bool; Limit, Remaining int
    RetryAfter, ResetAfter time.Duration
    Degraded    bool // decided by the fallback or by OnStoreError
    Unavailable bool // store failed and the policy said Deny → ErrUnavailable (503)
}
type Store interface{ Allow(ctx context.Context, key string, r Rule) (Decision, error) }

type Config struct {
    Store          Store             // nil → in-memory store (per instance)
    Default        *Policy           // nil → only Policies are limited
    Policies       map[string]Policy // gRPC full method or HTTP route pattern
    Skip           []string          // never limited
    Subject, Anonymous, Device func(context.Context) (string, bool) // R4, R6
    Exempt         []string          // "user:", "anon:", "device:", "ip:<addr|CIDR>"
    Overrides      map[string]Rule   // rule per identity, bucket unchanged
    DryRun, Disabled bool
    OnDecision     func(context.Context, Event)
    Instances      int               // divides the rule in the local fallback; default 1
    StoreTimeout   time.Duration     // default 100ms
    BreakerCooldown time.Duration    // default 1s, single half-open probe
    LocalMaxKeys   int               // LRU bound for the in-memory store, default 100k
    Prefix         string            // default "rl"
    Logger *slog.Logger; MeterProvider metric.MeterProvider
}
func New(cfg Config) (*Limiter, error)
func (l *Limiter) Check(ctx context.Context, policy string) (Decision, error)    // identity from ctx
func (l *Limiter) Allow(ctx context.Context, policy, key string) (Decision, error) // caller-chosen key
func (l *Limiter) UnaryServerInterceptor() grpc.UnaryServerInterceptor
func (l *Limiter) StreamServerInterceptor() grpc.StreamServerInterceptor
func (l *Limiter) Middleware(next http.Handler) http.Handler // policy = r.Pattern
func (l *Limiter) Key(ctx context.Context, remote string) (string, bool)
func (l *Limiter) LimitHTTP(w http.ResponseWriter, r *http.Request, pattern string) error // used by server.WithRateLimit

func NewLocalStore(maxKeys int) *LocalStore // integer-µs GCRA (same arithmetic as the Lua script), sharded LRU, no goroutines
```

When a request is denied the client gets `ResourceExhausted`, reason
`RATE_LIMITED` (the service's domain), and `RetryInfo`. Over HTTP that is
429 with `Retry-After`, `RateLimit-Policy`, `RateLimit` and `X-RateLimit-Limit/Remaining/Reset` (R5).

`ratelimit/redis`: `redis.New(client redis.UniversalClient) (*Store, error)`
implements the plain-typed `ratelimit.Remote` interface
(`GCRA(ctx, key, rate, burst, per) (allowed, remaining, retryAfter, resetAfter, err)`)
without importing core; wire it with `ratelimit.RemoteStore(store)`. It
runs the embedded GCRA Lua script via `EVALSHA` and falls back to `EVAL` on
`NOSCRIPT`. It returns an error if the client does not have
`ContextTimeoutEnabled`, because otherwise the 100ms store timeout cannot be honoured.

Metrics: `kitchen.ratelimit.decisions{policy,result}` (results: allowed /
limited / dry_run / degraded), `kitchen.ratelimit.store.errors`, and
`kitchen.auth.results{result}`.

## 6. Testing

`-race` everywhere and goleak in packages that start goroutines (JWKS,
introspection). miniredis for the Lua script. `httptest` serves a JWKS endpoint
and an RFC 7662 endpoint. `bufconn` runs the interceptor tests. Each package
has runnable `Example*` tests.

## 7. For other areas

- **transport**: build one `errs.Handler` per service. Put its interceptors
  outermost (after recovery), pass `GatewayErrorHandler` to the REST gateway,
  and use `WriteHTTP` in plain HTTP handlers. Convert protovalidate errors with
  `errs/validation.FromError`.
- **Submodules are independent of core**: neither `auth` nor `ratelimit/redis`
  requires `github.com/dungnh3/kitchen`, so each module builds and tidies on
  its own (no fake pseudo-version, no go.work needed).

## Review of 2026-11-21 (group 2, `guard` → `auth`)

Approved by the owner (all "Recommended"):

| # | Finding | Decision |
|---|---|---|
| A1 | The introspection Redis tier trusted Redis: anyone able to write there could store a principal under `sha256(their token)` (auth bypass) or move a genuine entry to another token. | `IntrospectionConfig.CacheKey` (≥ 32 bytes) is required with Redis; entries are `HMAC-SHA256 ‖ JSON` bound to their token hash. A failing entry is ignored (Warn) and the authority is asked; after two unusable answers the authority is asked directly, never the cache. |
| A2 | No optional authentication. | `Config.Optional`: no token → anonymous (no principal, no Authorizer); a token present must be valid (fail closed). |
| A3 | gRPC streams were authenticated once and outlived their token. | The stream context is cancelled at `Principal.ExpiresAt` and the stream ends with `TOKEN_EXPIRED`; `Config.StreamOutlivesToken` opts out. |
| A4 | No token size limit before decoding. | `Config.MaxTokenBytes`, default 8192. |
| A5 (group-1 finding) | Verifiers sharing one Redis with the default prefix shared introspection answers across different rules. | `IntrospectionConfig.Prefix` is required with Redis. |

Reuse review (2026-11-21): interceptors stay hand-written rather than built on go-grpc-middleware/v2 `auth` (owner: kitchen has no go-grpc-middleware dependency and only ~30 lines would move); JWT stays on jwx v3, HTTP on `httpx`, caching on `cache`, errors on `errs`.


## Deep review of 2026-11-24 (`ratelimit`, `ratelimit/redis`)

Owner decisions (all "Recommended"):

| # | Finding | Decision |
|---|---|---|
| RL1 | The Lua script worked in float seconds: `(now - allow_at) / interval` lost precision and reported `remaining` one too low on about half of the allowed requests (seen with a differential test), and `tostring()` on real Redis keeps 14 significant digits, so the stored TAT was rounded to 10µs (high-rate buckets admitted without limit). | Integer microseconds everywhere; the TAT is stored with `string.format("%d")`; replies are integers. `redis.replicate_commands()` is called when it exists (Redis 5/6). |
| RL2 | `LocalStore` was a token bucket (x/time/rate): truncated interval, an infinite limiter for rules over 1 request/ns, a reset when the rule of a key changed, four lock round-trips per call. | `LocalStore` is an integer-µs GCRA with the Lua script's arithmetic; x/time/rate is no longer a dependency. A differential test (synctest + miniredis) requires identical `Decision`s. 437 → 145 ns per call. |
| RL3 | Multi-limit: a request denied by a shared limit (IP, global) has used a token of the specific limits. | Kept and documented (no refund round-trip under a flood; Envoy's default). |
| RL4 | `RateLimit` header draft revision 12 renamed `r`/`t` to `a`/`w`. | Keep revisions 09–11, documented. Sub-second `Per` is now scaled into `q`/`w`. |
| RL5 | IPv6 clients rotate across the /64s of their /56 or /48. | `Config.IPv6Prefix` (32–128), default 64. |

Fixed without a decision (bugs): bucket keys percent-encode `{`, `}`, `%`
(and `:` in policy names) so a route like `GET /files/{id}` no longer puts
every bucket of the route in one Redis Cluster slot and a policy name cannot
reach another policy's keys; the breaker logs once per outage instead of a
warning per request; `OnStoreError: Deny` 503s carry `RetryInfo`, so HTTP
gets `Retry-After`; rules over one request per microsecond or with a burst
tolerance over 2^52 µs are rejected by `New`; `LocalStore.Allow` returns an
error for an invalid rule instead of allowing.

Deferred: one breaker per Limiter, not per Cluster node (documented in
USAGE.md); the REST gateway error path does not forward the RateLimit
response metadata on a 429 (a `server`/`errs` change, routed to the owner).
