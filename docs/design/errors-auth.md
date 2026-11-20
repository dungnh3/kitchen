# Design: errors, i18n, auth, rate limiting

Area: **errors-auth**. Status: approved by the owner on 2026-11-20.
Cross-cutting rules are in [../decisions.md](../decisions.md); the review of the
legacy packages is in [../review/05-errors-auth-ratelimit.md](../review/05-errors-auth-ratelimit.md).

kitchen is a personal, general-purpose library. Nothing here is shaped by the
contracts of services that used the legacy library (old error JSON, business
error codes, existing token issuers).

## 1. Decisions (agreed with the owner)

| # | Topic | Decision |
|---|---|---|
| E1 | Wire error details | Standard `google.rpc` types only (AIP-193): `ErrorInfo{reason, domain, metadata}`, `LocalizedMessage`, `BadRequest`, `RetryInfo` from `genproto/googleapis/rpc/errdetails`. No kitchen error proto, so no `proto/` dir or buf config in this area. |
| E2 | HTTP status | Derived from the gRPC code only. No override, nothing about HTTP on the wire. |
| E3 | HTTP error body | AIP-193 envelope `{"error":{"code","message","status","details"}}`. `errs` renders it; transport calls it. |
| E4 | `errs` API | Immutable sentinels; `errors.Is` matches by reason + domain (also after `Wrap`/`With`); the cause is for logs only. Ships a minimal set of generic sentinels; no business errors. |
| E5 | Validation | No proto. `errs.BadRequest(...FieldViolation)` and `errs.FromViolationsProto(proto.Message)` turn protovalidate results into `InvalidArgument` + `BadRequest` without importing protovalidate. The validation interceptor belongs to transport. |
| E6 | Unknown errors | A non-`errs`, non-status error from a handler is sanitized to `Internal` / `INTERNAL` / `"internal error"`; the cause is logged with slog. Existing status errors keep all details (e.g. `RetryInfo`). |
| I1 | Catalogs | `i18n` embeds en + vi for the generic `errs` reasons; apps add their own catalogs from an `fs.FS`. The bundle is immutable once built. Message ID = `ErrorInfo.reason`. |
| I2 | Locale source | Standard `Accept-Language` (HTTP header; gRPC metadata `accept-language` and `grpcgateway-accept-language`). No custom `locale` key. |
| A1 | Fail-closed | Every token is verified (signature, `exp`/`nbf`/`iss`/`aud`, algorithm allow-list). Unknown issuer, no verifier, unsigned, `alg=none`, expired → `Unauthenticated`. There is no "trust upstream" mode. |
| A2 | Verifiers | Small `Verifier` interface plus built-ins: JWKS (auto-refresh), static keys (HMAC / public keys, several kids for rotation), introspection (RFC 7662 client or a custom func) with a cache. |
| A3 | JWT library | `github.com/lestrrat-go/jwx/v3` for everything (JWS, JWT validation, `jwk.Cache`). |
| A4 | Public methods | Deny by default. `Config.Public` is an explicit allow-list: an exact full method, or a `/pkg.Service/` prefix. No name regexes (the old `*Internal*Service` bypass is gone). |
| A5 | Introspection cache | TTL cache keyed by SHA-256 of the token. Active results live 30s by default, never past the token `exp`; inactive results live 5s. Concurrent lookups are coalesced with singleflight, the cache is a bounded LRU, and an endpoint error fails closed (`Unavailable`). |
| A6 | Authorization | `Authorizer` hook, plus a built-in `RequireScopes(method → scopes)` that reads the `scope`/`scp` claim. A missing scope → `PermissionDenied` (403). |
| R1 | Store outage | Per-policy `OnStoreError`: `FallbackLocal` (default) / `Allow` / `Deny`. The circuit breaker lives in the limiter. The local fallback divides the rule by `Config.Instances`. |
| R2 | Metrics | OTel metric API (`MeterProvider` option, default global). No Prometheus package. |
| R3 | GCRA | Own embedded Lua script (algorithm after go-redis/redis_rate, BSD-2), run with `EVALSHA`. No redis_rate dependency. |
| R4 | Rate-limit key | Pluggable. Default: the subject if `Config.Subject` returns one (wire `auth.SubjectFromContext`), else the client IP (IPv6 collapsed to /64; `X-Forwarded-For` honoured only from `TrustedProxies`). |
| M1 | Modules | Core: `errs`, `i18n`, `ratelimit`. Submodules: `auth` (jwx), `ratelimit/redis` (go-redis). |
| G1 | Naming | No legacy organisation names anywhere in code, docs or tests. |

## 2. Package `errs` (core, leaf)

Dependencies: `google.golang.org/grpc` (codes, status), `genproto/googleapis/rpc`
(errdetails), `google.golang.org/protobuf`, and the stdlib. No other kitchen package.

```go
type Error struct{ /* code, reason, domain, message, metadata, details, cause: all immutable */ }

func New(code codes.Code, reason string, opts ...Option) *Error
func Wrap(cause error, sentinel *Error, opts ...Option) *Error // copy with cause (+ opts)
func (e *Error) With(opts ...Option) *Error                    // copy with opts

// Options
func Domain(d string) Option
func Message(m string) Option              // developer-facing, client-safe English text
func Meta(key, value string) Option         // ErrorInfo.metadata
func Details(ds ...proto.Message) Option    // extra google.rpc details

func (e *Error) Error() string          // "REASON: message: cause"
func (e *Error) Unwrap() error          // cause
func (e *Error) Is(target error) bool   // same reason and (target.domain == "" || same domain)
func (e *Error) GRPCStatus() *status.Status // code, message, [ErrorInfo, details...]; never the cause
func (e *Error) Code() codes.Code; Reason() string; Domain() string; Message() string
func (e *Error) Metadata() map[string]string // copy

func Code(err error) codes.Code          // nil→OK, ctx errors→Canceled/DeadlineExceeded, *Error, GRPCStatus(), else Unknown
func HTTPStatus(err error) int
func HTTPStatusFromCode(c codes.Code) int // grpc-gateway table: 499 for Canceled, 409 for Aborted, ...
func Info(err error) (*errdetails.ErrorInfo, bool) // works on received status errors too
func ToStatus(err error) (st *status.Status, sanitized bool) // E6

// HTTP (E3)
func WriteHTTP(w http.ResponseWriter, err error) // body, Content-Type, Retry-After from RetryInfo, WWW-Authenticate on 401
func MarshalHTTP(st *status.Status) (body []byte, httpStatus int)

// Validation (E5)
type FieldViolation struct{ Field, Description, Reason string }
func BadRequest(vs ...FieldViolation) *Error
func FromViolationsProto(m proto.Message) *Error // reads buf.validate.Violations via protoreflect

// gRPC server side: sanitize, log, and localize.
type Localizer interface {
    Localize(ctx context.Context, reason string) (locale, message string, ok bool)
}
type ServerConfig struct{ Logger *slog.Logger; Localizer Localizer }
func UnaryServerInterceptor(cfg ServerConfig) grpc.UnaryServerInterceptor
func StreamServerInterceptor(cfg ServerConfig) grpc.StreamServerInterceptor
```

Generic sentinels have an empty domain, so they match an error with the same
reason in any domain: `ErrInvalidArgument`, `ErrNotFound`, `ErrAlreadyExists`,
`ErrPermissionDenied`, `ErrUnauthenticated`, `ErrResourceExhausted`,
`ErrFailedPrecondition`, `ErrAborted`, `ErrOutOfRange`, `ErrUnimplemented`,
`ErrInternal`, `ErrUnavailable`, `ErrDeadlineExceeded`, `ErrCanceled`. Each
reason is the code name (e.g. `NOT_FOUND`).

The interceptor logs at Error level, including the cause, for sanitized errors
and for `Internal`/`Unknown`/`DataLoss` errors. It adds a `LocalizedMessage`
only when the reason is in the catalog and the status does not already carry one.

HTTP sample (`errs.WriteHTTP`):

```json
HTTP/1.1 404 Not Found
{"error": {"code": 404, "message": "agent not found", "status": "NOT_FOUND",
  "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
               "reason": "AGENT_NOT_FOUND", "domain": "agents.example.com",
               "metadata": {"agent_id": "a1"}}]}}
```

## 3. Package `i18n` (core)

Dependencies: `github.com/nicksnyder/go-i18n/v2`, `golang.org/x/text/language`.

```go
func New(def language.Tag, opts ...Option) (*Bundle, error)
func WithDefaults() Option                                 // embedded en + vi for errs reasons
func WithFS(fsys fs.FS, patterns ...string) Option         // *.json / *.toml / *.yaml catalogs
func WithMessages(tag language.Tag, msgs map[string]string) Option

func (b *Bundle) Message(id string, acceptLanguage ...string) (msg string, tag language.Tag, ok bool)
func (b *Bundle) Match(acceptLanguage ...string) language.Tag
func (b *Bundle) Localize(ctx context.Context, id string) (locale, msg string, ok bool) // implements errs.Localizer

func WithAcceptLanguage(ctx context.Context, v string) context.Context // HTTP path
func AcceptLanguage(ctx context.Context) []string // ctx value, then gRPC metadata
```

The bundle is immutable after `New` and safe for concurrent use. A missing
message falls back to the default language and then reports `ok=false`.
Nothing panics.

## 4. Module `auth` (submodule `github.com/dungnh3/kitchen/auth`)

Dependencies: core (`errs`), `lestrrat-go/jwx/v3`, `golang.org/x/sync`, grpc, OTel metric.

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
    Audience   string     // checked on every principal; verifiers may also check their own
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

Errors (domain `auth.kitchen`): `ErrTokenMissing`, `ErrTokenInvalid`,
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

Core dependencies: `golang.org/x/time/rate`, grpc, OTel metric, `errs`.

```go
type Rule struct{ Rate int; Per time.Duration; Burst int } // Burst 0 → Rate
type Policy struct {
    Rule
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
    Subject        func(context.Context) (string, bool) // R4
    TrustedProxies []netip.Prefix
    Instances      int               // divides the rule in the local fallback; default 1
    StoreTimeout   time.Duration     // default 100ms
    BreakerCooldown time.Duration    // default 1s, single half-open probe
    LocalMaxKeys   int               // LRU bound for the in-memory store, default 100k
    Prefix         string            // default "rl"
    Logger *slog.Logger; MeterProvider metric.MeterProvider
}
func New(cfg Config) (*Limiter, error)
func (l *Limiter) Allow(ctx context.Context, policy, key string) (Decision, error)
func (l *Limiter) UnaryServerInterceptor() grpc.UnaryServerInterceptor
func (l *Limiter) StreamServerInterceptor() grpc.StreamServerInterceptor
func (l *Limiter) Middleware(next http.Handler) http.Handler // policy = r.Pattern
func (l *Limiter) Key(ctx context.Context, remote string, xff []string) (string, bool)

func NewLocalStore(maxKeys int) *LocalStore // GCRA via x/time/rate, sharded LRU, no goroutines
```

When a request is denied the client gets `ResourceExhausted`, reason
`RATE_LIMITED` (domain `ratelimit.kitchen`), and `RetryInfo`. Over HTTP that is
429 with `Retry-After` and `X-RateLimit-Limit/Remaining/Reset`.

`ratelimit/redis`: `redis.New(client redis.UniversalClient) (*Store, error)`
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

- **transport**: render HTTP errors with `errs.WriteHTTP` (or `errs.MarshalHTTP`).
  Map codes with `errs.HTTPStatusFromCode`. Put `errs.UnaryServerInterceptor`
  outermost (after recovery) so every error is sanitized. Turn protovalidate
  results into errors with `errs.FromViolationsProto(verr.ToProto())`.
- **Submodule publishing**: `auth` and `ratelimit/redis` require the core
  module. Core must be tagged before CI can run `go mod tidy -diff` in them;
  until then, develop with a local `go.work`.
