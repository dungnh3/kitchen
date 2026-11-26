# Cross-cutting decisions

These decisions apply to every package in `github.com/dungnh3/kitchen`.
They were agreed with the owner on 2026-11-20 before the per-area redesign
started. Per-area decisions live in `docs/design/<area>.md`.

## D1. Module layout: hybrid

- **Core module** `github.com/dungnh3/kitchen` (root `go.mod`): packages whose
  dependencies are light (stdlib, grpc, protobuf, otel API, small libs).
- **Adapter submodules**, each with its own `go.mod`, for anything that pulls a
  heavy SDK or a backend client: e.g. `pubsub/gcp`, `sse`, `objstore/gcs`, `objstore/s3`,
  `mcpkit`, `zen`, OTel exporters for a specific vendor.
- Submodules are tagged `<dir>/vX.Y.Z` (e.g. `pubsub/gcp/v0.1.0`); core is
  tagged `vX.Y.Z`. No date-in-patch versions, no pseudo-version pinning in docs.
- No `replace` directives in committed `go.mod` files. Use a local, uncommitted
  `go.work` for cross-module development (it is git-ignored).
- No cycles: a submodule may depend on core; core never depends on a submodule.
- **Tag order** (a submodule's required versions must exist first):
  1. core `vX.Y.Z`;
  2. `log/gormlog/vX.Y.Z` (requires core) and `auth/vX.Y.Z`;
  3. submodules that require them: `pgx/gorm` and `mysqlx/gorm` (core +
     `log/gormlog`), `mongox`, `migrate` (core), `objstore/gcs`,
     `objstore/s3`, `sse`, and every other submodule requiring core.

## D2. Go version

- `go 1.26.0` directive in every `go.mod`. Use modern stdlib freely:
  `log/slog`, `slices`, `maps`, `cmp`, `iter`, `sync.WaitGroup.Go`,
  `testing/synctest`, `http.ResponseController`, method-pattern `ServeMux`.
- Do not add a library for what the stdlib now does.

## D3. Compatibility: clean break

- The new API is designed freely; it does not have to keep names or shapes from
  the old kitchen library.
- Every area ships `docs/migration/<area>.md`: old symbol → new symbol, behaviour
  changes (defaults, wire formats, JSON keys, HTTP codes), and a codemod or
  sed/gofmt -r recipe where mechanical.

## D4. Git flow

- `main` is protected; all work lands via PR.
- One branch/PR per area (`feat/<area>`), opened from a worktree.
- `go.mod`/`go.sum` conflicts in the core module are expected when areas merge;
  resolve by rebasing and running `go mod tidy`.

## D5. Library conventions (all areas)

- **Logging** (owner, 2026-11-22; replaces the earlier `*slog.Logger` rule):
  every package logs through `github.com/dungnh3/kitchen/log`, so a change of
  format, schema or backend is made once there and applies everywhere.
  - Packages take a `*log.Logger` (`Config.Logger` or `WithLogger`); nil →
    `log.Default()` (use `log.OrDefault`). `log.Logger` is an alias of
    `slog.Logger`, so it can be passed to libraries that take `*slog.Logger`.
  - Attributes and levels come from kitchen/log (`log.String`, `log.Err`,
    `log.Attr`, `log.LevelWarn`, ...). `log/slog` is imported only inside
    `log/` (tests may when they must). Enforced by depguard and forbidigo in
    `.golangci.yml`.
  - `main` calls `log.Setup(opts)` once: it installs the process logger for
    `log.Default()` (also loggers taken from it earlier), slog's default and
    the standard `log` package. Library packages never call `Setup` or
    `log.SetDefault`. Without Setup, `log.Default()` is kitchen JSON built from
    the environment, never Go's text handler.
  - `kitchen/context` stores `request_id` / `user_id` as log attributes
    (`log.WithAttrs`), so every record logged with a request context carries
    them. Tests use `log.Discard()`.
  - The one process-wide default lives in kitchen/log; no other package keeps
    a package-level logger.
- **No global state, no `init()` side effects, no `log.Fatal`/`panic` in
  constructors.** Return errors. `Must*` variants only where clearly useful.
- **Context first:** every I/O function takes `ctx context.Context` first.
- **Lifecycle:** long-running components expose `Run(ctx) error` (returns after
  drain) and/or `Close(ctx) error`; goroutines never outlive them (verify with
  `go.uber.org/goleak` in tests).
- **Telemetry:** instrument via the OTel API (`go.opentelemetry.io/otel`),
  taking `TracerProvider`/`MeterProvider` options that default to the globals.
  Follow current semantic conventions.
- **Errors:** stdlib wrapping (`%w`, `errors.Is/As/Join`); no `pkg/errors`.
- **Options:** functional options (`New(opts ...Option)`) or a plain `Config`
  struct with documented zero-value defaults; pick one per package.
- **Tests:** `go test -race` passes; table tests; `httptest`/`bufconn`/
  miniredis/emulators; `testing/synctest` instead of `time.Sleep`.
- **Docs** (owner, 2026-11-22): each package has a doc comment and runnable
  `Example*` tests. Each top-level package also has a `USAGE.md`, which
  follows `docs/USAGE_TEMPLATE.md`:
  - It covers usage from the defaults up to advanced cases, plus how the
    package integrates with other kitchen packages.
  - Its snippets come from `Example*` tests.
  - It runs about 150–400 lines.
  - Adapter subpackages are documented in their parent package's `USAGE.md`.
- **No business-domain code** (orders, payments, ...) in kitchen.
- Interfaces between areas that are not merged yet: define a small local
  interface in the consumer package rather than importing an unmerged package.

## D6. Area ownership (to avoid parallel edit conflicts)

| Area | Old packages | Directories owned in the new repo (proposed) |
|---|---|---|
| observability | `l`, `log`, `tracing` | `log/`, `otel/` (+ vendor exporter submodules) |
| transport | `server`, `grpc`, `nap`, `kresty`, `healthcheck`, `context`, `pb`/`proto` (health) | `server/`, `httpx/`, `context/` |
| messaging | `pubsub`, `syncmsg`, `syncbrokers`, `coordinators`, `queue`, `scheduler` | `pubsub/`, `pubsub/gcp/`, other messaging dirs |
| sse | `sse` | `sse/` (submodule) |
| errors-auth | `ecode`, `errors`, `localization`, `guard`, `rlimit`, `pb`/`proto` (errors) | `errs/`, `i18n/`, `auth/`, `ratelimit/`, `proto/kitchen/errors/` |
| data-utils | `cache`, `storage`, `migrations`, `uploader`, `utils`, `parallel`, `crawl` | `cache/`, `pgx/`, `mysqlx/`, `mongox/`, `migrate/`, `objstore/` (was `blob/`), `vn/`, `timex/`, `webmeta/` |
| codegen-mcp | `mcpkit`, `nest`, `gen`, MCP and service scaffolders | `mcpkit/` (submodule), `zen/` (submodule), `templates/` |

Final directory names are decided in each area's design doc. An area must not
edit another area's directories; shared files (root `go.mod`, `README.md`,
CI) only get additive changes.

## D7. Canonical packages (owner, 2026-11-22)

D5's logging rule generalised: every concern has **one canonical kitchen
package**, and every other package reuses it. Nobody reimplements the concern
or calls the library behind it directly. Changing how kitchen does something
(a backend, a default, a security fix) is then an edit to one package, and
every caller follows.

- **Reuse first.** Before implementing anything, look in this table. Then, in
  order: the Go standard library, a mature library wrapped by a new canonical
  package, and custom code last.
- **New concerns.** A concern gets a canonical package (with a row here and a
  lint rule) before a second package needs it.
- **Adapters.** Adapter submodules of the canonical package (`pubsub/gcp`,
  `objstore/s3`, `ratelimit/redis`, ...) may use the library they adapt.
  Packages may still *accept* a library's types (`redis.UniversalClient`,
  `*pgxpool.Pool`); they must not construct or configure them.
- **Enforcement.** `.golangci.yml` checks this: depguard covers imports and
  forbidigo covers constructors. Messages start with `D7`. Tests may use the
  libraries directly.

| Concern | Canonical package | Library inside | Not allowed outside it |
|---|---|---|---|
| Logging | `log` (+ `log/gormlog` for gorm) | slog, zap | `log/slog`, `log`, zap, zerolog, logrus; `slog.New`/`Default`/`SetDefault` (D5) |
| Errors, gRPC/HTTP status mapping | `errs`; DB adapters map into `errs` (`pgx`, `mysqlx`, `mongox`) | grpc status, errdetails | `status.New`/`Newf`/`Error`/`Errorf`/`FromProto`; `pkg/errors`, multierr (use `errors.Join`) |
| Request validation | `errs/validation`, `server` (validates every request by default) | protovalidate | protovalidate, go-playground/validator |
| Request metadata (request id, user id, client IP, headers) | `context` (import as `xcontext`) | realclientip-go | realclientip-go |
| i18n | `i18n` | go-i18n | go-i18n |
| Telemetry setup, exporters | `otelx/sdk`, `otelx/gcp`, `otelx/gcp/cloudtrace`, `otelx/prometheus` | OTel SDK, exporters | OTel SDK, exporters, detectors, bridges; `otel.Set*` globals |
| Telemetry instrumentation | `otelx` (`HTTPHandler`, `HTTPTransport`, `GRPCServerHandler`, `GRPCClientHandler`) | otelhttp, otelgrpc | `otelhttp.New*`, `otelgrpc.New*` (the OTel API for spans and metrics is fine everywhere) |
| HTTP client | `httpx` | resty v3 | resty, retryablehttp, req; `http.Client`, `http.DefaultClient`, `http.Get`/`Head`/`Post`/`PostForm` |
| Outbound gRPC client | `grpcx` (`grpcx.New`, + `grpcx/grpcxtest`) | grpc, gobreaker v2 | `grpc.NewClient`, `grpc.Dial`, `grpc.DialContext` (except `server`'s in-process gateway loopback); sony/gobreaker |
| HTTP/gRPC server, gateway, admin port, compression | `server` | grpc, grpc-gateway, gzhttp | `grpc.NewServer`, `http.Server`, `http.ListenAndServe`/`Serve`; grpc-gateway, gzhttp |
| Redis client | `redisx` (`redisx.Open`) | go-redis v9 | `redis.NewClient`/`NewUniversalClient`/`NewClusterClient`/`NewFailoverClient`/`NewRing`/`ParseURL`; rueidis, redigo |
| Backoff, jitter | `timex.Backoff` (HTTP retries: `httpx.WithRetry`) | `math/rand/v2` | `math/rand`, cenkalti/backoff, retry-go |
| Cache (L1 + Redis L2) | `cache` | otter, singleflight | otter, ristretto, go-cache, golang-lru, gocache |
| Distributed lock | `lease` (+ `lease/redis`) | | redislock, redsync |
| Rate limiting | `ratelimit` (+ `ratelimit/redis`) | x/time/rate, GCRA Lua | x/time/rate, redis_rate, ulule/limiter |
| Auth: JWT, JWKS, introspection | `auth` | jwx v3 | jwx, httprc, golang-jwt, go-jose |
| Messaging | `pubsub` (+ `gcp`, `sqs`, `kafka`, `outbox`, `outbox/pgx`, `reply/redis`) | cloud pubsub, SQS, franz-go | those SDKs, other Kafka clients, watermill |
| Object storage | `objstore` (+ `gcs`, `s3`) | GCS, S3 SDKs | those SDKs, gocloud.dev |
| PostgreSQL | `pgx` (`pgx.Open`, + `pgx/gorm`); import jackc/pgx as `jpgx` next to it | jackc/pgx v5 | `pgxpool.New`/`NewWithConfig`, jackc `pgx.Connect`, `sql.Open`, lib/pq, gorm postgres driver |
| MySQL | `mysqlx` (`mysqlx.Open`, + `mysqlx/gorm`) | go-sql-driver | go-sql-driver/mysql, gorm mysql driver, `sql.Open` |
| MongoDB | `mongox` | mongo-driver v2 | mongo-driver |
| Migrations | `migrate` | goose | goose, golang-migrate |
| Server-sent events | `sse` | | r3labs/sse, go-sse |
| Web page metadata | `webmeta` | x/net/html | x/net/html, goquery |
| MCP servers | `mcpkit` (generator: `zen`) | MCP go-sdk | MCP go-sdk |
| Collections | stdlib `slices`/`maps`, then `github.com/samber/lo`; no in-house `xslices` | | go-funk, pie |
