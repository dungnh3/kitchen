# Cross-cutting decisions

These decisions apply to every package in `github.com/dungnh3/kitchen`.
They were agreed with the owner on 2026-11-20 before the per-area redesign
started. Per-area decisions live in `docs/design/<area>.md`.

## D1. Module layout: hybrid

- **Core module** `github.com/dungnh3/kitchen` (root `go.mod`): packages whose
  dependencies are light (stdlib, grpc, protobuf, otel API, small libs).
- **Adapter submodules**, each with its own `go.mod`, for anything that pulls a
  heavy SDK or a backend client: e.g. `pubsub/gcp`, `sse`, `blob/gcs`,
  `mcpkit`, `cmd/kitchen`, OTel exporters for a specific vendor.
- Submodules are tagged `<dir>/vX.Y.Z` (e.g. `pubsub/gcp/v0.1.0`); core is
  tagged `vX.Y.Z`. No date-in-patch versions, no pseudo-version pinning in docs.
- No `replace` directives in committed `go.mod` files. Use a local, uncommitted
  `go.work` for cross-module development (it is git-ignored).
- No cycles: a submodule may depend on core; core never depends on a submodule.

## D2. Go version

- `go 1.26.0` directive in every `go.mod`. Use modern stdlib freely:
  `log/slog`, `slices`, `maps`, `cmp`, `iter`, `sync.WaitGroup.Go`,
  `testing/synctest`, `http.ResponseController`, method-pattern `ServeMux`.
- Do not add a library for what the stdlib now does.

## D3. Compatibility: clean break

- The new API is designed freely; it does not have to keep names or shapes from
  `github.com/vulcanlabsvn/kitchen`.
- Every area ships `docs/migration/<area>.md`: old symbol → new symbol, behaviour
  changes (defaults, wire formats, JSON keys, HTTP codes), and a codemod or
  sed/gofmt -r recipe where mechanical.

## D4. Git flow

- `main` is protected; all work lands via PR.
- One branch/PR per area (`feat/<area>`), opened from a worktree.
- `go.mod`/`go.sum` conflicts in the core module are expected when areas merge;
  resolve by rebasing and running `go mod tidy`.

## D5. Library conventions (all areas)

- **Logging:** packages accept a `*slog.Logger` (nil → `slog.Default()`); they
  never import a kitchen logging package and never create package-level loggers.
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
- **Docs:** package doc comment + runnable `Example*` tests; no multi-thousand
  line USAGE.md files.
- **No business-domain code** (orders, payments, ...) in kitchen.
- Interfaces between areas that are not merged yet: define a small local
  interface in the consumer package rather than importing an unmerged package.

## D6. Area ownership (to avoid parallel edit conflicts)

| Area | Old packages | Directories owned in the new repo (proposed) |
|---|---|---|
| observability | `l`, `log`, `tracing` | `log/`, `otel/` (+ vendor exporter submodules) |
| transport | `server`, `grpc`, `nap`, `kresty`, `healthcheck`, `context`, `pb`/`proto` (health) | `server/`, `httpx/`, `reqctx/` (names to be confirmed in design) |
| messaging | `pubsub`, `syncmsg`, `syncbrokers`, `coordinators`, `queue`, `scheduler` | `pubsub/`, `pubsub/gcp/`, other messaging dirs |
| sse | `sse` | `sse/` (submodule) |
| errors-auth | `ecode`, `errors`, `localization`, `guard`, `rlimit`, `pb`/`proto` (errors) | `errs/`, `i18n/`, `auth/`, `ratelimit/`, `proto/kitchen/errors/` |
| data-utils | `cache`, `storage`, `migrations`, `uploader`, `utils`, `parallel`, `crawl` | `cache/`, `pg/`, `migrate/`, `blob/`, `xslices/`, `vn/`, `timex/`, `webmeta/` |
| codegen-mcp | `mcpkit`, `nest`, `gen`, `vulgen-mcp`, `vulgen-service` | `mcpkit/` (submodule), `cmd/kitchen/` (submodule), `templates/` |

Final directory names are decided in each area's design doc. An area must not
edit another area's directories; shared files (root `go.mod`, `README.md`,
CI) only get additive changes.
