# kitchen

Shared Go library (`github.com/dungnh3/kitchen`) for the owner's services:
server, errors, logging, telemetry, auth, messaging, storage, SSE, MCP. It
replaces an earlier kitchen library (clean break; see
`docs/migration/`). No business-domain code belongs here.

The binding decisions live in `docs/decisions.md` (D1–D7). Read the relevant
section before changing a package; this file only summarises them.

## Layout

- Hybrid modules (D1): the core module is the repo root; heavy SDKs live in
  adapter submodules with their own `go.mod` (`make modules` lists them).
  Core never imports a submodule. No `replace` directives in committed
  `go.mod` files.
- Local cross-module work uses an uncommitted, git-ignored `go.work` at the
  root. Never commit `go.work`/`go.work.sum`.
- Submodules are tagged `<dir>/vX.Y.Z`, core `vX.Y.Z`. Tag order is in D1.
- Design docs: `docs/design/<area>.md`. Migration guides:
  `docs/migration/<area>.md`.

## Commands

Every target loops over all modules:

```bash
make vet
make test     # go test -race -count=1 ./... per module
make lint     # golangci-lint v2, config in .golangci.yml
make tidy
```

- For a single package: `go test -race -count=1 ./pubsub/...` (run it from the
  module that owns the package).
- Postgres-backed tests skip unless `KITCHEN_TEST_PG_DSN` is set.
- Scaffold golden files: `cd zen && go test ./... -update`.
- `consistency_test.go` at the root checks that server, pubsub and cache all
  log in one schema. Keep it green when you touch logging.

A change is done when `make vet test lint` passes for every module.

## Rules

### One canonical package per concern (D7)

Before writing anything, check the D7 table in `docs/decisions.md`. If
kitchen already has a package for the concern, use it. Do not reimplement it,
and do not call the library behind it.

| Concern | Canonical package |
|---|---|
| Logging | `log` |
| Errors | `errs` |
| Request metadata | `context` (import as `xcontext`) |
| i18n | `i18n` |
| Telemetry | `otelx` |
| HTTP client | `httpx` |
| gRPC client | `grpcx` |
| HTTP/gRPC server | `server` |
| Redis | `redisx` |
| Backoff | `timex` |
| Cache | `cache` |
| Locks | `lease` |
| Rate limiting | `ratelimit` |
| Auth/JWT | `auth` |
| Messaging | `pubsub` |
| Object storage | `objstore` |
| PostgreSQL / MySQL / MongoDB | `pgx` / `mysqlx` / `mongox` |
| Migrations | `migrate` |
| SSE | `sse` |
| Web page metadata | `webmeta` |
| MCP servers | `mcpkit` |

For collections, use stdlib `slices`/`maps`, then `samber/lo`.

depguard and forbidigo enforce this; their messages start with `D7`. Do not
silence them with `//nolint`. Only adapter submodules may use the library
they adapt.

For a new concern, research what already exists and present the options to
the owner before writing code. Preference order: a kitchen package, then the
stdlib, then a mature library behind a thin kitchen wrapper, then custom code
(only with a clear reason). A new concern gets its own canonical package and
a D7 row before a second package needs it.

### Logging (D5)

- Packages take a `*log.Logger` (`Config.Logger` or `WithLogger`). nil means
  `log.Default()`; use `log.OrDefault`.
- Build attributes with `log.String`, `log.Err`, and so on.
- `log/slog` is imported only inside `log/`.
- Only `main` calls `log.Setup`; library code never does.
- Tests use `log.Discard()`.

### Library conventions (D5)

- No globals, no `init()` side effects.
- No `panic` or `log.Fatal` in constructors; return errors.
- Every I/O function takes `ctx` as its first argument.
- Long-running components expose `Run(ctx) error` and/or `Close(ctx) error`,
  and no goroutine outlives them. Check this with goleak.
- Use stdlib error wrapping (`%w`, `errors.Is/As/Join`).
- Instrument with the OTel API, taking provider options that default to the
  globals.
- Each package uses either functional options or a `Config` struct, not both.
- Tests:
  - must pass with `-race`;
  - table-driven;
  - use `testing/synctest` instead of `time.Sleep`;
  - use `httptest`, `bufconn` or `miniredis` instead of real network services.
- Each package has a doc comment and runnable `Example*` tests.
- Each top-level package has a `USAGE.md` that follows
  `docs/USAGE_TEMPLATE.md`. When you change a package's API, defaults or
  integration, update its `USAGE.md` in the same commit.

### Security defaults

Security defaults stay safe and fail closed:
- auth denies by default;
- `httpx` and `webmeta` keep their SSRF guards;
- TLS is required by default for databases;
- unknown errors are sanitised before they reach clients.

Never weaken a default to make a test pass.

### Wire contracts

Error JSON (AIP-193), HTTP status mapping, log schema keys, SSE event format
and pubsub settlement semantics are contracts with services and clients. Any
change to them must be recorded in `docs/migration/<area>.md`.

## Workflow

- Do not guess the owner's intent. Ask a question with the options, putting
  your recommendation first with its pros and cons. Prefer the option that is
  safest for robustness and security.
- Commit to local `main` (or a worktree branch that you rebase onto it). Never
  push branches or tags; the owner pushes and tags manually.
- Some packages have their own `CLAUDE.md` with package-specific invariants.
  Read it before editing that package.
