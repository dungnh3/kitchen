# Design: codegen-mcp (`mcpkit`, `cmd/kitchen`)

Status: accepted 2026-11-20. Old packages covered: `mcpkit`, `nest`, `gen`,
and the two one-shot scaffolders (MCP server, gRPC service). Migration:
[docs/migration/codegen-mcp.md](../migration/codegen-mcp.md).

## 1. Summary

| Old | New | Module |
|---|---|---|
| `mcpkit` (builder, registries, own `http.Server`, SSE) | `mcpkit`: thin typed layer over go-sdk, `http.Handler` only | `github.com/dungnh3/kitchen/mcpkit` (submodule) |
| – | `mcpkit.Verifier`: adapter from kitchen `auth.Verifier` (JWKS, static keys, introspection) to the SDK's token verifier; `mcpkit/jwtauth` was removed (owner, 2026-11-21: one JOSE stack in kitchen) | same submodule (requires core and `auth`) |
| – | `Server.Service(pattern)`: mounts the endpoint on kitchen's `server` (`server.Service` + `HTTPRegistrar`) | same submodule |
| – | `mcpkit/mcpkittest`: in-memory client for tool tests | same submodule |
| `nest` (fluent HTTP client) | dropped; use kitchen `httpx` + `golang.org/x/oauth2` (owner, 2026-11-21) | – |
| `gen` (add CRUD model) | dropped | – |
| MCP scaffolder + service scaffolder | one CLI: `kitchen new mcp` and `kitchen new service` (§3a, 2026-11-22) | `github.com/dungnh3/kitchen/cmd/kitchen` (submodule) |

Versions verified on 2026-11-20: go-sdk v1.8.0 (latest stable; v1.9.0-pre.1
exists), MCP spec 2026-07-28 (SDK falls back to 2025-11-25 automatically),
huh v2.0.3 (`charm.land/huh/v2`), OTel Go v1.47.0 (semconv
v1.41.0 has `mcpconv`), Go 1.27.4 latest / 1.26 supported, Alpine 3.24,
`gcr.io/distroless/static-debian13:nonroot`, actions/checkout v7.0.1,
actions/setup-go v7.1.0, golangci-lint-action v9.3.0, golangci-lint v2.15.0,
docker/setup-buildx-action v4.4.1, docker/build-push-action v7.4.0 (SHAs from
`git ls-remote`).

## 2. `mcpkit`

### API

```go
srv, err := mcpkit.New(&mcp.Implementation{Name: "notes", Version: v},
    mcpkit.WithLogger(logger),                 // *log.Logger (kitchen log); nil -> log.Default()
    mcpkit.WithToolTimeout(2*time.Minute),     // default none; per tool: mcpkit.Timeout(d)
    mcpkit.WithAuth(mcpkit.Auth{...}),         // bearer auth + RFC 9728 metadata
    mcpkit.WithAllowedOrigins("https://app.example.com"),
    mcpkit.WithStateful(idle),                 // default stateless
    mcpkit.WithMaxRequestBodyBytes(n),         // default SDK 4 MiB
    mcpkit.WithTracerProvider(tp), mcpkit.WithMeterProvider(mp),
    mcpkit.WithMiddleware(mw...),              // SDK receiving middleware
    mcpkit.WithServerOptions(func(*mcp.ServerOptions){...}),
)
mcpkit.AddTool(srv, &mcp.Tool{Name: "search"}, handler)   // mcp.ToolHandlerFor[In, Out]

mux.Handle("/mcp", srv.Handler())
mux.Handle("GET "+srv.MetadataPath(), srv.MetadataHandler())
srv.RunStdio(ctx, mcpkit.EnvToken("UPSTREAM_TOKEN"))
srv.Run(ctx, transport, tokenSource)        // any mcp.Transport
srv.SDK()                                   // *mcp.Server escape hatch

mcpkit.TokenFrom(ctx)      // raw bearer / stdio token, for upstream calls
mcpkit.TokenInfoFrom(ctx)  // verified auth.TokenInfo (HTTP only)
mcpkit.PassThrough()       // verifier for upstream OAuth tokens
```

### Behaviour

- **Typed tools.** `AddTool` delegates to `mcp.AddTool[In,Out]`: schemas are
  inferred from Go types, input is validated, `Out` becomes structured
  content, a returned `error` becomes `IsError` (the model can recover);
  only `*jsonrpc.Error` stays a protocol error.
- **Timeout** is `context.WithTimeoutCause` around the handler, with no extra
  goroutine. A deadline hit becomes the tool error `tool "x" timed out after d`.
- **Panic recovery** is SDK receiving middleware, so it covers every method.
  A tools/call panic returns `IsError` "internal error"; other methods return
  JSON-RPC -32603. The panic value and stack go only to the log.
- **Telemetry**: one SERVER span per request named `{mcp.method.name} {target}`,
  with `gen_ai.tool.name`, `gen_ai.operation.name=execute_tool`,
  `gen_ai.prompt.name`, `mcp.resource.uri`, `mcp.session.id` and `error.type`
  (`tool_error` for IsError, JSON-RPC code otherwise). It also records the
  `mcp.server.operation.duration` histogram (semconv v1.41 `mcpconv`).
- **HTTP handler** wraps, from outside in:
  1. CORS: exact-origin allow-list. An empty list sends no CORS headers; a denied preflight gets 403.
  2. `http.CrossOriginProtection`: rejects cross-site browser writes; allowed origins are trusted.
  3. Raw-token capture.
  4. `auth.RequireBearerToken`: 401/403 with a `WWW-Authenticate` header whose `resource_metadata` points at the metadata URL.
  5. The SDK `StreamableHTTPHandler`: `Stateless` by default, `PropagateRequestCancellation`, the SDK body limit, and DNS-rebinding protection (SDK default).
- **No `http.Server`** in mcpkit. The app (or kitchen `server/`) owns it and
  must not set `WriteTimeout`, which would cut long-running streamed tool
  responses.
- **stdio**: the token comes from a `TokenSource` (env), never from `_meta`.
- **Transports**: stdio and Streamable HTTP. Legacy HTTP+SSE is not offered.
- `tools/list` order is sorted by name (the SDK sorts features), so it is stable
  across restarts.

### Auth: kitchen `auth` through `Verifier` (owner, 2026-11-21)

`mcpkit.Verifier(v auth.Verifier, audience string)` adapts any kitchen
verifier (`auth.NewJWKS`, `auth.NewStatic`, `auth.NewIntrospection`,
`auth.All`). `audience` is the MCP resource URL and is required: every token
must list it in `aud` (RFC 8707) even when the kitchen verifier has no
`Audience`. Rejections are `auth.ErrInvalidToken` (401, body "invalid token":
the reason is never sent). `auth.ErrAuthUnavailable` fails closed with the errs
UNAVAILABLE response (503, `AUTH_UNAVAILABLE`, cause logged), the same as
`server` routes, so an IdP outage is not reported as a bad token. The SDK's
`RequireBearerToken` can only answer 500 there and writes `err.Error()` as the
body, so mcpkit wraps it and rewrites that one response with the
`mcpkit.WithErrors` handler (default `errs.NewHandler` with the server logger;
pass the handler given to `server.WithErrors`). The verified
`*auth.Principal` is put in the request context, so tools use
`auth.FromContext`. JWKS discovery from issuer metadata (old `jwtauth`) is
not built in: pass the JWKS URL (or read it with the SDK's
`auth.GetAuthServerMetadata`).

### Hosting on kitchen's server (owner, 2026-11-21)

`srv.Service("/mcp")` is a `server.Service` + `server.HTTPRegistrar`
(no-op gRPC) that mounts the endpoint and, with `Auth.Metadata`, the RFC 9728
document; `server.WithServices(srv.Service("/mcp"))` then gives timeouts (no
`WriteTimeout`), drain, `/livez`/`/readyz`, access logs and HTTP telemetry.
`Patterns()` lists the mounted patterns for a server-level `auth.Authenticator`'s
`Public` list. Use `server.WithGRPCAddr("")` for an HTTP-only server (no
public gRPC port).

### Tool errors (owner, 2026-11-21)

Only `*errs.Error` messages reach the model (client-fault codes). Any other
error, and `Internal`/`Unknown`/`DataLoss` errs, is logged with the tool name
and returned as "internal error", like `errs.Handler` does for HTTP and gRPC.
`*jsonrpc.Error` passes through as a protocol error.

## 3. `cmd/kitchen`

```
kitchen new mcp -module github.com/acme/mcp-notes [-dir] [-name] [-description]
                [-ci standalone|reusable] [-ci-workflow owner/repo/.github/workflows/x.yml@<sha>]
                [-mcpkit-version v0.1.0] [-kitchen-version v0.1.0] [-yes]
kitchen version
```

- **Prompts:** when stdin is a terminal, `-module` is missing and `-yes` is
  not set, the CLI asks for the missing values with huh v2. Otherwise it uses
  flags only and fails on missing values, which suits CI.
- **Templates:** `embed.FS` under `cmd/kitchen/templates/<kind>/`, rendered with
  `text/template` using `{%`/`%}` delimiters, so `${{ }}` in workflows needs
  no escaping. Only `*.tmpl` files are templated; `.go` output is gofmt'd and
  a format error fails generation.
- **Safe writes:** files are written through `os.Root`, so paths cannot escape
  the target. The CLI refuses a non-empty target directory and never
  overwrites anything.
- **Validation:** `-module` is checked with `x/mod/module.CheckPath`, and the
  name with `^[a-z][a-z0-9-]*[a-z0-9]$`. A reusable workflow must be pinned to
  a 40-character commit SHA.

Generated tree (`kitchen new mcp`):

```
go.mod                       go 1.26.0; mcpkit v0.1.0; go-sdk v1.8.0
cmd/server/main.go           process logger (kitchen log, stderr) first, env config, otelx/sdk, http (kitchen server)|stdio
internal/config/config.go    env + stdlib, validated (https resource URL, issuers)
internal/config/config_test.go
internal/tools/tools.go      typed example tool
internal/tools/tools_test.go mcpkittest in-memory client
Dockerfile                   golang:1.27-alpine3.24 -> distroless static nonroot, netrc BuildKit secret
.github/workflows/ci.yml     SHA-pinned actions (standalone) or a SHA-pinned reusable workflow
Makefile  README.md  .golangci.yml  .gitignore  .dockerignore
```

Tests:
- Golden files: `testdata/golden/<case>/**.golden`; refresh them with `-update`.
- A build test generates both cases into a temp dir and runs `go build`, `go vet` and `go test` on each. It uses a `go.work` that replaces mcpkit with the local module and mcpkit's `go.sum`. It is skipped with `-short` or when offline.
- Validation and CLI exit-code tables.

## 3a. `kitchen new service` (owner, popups 2026-11-22)

```
kitchen new service -module github.com/acme/orders [-dir] [-name] [-description]
                    [-db postgres|none] [-ci standalone|reusable] [-ci-workflow ...]
                    [-kitchen-version v0.1.0] [-no-generate] [-yes]
```

It scaffolds a gRPC + REST (grpc-gateway) service on kitchen packages, then
runs code generation in the new project: `go mod tidy -e`, `buf generate`,
`go mod tidy`. `-no-generate` skips that step and prints it instead. Proto
output depends on the module and package names (the raw descriptor embeds
them), so it cannot ship pre-rendered; the sqlc output does not, so it ships
pre-generated and scaffolding needs no cgo.

Generated tree (`-db postgres`; `-db none` drops the starred files and uses
`memstore`):

```
go.mod                         tool: protoc-gen-go, -go-grpc, -grpc-gateway, -openapiv2
buf.yaml buf.lock buf.gen.yaml buf v2; deps protovalidate + googleapis (locked); local plugins via "go tool"
proto/<name_snake>/v1/item_service.proto   AIP CRUD example (Get/List/Create/Delete), protovalidate rules
gen/                           generated pb, grpc, gateway, openapiv2 (committed)
cmd/server/main.go             log.Setup (kitchen/log), config, serve | migrate up|down|status*
internal/config/               caarlos0/env v11; Load + ValidateServer (auth fail-closed)
internal/app/app.go            otelx/sdk (OTLP when an endpoint is set), pg*, migrate on start*, i18n + errs.Handler, ratelimit,
                               auth (JWKS), admin port (pprof, Prometheus /metrics), server.Run
internal/app/migrate.go*       goose via kitchen migrate, advisory lock, status table
internal/app/app_test.go       whole service on random ports (+ testcontainers postgres*): REST CRUD,
                               validation 400, NOT_FOUND envelope, 401 without/with a bad token
internal/service/              ItemService over store.Store; opaque page tokens; unit tests
internal/store/                Store interface; memstore; pgstore* + queries.sql* + sqlcdb* (sqlc)
migrations/*                   goose SQL, embedded
sqlc.yaml*                     pgx/v5, uuid -> string, timestamptz -> time.Time
Dockerfile Makefile .golangci.yml .github/workflows/ci.yml .github/pull_request_template.md
README.md .gitignore .dockerignore
```

| # | Decision (owner) | Alternatives | Why |
|---|---|---|---|
| S1 | Embedded `text/template`, same engine as `new mcp` | template module + gonew rewrite (gonew is still "highly experimental", last release 2024, no conditionals); copier/cookiecutter (Python) | One pattern, conditionals (`-db`), golden + build tests |
| S2 | The CLI runs generation; `-no-generate` opts out | print next steps only (A18) | The project builds right away; on failure the files stay and the manual steps are printed |
| S3 | `pg` (pgx) + sqlc v1.31.1 | `pg/gorm`; both behind a flag | Typed SQL, no reflection, `pg.MapError`; one template to maintain |
| S4 | caarlos0/env v11.4.2 | stdlib (as `new mcp`), sethvargo/go-envconfig | A service has ~25 variables; zero-dependency, MIT |
| S5 | `server migrate up\|down\|status` + `DB_MIGRATE_ON_START` (default false) | always on start; kmigrate only | Production runs migrations as a separate step; local runs stay one command |
| S6 | Auth `auth.NewJWKS`, fail-closed: `AUTH_ISSUER`, `AUTH_JWKS_URL` (https, http only for localhost), `AUTH_AUDIENCE` required; `AUTH_DISABLED=true` is explicit and refused when `APP_ENV=production`; only `/grpc.health.v1.Health/` is public | add introspection; no auth | Secure by default; an unreachable JWKS stops startup |
| S7 | Plugins are `tool` deps in go.mod; buf v1.75.0, sqlc and golangci-lint run with `go run …@version` | everything as `tool`; BSR remote plugins | Plugin versions match the runtime; buf/sqlc do not pull hundreds of modules into go.mod; no BSR plugin rate limits |
| S8 | Only `-db none` as a feature flag; admin port (pprof, Prometheus via `otelx/prometheus` on a private registry) behind `ADMIN_ADDR`; outbox and Redis are documented in the README | flags for outbox and Redis | Fewer combinations to test |
| S9 | AIP CRUD example + openapiv2 | greeting; openapiv3 (alpha upstream) | Shows sqlc, errs mapping, validation and pagination |
| S10 | CI like `new mcp` plus a generated-code drift check (`make generate` + `git diff`), `buf lint`, `buf breaking` against the PR base | test + lint only | Catches stale code and breaking API changes |
| S11 | Dropped: scheduler, mockery, App Check / guest / mock-user / blacklist middleware, redocly HTML | keep the scheduler | See the migration guide |
| S12 | `main` calls `log.Setup` (stdout) first; every package logs through `kitchen/log` (`log.OrDefault`, `log.Discard` in tests). The template's `.golangci.yml` mirrors kitchen's D5/D7 rules (depguard + forbidigo + sloglint); the only exclusion is the grpc-gateway `runtime` type in `internal/service`, which `server.GatewayRegistrar` takes. Telemetry is exported over OTLP only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, like the MCP template | own slog handler; OTel always on | One logger and one package per concern (owner, 2026-11-22) |

Defaults chosen without a popup: `ratelimit.DefaultLimits()` per instance
with the health service skipped; `IgnoreRemoteSampled` in telemetry (public
traffic cannot force sampling); `SHUTDOWN_DELAY=5s`, `SHUTDOWN_TIMEOUT=30s`;
`postgres:18-alpine` for `make db-up` and the integration test; the proto
package is the name with dashes as underscores, and the example service is
always `ItemService` so the name never doubles (`OrdersServiceService`).

Tests (`cmd/kitchen`):
- Golden trees `testdata/golden/service-postgres` and `service-nodb` (refresh with `-update`).
- `TestGeneratedServiceBuilds` writes both cases, replaces the kitchen modules
  with the local checkout (they are not tagged yet), runs the CLI's
  generation step, then `go build`, `go vet`, `go test -race` (the integration
  test starts Postgres with testcontainers), golangci-lint v2.15.0, `buf lint`
  and `sqlc diff` (the shipped sqlc output must match the queries). Skipped
  with `-short` or offline.

## 3b. Service features and the worker (owner, popups 2026-11-24)

```
kitchen new service ... [-with redis,lease,pubsub,outbox] [-pubsub gcp|kafka|sqs]
```

| # | Decision (owner) | Alternatives | Why |
|---|---|---|---|
| F1 | Optional features with `-with`; each one wires one canonical package (`redis` → `redisx` + `ratelimit/redis`, `lease` → `lease/redis`, `pubsub` → `pubsub` + adapter, `outbox` → `pubsub/outbox/pgx`). `lease` adds `redis`; `outbox` without `-db postgres` and `pubsub` is refused | fixed presets; generate everything and switch by env | Only what the service needs is generated and required in go.mod; matches D7 |
| F2 | One binary with commands `serve` (default), `worker`, `migrate`; the worker exists with `pubsub` or `lease` | one `cmd/*` per process; consumers inside the server process | One image and one wiring; the worker scales and deploys apart from the API |
| F3 | Periodic jobs run in the worker under `lease.Elector` (one replica per job), interval only | a new `cron` package; Kubernetes CronJob only | No new concern or dependency |
| F4 | Local dependencies in `compose.yaml` (`make up`/`down`, creates topics, subscriptions, queues) and every variable in `docs/ENV.md`; no Kubernetes or Helm files | kustomize; Helm | Manifests live with the platform; the env contract is what a deploy needs |
| F5 | Upgrades of generated projects: a manifest plus a drift check first, a three-way `kitchen upgrade` later | upgrade now; none | Cheap and safe while templates still change |
| F6 | Add-on generators planned: `kitchen add domain`, `add consumer`, `add job`; no `add rpc` or `add migration` (buf and goose do that) | – | – |
| F7 | Internal APIs: a per-method policy, deny by default | a second gRPC port | One mechanism, fail-closed |
| F8 | MySQL template later; MongoDB documented only | both; Postgres only | – |

Defaults chosen without a popup: broker default `gcp`; Redis and Kafka use
TLS unless `REDIS_TLS` / `PUBSUB_TLS` is `false` (local only); without
`outbox` an event is published after the item is stored and a failed
publish is logged (the README says to use the outbox for guaranteed
delivery); the broker client is created only in `internal/app/broker.go`,
the one file where the template's lint config allows the broker SDK;
Pub/Sub sources take the subscription's ordering and dead-letter settings
from configuration; Kafka sends poison messages to a dead-letter topic
after `PUBSUB_MAX_DELIVERY_ATTEMPTS`; the worker serves its probes on
`ADMIN_ADDR`. The integration test uses miniredis and the in-memory broker.
`cache` and `sse` features wait for the reviews of those packages.

## 4. Sequencing

1. **This PR:** mcpkit, jwtauth, mcpkittest, the CLI framework and `new mcp`
   (it needs mcpkit and kitchen core: log, server, errs, plus otelx/sdk).
2. **Done 2026-11-22:** `kitchen new service` (§3a), after transport,
   observability, errors-auth, data-utils and messaging merged.
3. **When go-sdk v1.9.0 reaches GA:** bump `goSDKVersion` and mcpkit together.
4. **After the first `mcpkit/v0.1.0` tag:** the generated `go.mod` resolves
   without `go.work`.

## 5. Decisions taken with the owner (popups, 2026-11-20)

1. mcpkit is a thin typed layer (vs using go-sdk directly / keeping the builder).
2. Auth uses a pluggable verifier with PassThrough + JWT (vs gateway-only / pass-through only).
3. Transports are stdio + Streamable HTTP; legacy SSE is dropped. 2025-11-25 clients are served through the SDK fallback.
4. `nest` is dropped, with a migration guide only (vs RoundTripper middlewares / fixing nest).
5. One CLI with `new mcp` + `new service`; `gen`/`add model` is dropped.
6. Generated service stack: gRPC + gateway, pgx + sqlc, optional Pub/Sub.
7. Templates are org-neutral, with a `-ci reusable` preset.
8. This PR ships mcpkit + CLI + the MCP template; the service template follows the other areas.
9. Only an `http.Handler` (no own `http.Server`).
10. SDK middleware + ctx timeout; resource cache and MCP logging dropped.
11. JWT lives in `mcpkit/jwtauth` on go-jose v4.
12. The stdio token comes from an environment variable; `_meta.token` is dropped.
13. go-sdk is pinned at v1.8.0.
14. Generated config is env + stdlib.
15. Docker: `golang:1.27` builder + distroless nonroot.
16. Golden-file tests + a build test through `go.work`.

## 6. Decisions taken by default (to review)

The owner asked to accept every recommended option and to decide new
questions by what is best for the system and for security. Each decision
below can be revisited.

| # | Decision | Alternatives | Pros | Cons |
|---|---|---|---|---|
| A1 | A missing or invalid bearer token always gets **401 + WWW-Authenticate** when `WithAuth` is set. Without `WithAuth`, the server is public. | (b) an `Optional` flag that lets requests through; (c) always let them through, as before | Spec-compliant; OAuth clients discover the IdP by themselves; no unauthenticated path by accident | A server cannot mix public and private tools behind one handler (mount two handlers) |
| A2 | **Stateless by default, `WithStateful(idle)` opt-in**, `SDK()` escape hatch | stateless only | Scales horizontally without sticky sessions (30K-connection target); stateful is still possible | One more configuration branch to test (covered by `TestHTTPStateful`) |
| A3 | **Functional options** for `mcpkit.New` | `Config` struct | Extensible without breaking callers; clear defaults | Harder to dump the effective config |
| A4 | **huh v2 prompt when TTY and flags are missing**; flags only with `-yes` or without a TTY | flags only | Friendly for people, still works in CI | Pulls bubbletea/lipgloss into `cmd/kitchen` (CLI-only module, not into libraries) |
| A5 | Templates live in **`cmd/kitchen/templates/`**, not root `templates/` (D6) | root `templates/` + a separate embed module | `go:embed` cannot reach outside the module directory | Differs from the D6 table; no other area is affected |
| A6 | ~~`jwtauth` on go-jose v4~~ → **kitchen `auth` via `mcpkit.Verifier`** (2026-11-21) | keep `jwtauth` | One token-verification stack (jwx) for gRPC, HTTP and MCP; introspection for opaque tokens comes for free | mcpkit requires the `auth` submodule; no JWKS discovery |
| A7 | `PassThrough` needs an explicit `AllowMissingExpiration: true`; JWTs must carry `exp` | PassThrough fakes an expiry | Never weakens expiry checks silently | One more field to set (shown in the examples and the template) |
| A8 | A tool panic returns a generic "internal error"; details go only to the log | return the panic text | No information leak to the model or client | Callers must read logs to debug |
| A9 | **No default tool timeout** in the library; the generated server sets `MCP_TOOL_TIMEOUT=2m` | a library default | The library has no hidden policy; the app decides | A server built without the template has no deadline unless it sets one |
| A10 | Timeout errors become a tool error naming the tool and the duration | keep the raw `context deadline exceeded` | The model can understand and retry | – |
| A11 | CORS uses an exact-origin allow-list (rs/cors, the same library as `server`) combined with stdlib `CrossOriginProtection`; a denied preflight gets no CORS headers; no wildcard | reflect any origin (old behaviour) | Closes the old "empty list reflects every origin" hole; defends against cross-site writes | Browser clients must be listed explicitly |
| A12 | `PropagateRequestCancellation: true` | SDK default false | A tool stops when the client goes away (2026-07-28 clients); no wasted work under load | A tool that must finish after a disconnect has to detach its context itself |
| A13 | `mcpkit.Verifier` requires `aud` to contain the resource URL; algorithm allow-list, `exp`, JWKS refresh and outage handling come from kitchen `auth`; key-fetch errors are 503 (errs UNAVAILABLE), not 401 | trust the kitchen verifier's optional Audience | Defends against token replay to other resources even when the verifier was configured loosely | – |
| A18 | Tool errors: only client-fault `*errs.Error` messages reach the model; others are logged and become "internal error" | send `err.Error()` (SDK default) | No SQL, hosts or internals leak to the model or client | Errors the model should read must be `errs` errors |
| A14 | The `-ci reusable` workflow must be pinned to a 40-character commit SHA | allow tags/branches | Supply-chain safety (the old template used `@master`) | Updating means changing the SHA (use Renovate) |
| A15 | Dockerfile: private modules via `GOPRIVATE` build-arg + a BuildKit `netrc` secret; no `GOSUMDB=off`; `CGO_ENABLED=0`, `-trimpath`, nonroot | git token in a layer (old) | No credential in any layer; checksum DB stays on | The build needs BuildKit (default in Docker 23+) |
| A16 | The generated config requires an `https` `MCP_RESOURCE_URL` (http only for localhost) plus at least one issuer when auth is enabled | accept any URL | Prevents leaking tokens over plain HTTP | Local non-localhost HTTP testing needs auth off |
| A17 | The generated stdio mode uses `UPSTREAM_TOKEN` when set; otherwise it runs without a token | require the token | Tools without upstream calls work out of the box | A missing token surfaces in the tool, not at startup |
| A18 | The CLI does not run `go mod tidy`; it prints next steps | run tidy automatically | Works offline and before mcpkit is tagged; no network side effects | One manual step |
| A21 | The generated `main` calls `log.Setup` first with `Output` defaulting to stderr (`LOG_OUTPUT` overrides), and the template's golangci config forbids `log/slog` and `slog.Default/New*` like kitchen's own (owner, 2026-11-22: one logger); since D7 it also denies, by depguard and forbidigo, the libraries and constructors behind every canonical kitchen package (raw Redis/SQL/Mongo clients, `http.Client`, `http.Server`, `status.New`, OTel SDK setup, ...) | stdout (Setup's default) | stdout carries the protocol in stdio mode | Logs written before `main` (package init) still go to stdout unless `LOG_OUTPUT=stderr` is set in the launch config |
| A20 | The generated MCP server runs on kitchen's `server` (`srv.Service` + `WithGRPCAddr("")`), logs with kitchen `log` (`LOG_*`) and sets up `otelx/sdk` when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (owner, 2026-11-21) | hand-written `http.Server` + `/healthz` + `slog.NewJSONHandler`, no telemetry | One standard for probes, drain, access logs, log schema and telemetry; mcpkit's spans and metrics are exported | The generated `go.mod` also requires core and `otelx/sdk` (`-kitchen-version`) |
| A19 | ~~`kitchen new service` returns "not available yet"~~ → **shipped 2026-11-22** (§3a) | hide the subcommand | – | – |
| A20 | Telemetry uses semconv **v1.41.0** `mcpconv` (the newest with MCP metrics in OTel Go v1.47) | v1.43 attribute keys with hand-written metric | Generated instrument definitions | Bump when a newer semconv ships `mcpconv` |
| A21 | `mcpkittest.Connect(t, srv, token)` is part of the public API | tests build in-memory transports by hand | Generated projects and users test tools in three lines | One more public package to keep stable |
| A23 | Auth failures never echo the cause; `auth.ErrAuthUnavailable` is rewritten to the errs 503 response around the SDK's `RequireBearerToken` | keep the SDK's 500 with `err.Error()` body; reimplement bearer middleware | 503 matches `server`, no internal URLs or library errors leak; the SDK's `TokenInfo` context key (used to bind sessions to users) keeps working | Depends on the SDK deciding before it writes; drop the wrapper if the SDK gains a status hook |

## 7. Not carried over (and why)

- Builder, `Config`/`HTTPTransportConfig`, registries and `deprecated.go`: the
  SDK and functional options cover them.
- Own `http.Server`, TLS, gzip and the 60s `WriteTimeout`: these belong to
  the app/server; gzip was dead code for SSE.
- Resource TTL cache: keyed by URI only, so it leaked one user's data to
  another. Cache per principal inside the handler if needed.
- MCP logging capability: deprecated in 2026-07-28; use OTel and kitchen `log`.
- Legacy SSE transport: deprecated.
- `nest`: data race, broken retry, no tests; the stdlib + `x/oauth2` cover the
  real use (Google OAuth token exchange).
- `gen`: in 65 consumer services no file it generates exists; AST-based
  `add model` is not worth maintaining.
