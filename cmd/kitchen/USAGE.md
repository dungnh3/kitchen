# cmd/kitchen

`kitchen` is the code generator for projects built on kitchen. It has two
templates:

- `kitchen new service`: a gRPC + REST (grpc-gateway) service, with
  PostgreSQL or without a database.
- `kitchen new mcp`: an MCP server that speaks Streamable HTTP and stdio.

The output already follows the kitchen rules: `log.Setup` in `main`, the
canonical packages from D7 ([docs/decisions.md](../../docs/decisions.md)),
fail-closed auth, and a `.golangci.yml` that keeps it that way. Use it to
start a new project instead of copying an old one.

## Install

```bash
go install github.com/dungnh3/kitchen/cmd/kitchen@latest
kitchen version
```

`cmd/kitchen` is its own module (`github.com/dungnh3/kitchen/cmd/kitchen`),
so installing it does not pull the kitchen libraries. The generated
project requires them in its own `go.mod`.

## Quick start

A service with PostgreSQL (the default):

```bash
kitchen new service -module github.com/acme/orders
cd orders
make db-up        # local Postgres in Docker
make run          # gRPC on :10000, REST on :8080, auth disabled
make test
curl -s localhost:8080/v1/items
```

An MCP server:

```bash
kitchen new mcp -module github.com/acme/mcp-notes
cd mcp-notes
go mod tidy
make test
make run          # HTTP on :8080, endpoint /mcp
make run-stdio    # stdio, for desktop clients and the MCP Inspector
```

What the defaults give you:

- The directory and the name are the last element of `-module`.
- `kitchen new service` runs code generation for you: `go mod tidy -e`,
  `buf generate`, `go mod tidy`. The first run downloads modules and buf.
- The CLI never overwrites anything. It refuses a non-empty directory and
  writes only inside it.
- `.go` output is gofmt'd. A template error fails generation, not your build.
- CI is a self-contained GitHub Actions workflow with every action pinned
  to a commit SHA.
- `make run` sets `AUTH_DISABLED=true` for local runs only. Without it the
  service refuses to start until `AUTH_*` is set.

### Interactive prompts

With a terminal on stdin, no `-module` and no `-yes`, the CLI asks for the
values with a form: module path, name, description, database (`service`
only) and CI workflow. It asks for the reusable workflow only when you pick
"Reusable workflow pinned to a commit". Run `kitchen new service` or
`kitchen new mcp` with no flags to get the form.

Without a terminal, or with `-yes`, it uses flags only and fails on
missing or invalid values. Use `-yes` in scripts and CI.

## Configuration

Run `kitchen new service -h` or `kitchen new mcp -h` for the same list.

### Flags of both templates

| Flag | Default | When to change it |
|---|---|---|
| `-module` | (required) | Always. The Go module path, checked with `module.CheckPath` |
| `-dir` | last element of `-module` | To write somewhere else. It must not exist or must be empty |
| `-name` | last element of `-module` | When the module path ends in something else, for example `mcp-tickets` with name `tickets`. Lower-case letters, digits and dashes |
| `-description` | `A gRPC and REST service.` / `An MCP server.` | One line for the README |
| `-ci` | `standalone` | `reusable` to call a shared workflow instead of the generated jobs |
| `-ci-workflow` | | Required with `-ci=reusable`: `owner/repo/.github/workflows/file.yml@<40-char commit SHA>`. Tags and branches are refused because they are mutable |
| `-kitchen-version` | `v0.1.0` | To require another version of the kitchen modules in `go.mod`. Must be a semantic version |
| `-yes` | `false` | Never prompt; use flags only |

### `kitchen new service` only

| Flag | Default | When to change it |
|---|---|---|
| `-db` | `postgres` | `none` for a service without a database. It uses an in-memory store and drops `migrations/`, `sqlc.yaml`, `internal/app/migrate.go` and `internal/store/pgstore/` |
| `-no-generate` | `false` | Skip `go mod tidy` and `buf generate`, for example offline or in a sandbox. The CLI prints the steps to run by hand |

What `-name` controls in a service: the proto package (dashes become
underscores, so `edge-gw` gives `proto/edge_gw/v1/`), the telemetry service
name and the default `ERROR_DOMAIN`. The example service is always
`ItemService`.

### `kitchen new mcp` only

| Flag | Default | When to change it |
|---|---|---|
| `-mcpkit-version` | `v0.1.0` | To require another `github.com/dungnh3/kitchen/mcpkit` version |

In an MCP server, `-name` is the server name reported to clients and the
telemetry service name.

### Examples

```bash
# No database, custom name and directory.
kitchen new service -module example.com/team/edge-gateway -name edge-gw -dir ./edge -db none -yes

# Reusable CI workflow pinned to a commit.
kitchen new mcp -module example.com/team/mcp-tickets -name tickets \
  -ci reusable -ci-workflow acme/devops-workflows/.github/workflows/go.yml@0123456789abcdef0123456789abcdef01234567 -yes
```

## What you get

### `kitchen new service`

The exact tree is in
[testdata/golden/service-postgres](testdata/golden/service-postgres) and
[testdata/golden/service-nodb](testdata/golden/service-nodb).

| Path | What |
|---|---|
| `proto/<name>/v1/item_service.proto` | The API: an AIP CRUD example (Get, List, Create, Delete), protovalidate rules, REST mapping |
| `gen/` | Generated by `make generate` (pb, grpc, gateway, OpenAPI in `gen/openapi/`); committed |
| `cmd/server/main.go` | `log.Setup`, config, `serve` or `migrate up\|down\|status` |
| `internal/config/` | Environment config (caarlos0/env); `ValidateServer` enforces auth |
| `internal/app/app.go` | Wiring: telemetry, database, i18n + errs, rate limits, auth, admin port, server |
| `internal/app/migrate.go` | goose migrations through kitchen `migrate`, with an advisory lock |
| `internal/app/app_test.go` | Integration test of the whole service on random ports (Postgres via testcontainers) |
| `internal/service/` | `ItemService` over `store.Store`, with opaque page tokens; unit tests |
| `internal/store/` | `Store` interface, `memstore`, and `pgstore` with `queries.sql` and sqlc output (`sqlcdb`, committed) |
| `migrations/` | goose SQL, embedded in the binary |
| `buf.yaml`, `buf.lock`, `buf.gen.yaml`, `sqlc.yaml` | Code generation; protoc plugins are `tool` deps in `go.mod` |
| `Dockerfile`, `Makefile`, `.golangci.yml`, `.github/` | Build, tasks, lint rules, CI and a PR template |

The generated `README.md` lists every environment variable with its
default. Read it first; this file does not repeat it.

### `kitchen new mcp`

The exact tree is in [testdata/golden/standalone](testdata/golden/standalone)
and [testdata/golden/reusable](testdata/golden/reusable).

| Path | What |
|---|---|
| `cmd/server/main.go` | `log.Setup` (stderr), config, `otelx/sdk`, mcpkit, then HTTP on kitchen `server` or stdio |
| `internal/config/` | Environment config (stdlib), validated: https resource URL, issuers required with auth |
| `internal/tools/` | A typed example tool (`greet`) and its test with the `mcpkittest` in-memory client |
| `Dockerfile`, `Makefile`, `.golangci.yml`, `.github/workflows/ci.yml` | Build, tasks, lint rules, CI |

## Integration with kitchen

Both templates wire the same packages the same way. Read the generated
files rather than copies here:

- **Logging.** `main` calls `log.OptionsFromEnv` and `log.Setup` before
  anything else, then passes the `*log.Logger` down. The service logs to
  stdout. The MCP server logs to stderr, because stdout carries the
  protocol in stdio mode. `LOG_LEVEL`, `LOG_FORMAT`, `LOG_SCHEMA` and
  `LOG_OUTPUT` configure it.
- **Server.** The service builds `server.New` with `WithServices`,
  `WithErrors`, `WithRateLimit`, `WithAuth`, readiness checks and an
  optional admin port. The MCP server mounts `srv.Service("/mcp")` on an
  HTTP-only `server` (`server.WithGRPCAddr("")`). See
  [server/USAGE.md](../../server/USAGE.md) and
  [mcpkit/USAGE.md](../../mcpkit/USAGE.md).
- **Telemetry.** `otelx/sdk.Setup` exports over OTLP only when
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set. The service also serves Prometheus
  `/metrics` on `ADMIN_ADDR` through `otelx/prometheus`, and sets
  `IgnoreRemoteSampled` so callers cannot force sampling.
- **Errors.** Return `errs` errors from methods and tools. The service maps
  them to the same `google.rpc.Status` on gRPC and the AIP-193 JSON error on
  REST, localized by `i18n`. In a tool, only the message of an `errs` error
  reaches the model; any other error is logged and becomes "internal
  error".
- **Auth.** The service verifies JWTs with `auth.NewJWKS` and is
  fail-closed. The MCP server enables auth when `MCP_RESOURCE_URL` is set.
- **Database.** The service opens Postgres with `pg.Open` (TLS required
  unless the URL sets `sslmode`, UTC session time zone) and maps errors
  with `pg.MapError`.
- **Lint.** `.golangci.yml` mirrors the repo's D5/D7 rules with depguard,
  forbidigo and sloglint. It denies `log/slog`, raw Redis, SQL and HTTP
  clients, `http.Server`, `status.New`, OTel SDK setup and so on. Tests may
  use the libraries directly. Keep the file when you change the project.

## Advanced

### Add an RPC to a service

1. Edit `proto/<name>/v1/*.proto`. Validation rules go in the proto file;
   `server` enforces them before your method runs.
2. With Postgres, add a migration in `migrations/` and queries in
   `internal/store/pgstore/queries.sql`.
3. Run `make generate`. It runs `buf generate` and `sqlc generate`.
4. Implement the method in `internal/service`.
5. Run `make lint` (golangci-lint, `buf lint`, `sqlc vet`) and
   `make breaking` to compare the API with `main`.

`gen/` and `sqlcdb/` are committed. CI runs `make generate` and fails if
they are stale. A new gRPC service type is registered in
`internal/app/app.go` next to the existing one:

```go
server.WithServices(service.New(st)),
```

### Add a tool to an MCP server

Add a typed input struct, an output struct and a handler in
`internal/tools`, and register it in `Register`. The JSON schema comes from
the `json` and `jsonschema` tags, and input is validated before the
handler runs. From the generated `tools.go`:

```go
mcpkit.AddTool(srv, &mcp.Tool{
	Name:        "greet",
	Description: "Return a greeting for the given name.",
	Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
}, greet)
```

Test it the way `tools_test.go` does: `mcpkittest.Connect(t, srv, "")`
gives an in-memory client.

### Migrations

```bash
make migrate-up         # or: server migrate up, as a deploy step or init container
make migrate-down       # roll back the last one
make migrate-status
```

`make run` sets `DB_MIGRATE_ON_START=true`. In production keep it `false`
and run `server migrate up` as a separate step. Add a migration with
`goose create <name> sql` or by hand as `NNNNN_name.sql` in `migrations/`.

### Add a database to a `-db none` service

The simplest way is to generate a second project with `-db postgres` and
copy `internal/app/migrate.go`, `internal/store/pgstore/`, `migrations/`,
`sqlc.yaml`, and the `Database` config and `openDB` wiring from
[service-postgres](testdata/golden/service-postgres).

For MySQL or MongoDB there is no template. Write a `store.Store`
implementation and open the client in `internal/app` with `mysqlx.Open` or
`mongox.Open`; never call the drivers directly (the lint config denies
them). For MySQL migrations, use `migrate.NewMySQL` in place of
`migrate.NewPostgres`. See [pg](../../pg), [mysqlx](../../mysqlx),
[mongox](../../mongox) and [migrate](../../migrate).

### Auth

A service needs `AUTH_ISSUER`, `AUTH_JWKS_URL` (https, http only for
localhost) and `AUTH_AUDIENCE`. An unreachable JWKS stops startup. Only
`/grpc.health.v1.Health/` is public. To require scopes per method, set
`Authorizer: auth.RequireScopes(...)` in the `auth.Config` built by
`newAuthenticator` in `internal/app/app.go`. For opaque tokens, add an
`auth.NewIntrospection` verifier next to the JWKS one.

> `AUTH_DISABLED=true` makes every caller anonymous. It exists for local
> runs and tests only, and the service refuses it when `APP_ENV=production`.
> Do not set it in a shared environment.

An MCP server turns auth on with `MCP_RESOURCE_URL` (the public https URL
of `/mcp`) and `MCP_AUTH_SERVERS`. The template uses `mcpkit.PassThrough()`:
the bearer token is the upstream API's OAuth token, and tools read it with
`mcpkit.TokenFrom(ctx)`. To verify tokens from your own identity provider,
build a verifier with kitchen `auth` and pass it through `mcpkit.Verifier`.
In stdio mode the upstream token comes from `UPSTREAM_TOKEN`.

### Telemetry

| Variable | Effect |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Turns on OTLP export of traces (and metrics, logs) |
| other `OTEL_*` | Standard OpenTelemetry settings, read by `otelx/sdk` |
| `ADMIN_ADDR` (service) | Private admin port: `/readyz` with details, `/version`, Prometheus `/metrics`, `/debug/pprof/` |

Keep `ADMIN_ADDR` off the public network.

### Unreleased kitchen versions

`-kitchen-version` and `-mcpkit-version` only write versions into
`go.mod`. To build against a local kitchen checkout, add a `replace` for
each kitchen module the project requires, then tidy:

```bash
go mod edit -replace github.com/dungnh3/kitchen=../kitchen
go mod tidy
```

### Reusable CI workflow

With `-ci=reusable`, `.github/workflows/ci.yml` only calls your workflow,
pinned to the SHA you passed, with `secrets: inherit`. The standalone
service workflow also checks generated code, runs `buf lint`, and runs
`buf breaking` on pull requests; your shared workflow must do that itself.

## Testing

The generated projects:

- `make test` runs `go test -race -count=1 ./...`.
- The service's integration test starts the whole service on random ports
  and, with Postgres, a database through testcontainers. It needs Docker.
- `memstore` is the fake for `store.Store`; there are no mocks to generate.
- Config tests cover the environment parsing and validation.
- Lint: `make lint` runs golangci-lint v2.15.0 with `go run` in both
  generated projects, so nothing has to be installed.

The generator itself, from `cmd/kitchen`:

```bash
go test ./...            # golden files, validation, exit codes, generated builds
go test -short ./...     # skip the builds of generated projects (no network)
go test ./... -update    # rewrite testdata/golden after a template change
```

The build tests generate every variant, build, vet, test and lint them.
They need the network and, for the service, Docker. Review a golden diff
the way you would review real code.

## Production checklist

- Set `APP_ENV=production` so unsafe settings such as `AUTH_DISABLED` are
  refused.
- Set `AUTH_ISSUER`, `AUTH_JWKS_URL` and `AUTH_AUDIENCE`; never
  `AUTH_DISABLED`.
- Use a TLS `DB_URL`; `sslmode=disable` is for the local database only.
- Run `server migrate up` as a deploy step; keep `DB_MIGRATE_ON_START=false`.
- Set `TRUSTED_PROXIES` to the CIDRs of your load balancer, so rate limits
  see the real client IP. For several instances, pass a Redis store
  (`ratelimit/redis`) as `ratelimit.Config.Store`.
- Keep `ADMIN_ADDR` private, and point the probes at `/livez` and `/readyz`.
- For an MCP server over HTTP, set `MCP_RESOURCE_URL` and
  `MCP_AUTH_SERVERS`, and list browser origins in `MCP_ALLOWED_ORIGINS`.
- Keep `.golangci.yml` and the SHA pins in CI.
- `make docker` builds a distroless nonroot image. For private modules pass
  `--build-arg GOPRIVATE=...` and `--secret id=netrc,src=$HOME/.netrc`.

## Migrating from the old kitchen

The old generators (the interactive MCP scaffolder, the service scaffolder
and `gen`) are replaced by `kitchen new mcp` and `kitchen new service`;
`gen` has no replacement. See
[docs/migration/codegen-mcp.md](../../docs/migration/codegen-mcp.md).
