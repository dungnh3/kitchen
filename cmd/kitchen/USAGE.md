# cmd/kitchen

`kitchen` is the code generator for projects built on kitchen. It has two
templates, and a `check` command that keeps generated services in line
with their template:

- `kitchen new service`: a gRPC + REST (grpc-gateway) service, with
  PostgreSQL or without a database, and optional Redis, events (Pub/Sub,
  Kafka or SQS), a transactional outbox and leader-elected jobs in a
  worker.
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
make up           # local Postgres in Docker (compose.yaml)
make run          # gRPC on :10000, REST on :8080, auth disabled
make test
curl -s localhost:8080/v1/items
```

A service with events, a transactional outbox and periodic jobs:

```bash
kitchen new service -module github.com/acme/billing -with pubsub,outbox,lease -pubsub gcp
cd billing
make up           # Postgres, Redis and the Pub/Sub emulator, with the topic and subscription
make run          # the API; creating an item writes an ItemCreated event to the outbox
make run-worker   # the worker: relays the outbox, consumes the events, runs the jobs
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
values with a form: module path, name, description, database, optional
features and message broker (`service` only), and CI workflow. It asks for
the broker only when you pick `pubsub`, and for the reusable workflow only
when you pick "Reusable workflow pinned to a commit". The form refuses
`outbox` without PostgreSQL and `pubsub`. Run `kitchen new service` or
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
| `-with` | | Comma-separated optional features, see below |
| `-dry-run` | `false` | List the files that would be generated, marking the managed ones; write nothing |
| `-pubsub` | `gcp` | Message broker with `-with pubsub`: `gcp` (Google Cloud Pub/Sub), `kafka` or `sqs`. Refused without `-with pubsub` |

Each `-with` feature wires one kitchen package, with its configuration,
readiness check, local container in `compose.yaml` and tests:

| Feature | Package | What it adds |
|---|---|---|
| `redis` | `redisx`, `ratelimit/redis` | A Redis client (TLS by default) with a readiness check; rate limits shared by every instance |
| `lease` | `lease`, `lease/redis` | Periodic jobs in the worker (`internal/job`), each on one replica at a time. Adds `redis` |
| `pubsub` | `pubsub` and the broker adapter | An `ItemCreated` event per new item (`internal/events`), a consumer in the worker (`internal/consumer`) |
| `outbox` | `pubsub/outbox/pgx` | The event is written in the item's transaction and the worker relays it. Needs `-db postgres` and `pubsub` |

With `pubsub` or `lease` the binary gets a `worker` command (`server
worker`): deploy it from the same image as a second Deployment.

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

# Kafka events without a database, plus periodic jobs.
kitchen new service -module example.com/team/notify -db none -with pubsub,lease -pubsub kafka -yes

# Reusable CI workflow pinned to a commit.
kitchen new mcp -module example.com/team/mcp-tickets -name tickets \
  -ci reusable -ci-workflow acme/devops-workflows/.github/workflows/go.yml@0123456789abcdef0123456789abcdef01234567 -yes
```

## What you get

### `kitchen new service`

The exact trees are in [testdata/golden](testdata/golden):
`service-postgres` and `service-nodb` (no features), `service-events`
(Postgres, `lease,pubsub,outbox`, Pub/Sub), `service-kafka` (no database,
`pubsub`, Kafka) and `service-sqs` (Postgres, `redis,pubsub`, SQS).

| Path | What |
|---|---|
| `proto/<name>/v1/item_service.proto` | The API: an AIP CRUD example (Get, List, Create, Delete), protovalidate rules, REST mapping |
| `gen/` | Generated by `make generate` (pb, grpc, gateway, OpenAPI in `gen/openapi/`); committed |
| `cmd/server/main.go` | `log.Setup`, config, then `serve`, `worker` (with `pubsub` or `lease`) or `migrate up\|down\|status` |
| `internal/config/` | Environment config (caarlos0/env); `ValidateServer` enforces auth and the settings of each dependency |
| `internal/app/deps.go` | Opens and closes what the server and the worker share: telemetry, database, Redis, broker; readiness checks |
| `internal/app/app.go` | Wiring of `serve`: i18n + errs, rate limits, auth, admin port, server |
| `internal/app/worker.go` | Wiring of `worker`: consumers, outbox relay, jobs, admin port (with `pubsub` or `lease`) |
| `internal/app/redis.go`, `broker.go` | `redisx.Open`; the broker client handed to the kitchen adapter (with `redis`, `pubsub`) |
| `internal/events/`, `internal/consumer/`, `internal/job/` | Events and their handlers (`pubsub`), periodic jobs (`lease`) |
| `internal/app/migrate.go` | goose migrations through kitchen `migrate`, with an advisory lock |
| `internal/app/app_test.go` | Integration test of the whole service on random ports (Postgres via testcontainers) |
| `internal/service/` | `ItemService` over `store.Store`, with opaque page tokens; unit tests |
| `internal/store/` | `Store` interface, `memstore`, and `pgstore` with `queries.sql` and sqlc output (`sqlcdb`, committed) |
| `migrations/` | goose SQL, embedded in the binary; `00002_create_outbox.sql` with `outbox` |
| `compose.yaml` | Local Postgres, Redis and broker (emulator) for `make up` |
| `.kitchen.yaml` | How the project was generated and the hash of each managed file; read by `kitchen check` |
| `docs/ENV.md` | Every environment variable and what production needs |
| `buf.yaml`, `buf.lock`, `buf.gen.yaml`, `sqlc.yaml` | Code generation; protoc plugins are `tool` deps in `go.mod` |
| `Dockerfile`, `Makefile`, `.golangci.yml`, `.github/` | Build, tasks, lint rules, CI and a PR template |

The generated `docs/ENV.md` lists every environment variable with its
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

## Managed files and `kitchen check`

kitchen owns a few files of a generated service: `.golangci.yml` (the D5/D7
rules), `Dockerfile`, `.dockerignore`, `.github/workflows/ci.yml` (SHA pins)
and `buf.gen.yaml`. They start with a "Managed by kitchen" header, and
`.kitchen.yaml` records the answers used to generate the project and the
sha256 of each managed file. Every other file belongs to the project.

```bash
kitchen check            # in the project; or: make check
kitchen check -write     # update the unedited managed files to the current template
```

| Finding | Result |
|---|---|
| A managed file was edited or deleted | Error, exit 1. Restore it, or remove it from `managed` in `.kitchen.yaml` to own it |
| The current template writes a managed file differently | Warning; `-write` updates it and the hash |
| A managed file is new in the template and absent from the project | Warning; `-write` creates it |
| A newer kitchen is released | Warning with the command to run (skipped with `-offline` or when the module proxy is unreachable) |

`-write` never touches an edited file. The standalone CI workflow and
`make check` run `kitchen check` at the version that generated the project,
so CI fails on hand edits but not on a new kitchen release. With
`-ci=reusable`, add the step to your shared workflow.

| Flag | Default | When to change it |
|---|---|---|
| `-dir` | `.` | To check another directory |
| `-write` | `false` | To update the managed files to the current template |
| `-offline` | `false` | To skip the lookup of the latest kitchen version |

Seeded files (code, README, Makefile, `compose.yaml`, `docs/ENV.md`) are
not compared; there is no upgrade of seeded files yet.

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
- **Redis.** `redisx.Open` with TLS unless `REDIS_TLS=false`; the client
  backs the shared rate limits (`ratelimit/redis`) and the job leases
  (`lease/redis`). See [redisx](../../redisx) and [lease](../../lease).
- **Events.** The service publishes through a `pubsub.Publisher` from the
  broker adapter (`pubsub/gcp`, `pubsub/kafka`, `pubsub/sqs`). Without
  `outbox` it publishes after the item is stored and logs a failed publish;
  with `outbox` it writes the event in the item's transaction
  (`outboxpgx.Store.Enqueue`) and the worker relays it. The worker consumes
  with `pubsub.NewSubscriber` and a `pubsub.JSON` handler; on Kafka a poison
  message goes to a dead-letter topic after `PUBSUB_MAX_DELIVERY_ATTEMPTS`.
  See [pubsub/USAGE.md](../../pubsub/USAGE.md).
- **Jobs.** Each job runs under its own `lease.Elector`, so one replica
  runs it at a time; a run is cancelled when the lease is lost.
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

### Add a consumer or a job

A consumer is a `pubsub.Handler`, usually `pubsub.JSON` over an event type
from `internal/events`. Subscribe it in `RunWorker` like the `ItemCreated`
one, with its own topic and subscription settings in `config.PubSub`.
Handlers must be idempotent: return `nil` to ack, an error to retry and
`pubsub.Permanent(err)` to dead-letter.

A job is a `job.Job{Name, Every, Run}` in `jobs` in
`internal/app/worker.go`. Keep `Name` stable (it names the lease) and stop
the run when its context is cancelled.

### Run the dependencies locally

`make up` starts `compose.yaml` and waits for it: Postgres, Redis and the
broker of `-pubsub` (Pub/Sub emulator, Kafka, or an SQS-compatible queue
server). It also creates the topic and subscription (Pub/Sub), the topics
(Kafka) or the queue (SQS). `make run` and `make run-worker` point the
service at them; `make down` stops them. Production creates topics,
subscriptions and queues with its infrastructure tool, never the service.

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
and run `server migrate up` as a separate step. The worker never migrates:
start it after the migrations. Add a migration with
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
  With a worker it also runs the worker; Redis is miniredis and the broker
  is the in-memory `pubsub/memory`, so it checks that creating an item
  publishes its event (through the outbox with `outbox`).
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

The check tests generate a service and edit, delete, own and age its
managed files; the build tests also run `kitchen check` on every generated
service after code generation.

The build tests generate every golden variant, build, vet, test and lint
them. They need the network and, for the service, Docker. A matrix test
renders every valid combination of `-db`, `-with` and `-pubsub` and checks
the file set. Review a golden diff the way you would review real code.

## Production checklist

- Set `APP_ENV=production` so unsafe settings such as `AUTH_DISABLED` are
  refused.
- Set `AUTH_ISSUER`, `AUTH_JWKS_URL` and `AUTH_AUDIENCE`; never
  `AUTH_DISABLED`.
- Use a TLS `DB_URL`; `sslmode=disable` is for the local database only.
- Run `server migrate up` as a deploy step; keep `DB_MIGRATE_ON_START=false`.
- Set `TRUSTED_PROXIES` to the CIDRs of your load balancer, so rate limits
  see the real client IP. For several instances, generate with
  `-with redis` (or pass a `ratelimit/redis` store as
  `ratelimit.Config.Store`).
- Deploy `server worker` as its own Deployment with `ADMIN_ADDR` set, and
  point its probes at the admin port.
- Create topics, subscriptions and queues (with their dead-letter
  policies) with your infrastructure tool; follow the Production section of
  the generated `docs/ENV.md`.
- Keep `ADMIN_ADDR` private, and point the probes at `/livez` and `/readyz`.
- For an MCP server over HTTP, set `MCP_RESOURCE_URL` and
  `MCP_AUTH_SERVERS`, and list browser origins in `MCP_ALLOWED_ORIGINS`.
- Keep `.golangci.yml` and the SHA pins in CI; `kitchen check` fails when
  a managed file is edited.
- `make docker` builds a distroless nonroot image. For private modules pass
  `--build-arg GOPRIVATE=...` and `--secret id=netrc,src=$HOME/.netrc`.

## Migrating from the old kitchen

The old generators (the interactive MCP scaffolder, the service scaffolder
and `gen`) are replaced by `kitchen new mcp` and `kitchen new service`;
`gen` has no replacement. See
[docs/migration/codegen-mcp.md](../../docs/migration/codegen-mcp.md).
