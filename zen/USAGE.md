# zen

`zen` is the code generator for projects built on kitchen. It has two
templates, an `add` command that grows a generated service, and a `check`
command that keeps it in line with its template:

- `zen new service`: a gRPC + REST (grpc-gateway) service, with
  PostgreSQL or without a database, and optional Redis, events (Pub/Sub,
  Kafka or SQS), a transactional outbox and leader-elected jobs in a
  worker.
- `zen new mcp`: an MCP server that speaks Streamable HTTP and stdio.

The output already follows the kitchen rules: `log.Setup` in `main`, the
canonical packages from D7 ([docs/decisions.md](../docs/decisions.md)),
fail-closed auth, and a `.golangci.yml` that keeps it that way. Use it to
start a new project instead of copying an old one.

## Install

```bash
go install github.com/dungnh3/kitchen/zen@latest
zen version
```

`go install` puts the binary `zen` in `$(go env GOBIN)`, or in
`$(go env GOPATH)/bin` (usually `~/go/bin`) when GOBIN is not set. Add that
directory to `PATH` once, then open a new terminal and run `zen`. From a
checkout of this repository, for example before the first tag exists, use
the Makefile in `zen/`:

```bash
cd zen
make build      # bin/zen, without installing it
make install    # zen in $(go env GOPATH)/bin
```

`make test` runs the tests and `make clean` removes `bin/zen`.

`zen` is its own module (`github.com/dungnh3/kitchen/zen`),
so installing it does not pull the kitchen libraries. The generated
project requires them in its own `go.mod`.

## Quick start

A service with PostgreSQL (the default):

```bash
zen new service -module github.com/acme/orders
cd orders
make up           # local Postgres in Docker (compose.yaml)
make run          # gRPC on :10000, REST on :8080, auth disabled
make test
curl -s localhost:8080/v1/items
```

A service with events, a transactional outbox and periodic jobs:

```bash
zen new service -module github.com/acme/billing -with pubsub,outbox,lease -pubsub gcp
cd billing
make up           # Postgres, Redis and the Pub/Sub emulator, with the topic and subscription
make run          # the API; creating an item writes an ItemCreated event to the outbox
make run-worker   # the worker: relays the outbox, consumes the events, runs the jobs
```

An MCP server:

```bash
zen new mcp -module github.com/acme/mcp-notes
cd mcp-notes
go mod tidy
make test
make run          # HTTP on :8080, endpoint /mcp
make run-stdio    # stdio, for desktop clients and the MCP Inspector
```

What the defaults give you:

- The directory and the name are the last element of `-module`.
- `zen new service` runs code generation for you: `go mod tidy -e`,
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
`outbox` without PostgreSQL and `pubsub`. Run `zen new service` or
`zen new mcp` with no flags to get the form.

Without a terminal, or with `-yes`, it uses flags only and fails on
missing or invalid values. Use `-yes` in scripts and CI.

## Configuration

Run `zen new service -h` or `zen new mcp -h` for the same list.

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

### `zen new service` only

| Flag | Default | When to change it |
|---|---|---|
| `-db` | `postgres` | `mysql` for MySQL (`mysqlx`, database/sql, sqlc, goose; `internal/store/mysqlstore/`). `none` for a service without a database: an in-memory store, no `migrations/`, `sqlc.yaml` or `internal/app/migrate.go` |
| `-auth` | `jwks` | `introspection` for opaque tokens checked at an RFC 7662 endpoint (`auth.NewIntrospection`, cached; shared through Redis with `-with redis`) |
| `-no-generate` | `false` | Skip `go mod tidy` and `buf generate`, for example offline or in a sandbox. The CLI prints the steps to run by hand |
| `-with` | | Comma-separated optional features, see below |
| `-dry-run` | `false` | List the files that would be generated, marking the managed ones; write nothing |
| `-pubsub` | `gcp` | Message broker with `-with pubsub`: `gcp` (Google Cloud Pub/Sub), `kafka` or `sqs`. Refused without `-with pubsub` |

Each `-with` feature wires one kitchen package, with its configuration,
readiness check, local container in `compose.yaml` and tests:

| Feature | Package | What it adds |
|---|---|---|
| `redis` | `redisx`, `ratelimit/redis` | A Redis client (TLS by default) with a readiness check; rate limits shared by every instance |
| `cache` | `cache` | A read-through Redis cache in front of the item store (`internal/store/cachestore`, `CACHE_TTL`). Adds `redis` |
| `lease` | `lease`, `lease/redis` | Periodic jobs in the worker (`internal/job`), each on one replica at a time. Adds `redis` |
| `pubsub` | `pubsub` and the broker adapter | An `ItemCreated` event per new item (`internal/events`), a consumer in the worker (`internal/consumer`) |
| `outbox` | `pubsub/outbox/pgx` | The event is written in the item's transaction and the worker relays it. Needs `-db postgres` and `pubsub` |
| `jobs` | `jobs`, `jobs/pgx` or `jobs/mysql` | Durable background tasks in the service's database (`internal/task`, the `jobs` table migration, `JOBS_WORKERS`): enqueue in a transaction, retries with backoff, periodic runs once across replicas. Needs `-db postgres` or `-db mysql` |
| `webhooks` | `webhook/delivery` | Webhooks sent to customers' endpoints (`internal/outbound`, `WEBHOOK_DELIVERY_*`): one job per message and endpoint in its own queue, signed (Standard Webhooks), SSRF guard on, retried for about 17 hours. Implement `outbound.Endpoints` with your endpoints table. Needs `jobs` |

With `pubsub`, `lease` or `jobs` the binary gets a `worker` command (`server
worker`): deploy it from the same image as a second Deployment.

What `-name` controls in a service: the proto package (dashes become
underscores, so `edge-gw` gives `proto/edge_gw/v1/`), the telemetry service
name and the default `ERROR_DOMAIN`. The example domain is always `item`
(`ItemService`); add others with `zen add domain`.

### `zen new mcp` only

| Flag | Default | When to change it |
|---|---|---|
| `-mcpkit-version` | `v0.1.0` | To require another `github.com/dungnh3/kitchen/mcpkit` version |

In an MCP server, `-name` is the server name reported to clients and the
telemetry service name.

### Examples

```bash
# No database, custom name and directory.
zen new service -module example.com/team/edge-gateway -name edge-gw -dir ./edge -db none -yes

# Kafka events without a database, plus periodic jobs.
zen new service -module example.com/team/notify -db none -with pubsub,lease -pubsub kafka -yes

# Reusable CI workflow pinned to a commit.
zen new mcp -module example.com/team/mcp-tickets -name tickets \
  -ci reusable -ci-workflow acme/devops-workflows/.github/workflows/go.yml@0123456789abcdef0123456789abcdef01234567 -yes
```

## What you get

### `zen new service`

The exact trees are in [testdata/golden](testdata/golden):
`service-postgres` and `service-nodb` (no features), `service-events`
(Postgres, `lease,pubsub,outbox`, Pub/Sub), `service-kafka` (no database,
`pubsub`, Kafka), `service-sqs` (Postgres, `redis,pubsub`, SQS),
`service-jobs` (Postgres, `jobs,webhooks`) and `service-mysql` (MySQL,
`cache,jobs,webhooks`, `-auth introspection`).

| Path | What |
|---|---|
| `proto/<name>/v1/item_service.proto` | The API: an AIP CRUD example (Get, List, Create, Delete), protovalidate rules, REST mapping |
| `gen/` | Generated by `make generate` (pb, grpc, gateway, OpenAPI in `gen/openapi/`); committed |
| `cmd/server/main.go` | `log.Setup`, config, then `serve`, `worker` (with `pubsub`, `lease` or `jobs`) or `migrate up\|down\|status` |
| `internal/config/` | Environment config (caarlos0/env); `ValidateServer` enforces auth and the settings of each dependency |
| `internal/app/deps.go` | Opens and closes what the server and the worker share: telemetry, database, Redis, broker; readiness checks |
| `internal/app/app.go` | Wiring of `serve`: i18n + errs, rate limits, auth, admin port, server |
| `internal/app/worker.go` | Wiring of `worker`: consumers, outbox relay, jobs, tasks, admin port (with `pubsub`, `lease` or `jobs`) |
| `internal/app/jobs.go`, `internal/task/` | The job client, and the tasks it runs: typed args and workers, registered in `task.Register` and scheduled in `task.Periodic` (`jobs`) |
| `internal/outbound/` | Sent webhooks: `Deliveries` builds the jobs, `Endpoints` looks endpoints up, `NewWorker` is registered by the jobs runner (`webhooks`) |
| `internal/inbound/` | Received webhooks, one receiver per `zen add webhook` |
| `internal/app/redis.go`, `broker.go` | `redisx.Open`; the broker client handed to the kitchen adapter (with `redis`, `pubsub`) |
| `internal/events/`, `internal/consumer/`, `internal/job/` | Events and their handlers (`pubsub`), periodic jobs (`lease`) |
| `internal/app/migrate.go` | goose migrations through kitchen `migrate`, with an advisory lock |
| `internal/app/app_test.go` | Integration test of the whole service on random ports (Postgres via testcontainers) |
| `internal/service/` | One file per domain (`item_service.go`: `ItemService` over `store.Items`), opaque page tokens; unit tests |
| `internal/store/` | One file per domain in each package: types and interface (`store.Items`), `memstore`, and `pgstore` with `queries/<domain>.sql` and sqlc output (`sqlcdb`, committed) |
| `migrations/` | goose SQL, embedded in the binary; `00002_create_outbox.sql` with `outbox`, and the `jobs` table with `jobs` (the number after the outbox's) |
| `compose.yaml` | Local Postgres, Redis and broker (emulator) for `make up` |
| `.zen.yaml` | How the project was generated and the hash of each managed file; read by `zen check` |
| `docs/ENV.md` | Every environment variable and what production needs |
| `buf.yaml`, `buf.lock`, `buf.gen.yaml`, `sqlc.yaml` | Code generation; protoc plugins are `tool` deps in `go.mod` |
| `Dockerfile`, `Makefile`, `.golangci.yml`, `.github/` | Build, tasks, lint rules, CI and a PR template |

The generated `docs/ENV.md` lists every environment variable with its
default. Read it first; this file does not repeat it.

### `zen new mcp`

The exact tree is in [testdata/golden/standalone](testdata/golden/standalone)
and [testdata/golden/reusable](testdata/golden/reusable).

| Path | What |
|---|---|
| `cmd/server/main.go` | `log.Setup` (stderr), config, `otelx/sdk`, mcpkit, then HTTP on kitchen `server` or stdio |
| `internal/config/` | Environment config (stdlib), validated: https resource URL, issuers required with auth |
| `internal/tools/` | A typed example tool (`greet`) and its test with the `mcpkittest` in-memory client |
| `Dockerfile`, `Makefile`, `.golangci.yml`, `.github/workflows/ci.yml` | Build, tasks, lint rules, CI |

## Managed files and `zen check`

zen owns a few files of a generated service: `.golangci.yml` (the D5/D7
rules), `Dockerfile`, `.dockerignore`, `.github/workflows/ci.yml` (SHA pins)
and `buf.gen.yaml`. They start with a "Managed by zen" header, and
`.zen.yaml` records the answers used to generate the project and the
sha256 of each managed file. Every other file belongs to the project.

```bash
zen check            # in the project; or: make check
zen check -write     # update the unedited managed files to the current template
```

| Finding | Result |
|---|---|
| A managed file was edited or deleted | Error, exit 1. Restore it, or remove it from `managed` in `.zen.yaml` to own it |
| The current template writes a managed file differently | Warning; `-write` updates it and the hash |
| A managed file is new in the template and absent from the project | Warning; `-write` creates it |
| A newer zen is released | Warning with the command to run (skipped with `-offline` or when the module proxy is unreachable) |

`-write` never touches an edited file. The standalone CI workflow and
`make check` run `zen check` at the version that generated the project,
so CI fails on hand edits but not on a new zen release. With
`-ci=reusable`, add the step to your shared workflow.

| Flag | Default | When to change it |
|---|---|---|
| `-dir` | `.` | To check another directory |
| `-write` | `false` | To update the managed files to the current template |
| `-offline` | `false` | To skip the lookup of the latest zen version |

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
  [server/USAGE.md](../server/USAGE.md) and
  [mcpkit/USAGE.md](../mcpkit/USAGE.md).
- **Telemetry.** `otelx/sdk.Setup` exports over OTLP only when
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set. The service also serves Prometheus
  `/metrics` on `ADMIN_ADDR` through `otelx/prometheus`, and sets
  `IgnoreRemoteSampled` so callers cannot force sampling.
- **Errors.** Return `errs` errors from methods and tools. The service maps
  them to the same `google.rpc.Status` on gRPC and the AIP-193 JSON error on
  REST, localized by `i18n`. In a tool, only the message of an `errs` error
  reaches the model; any other error is logged and becomes "internal
  error".
- **Auth.** The service verifies JWTs with `auth.NewJWKS`, or with
  `-auth introspection` opaque tokens with `auth.NewIntrospection` and
  `auth.RFC7662`; both are fail-closed. The MCP server enables auth when `MCP_RESOURCE_URL` is set.
- **Database.** The service opens Postgres with `pgx.Open` (TLS required
  unless the URL sets `sslmode`, UTC session time zone) and maps errors
  with `pgx.MapError`, or MySQL with `mysqlx.Open` (TLS required unless the
  DSN sets `tls`) and `mysqlx.MapError`. Migrations run through
  `migrate.NewPostgres` (advisory lock) or `migrate.NewMySQL` (lock table).
- **Cache.** `cache.New` on the Redis client; `cachestore.Items` serves
  `GetItem` through `GetOrLoad` and deletes the entry after `DeleteItem`.
- **Redis.** `redisx.Open` with TLS unless `REDIS_TLS=false`; the client
  backs the shared rate limits (`ratelimit/redis`) and the job leases
  (`lease/redis`). See [redisx](../redisx) and [lease](../lease).
- **Events.** The service publishes through a `pubsub.Publisher` from the
  broker adapter (`pubsub/gcp`, `pubsub/kafka`, `pubsub/sqs`). Without
  `outbox` it publishes after the item is stored and logs a failed publish;
  with `outbox` it writes the event in the item's transaction
  (`outboxpgx.Store.Enqueue`) and the worker relays it. The worker consumes
  with `pubsub.NewSubscriber` and a `pubsub.JSON` handler; on Kafka a poison
  message goes to a dead-letter topic after `PUBSUB_MAX_DELIVERY_ATTEMPTS`.
  See [pubsub/USAGE.md](../pubsub/USAGE.md).
- **Jobs.** Each job runs under its own `lease.Elector`, so one replica
  runs it at a time; a run is cancelled when the lease is lost.
- **Tasks.** With `jobs`, `internal/task` holds typed tasks stored in the
  `jobs` table. Insert one in the transaction of the change that needs it
  (`client.InsertTx`) and the worker runs it with retries and backoff; a
  task with an interval also runs on a schedule, once across all worker
  replicas, with no Redis. A task can run twice, so it must be idempotent.
  See [jobs/USAGE.md](../jobs/USAGE.md).
- **Webhooks.** With `webhooks`, `outbound.Deliveries` returns the jobs of
  one event for its endpoints; insert them with `InsertManyTx` in the
  transaction of the change. Received webhooks come from `zen add webhook`.
  Both use the Standard Webhooks format; see
  [webhook/USAGE.md](../webhook/USAGE.md).
- **Lint.** `.golangci.yml` mirrors the repo's D5/D7 rules with depguard,
  forbidigo and sloglint. It denies `log/slog`, raw Redis, SQL, HTTP and gRPC
  clients, `http.Server`, `status.New`, OTel SDK setup and so on. Tests may
  use the libraries directly. Keep the file when you change the project.

## `zen add`

Run it in a generated service; it reads `.zen.yaml` for the module,
database and features. Names are lower-case words separated by dashes.

```bash
zen add domain order-item          # or: -plural people for an irregular plural
zen add consumer payments          # needs -with pubsub
zen add job cleanup -every 5m      # needs -with lease
zen add task send-email            # needs -with jobs; add -every 1h to run it on a schedule
zen add client billing             # a gRPC client of the billing service
zen add webhook payments           # a receiver of signed webhooks on POST /webhooks/payments
```

| Command | Writes | Changes |
|---|---|---|
| `add domain <name>` | `proto/<pkg>/v1/<name>_service.proto` (same proto package; CRUD with protovalidate and REST at `/v1/<plural>`), `internal/service/<name>_service.go` and test, `internal/store/<name>.go`, `memstore/<name>.go`, and with Postgres `pgstore/<name>.go`, `pgstore/queries/<name>.sql` and the next migration | `internal/app/app.go` (store and service); then runs `buf generate` and `sqlc generate` (`-no-generate` to skip) |
| `add consumer <name>` | `internal/consumer/<name>.go` (a `pubsub.JSON` handler over a message type to adapt) and test | `config.go` (a `PUBSUB_<NAME>_*` subscription, validated), `worker.go` (subscribe), the tests' config, `docs/ENV.md`, the `Makefile` local settings and the topic, subscription or queue that `make up` creates |
| `add job <name>` | `internal/job/<name>.go` and test | `worker.go` (the job list) |
| `add task <name>` | `internal/task/<name>.go` (typed args, worker, `Register<Name>`, and `<Name>Every` with `-every`) and test | `internal/task/task.go` (`Register`, and `Periodic` with `-every`) |
| `add client <name>` | `internal/app/client_<name>.go` (opens a `grpcx.Conn`: TLS with the system roots or `<NAME>_CA_FILE`, plaintext only with `<NAME>_INSECURE=true`, the service's logger and telemetry, `<NAME>_TIMEOUT`, the circuit breaker on) and test | `config.go` (a `config.Client` under `Clients` with `<NAME>_ADDR` required, validated), `deps.go` (the `d.<name>` field, opened with the other dependencies and closed after them), the tests' config, `docs/ENV.md` and the `Makefile` local settings |
| `add webhook <name>` | `internal/inbound/<name>.go` (a `server.Service` with the route `POST /webhooks/<name>` behind `webhook.NewMiddleware`: signature, 1 MiB body limit, and with `redis` one run per message id) and test; `internal/inbound/inbound.go` the first time | `config.go` (a `config.Webhook` under `Webhooks` with `<NAME>_WEBHOOK_SECRET` required and parsed), `app.go` (the receiver, and its route in `webhookRoutes`, which the authenticator lets through), the tests' config, `docs/ENV.md` and the `Makefile` local secret |

Changes go before marker comments such as `// zen:services` in the
generated files; keep the markers. Every file and marker is checked before
anything is written: an existing file, a taken name or a missing marker
stops the command and changes nothing, with the error naming the file. The
names of the services, stores, consumers and jobs come from `<name>`, so
`order-item` gives `OrderItemService`, `store.OrderItems`, the table
`order_items` and the REST collection `/v1/orderItems`.

| Flag | Commands | Default | Meaning |
|---|---|---|---|
| `-dir` | all | `.` | Project directory |
| `-plural` | `domain` | regular English plural | Plural for an irregular noun, for example `-plural people` |
| `-no-generate` | `domain` | `false` | Skip `buf generate` and `sqlc generate`; run `make generate` later |
| `-every` | `job` | `1m` | Time between two runs |
| `-every` | `task` | none | Also run the task on a schedule: time between two runs (at least `1s`, counted from the Unix epoch) |

`zen add client` does not touch readiness: the connection is made on the
first call, so the service starts and stays ready while the downstream is
down, and calls fail with `Unavailable` instead. Build the downstream's
generated client from `d.<name>` (for example
`billingv1.NewBillingServiceClient(d.billing)`) and pass it to your
services. The `Makefile` points it at `localhost:10001` in plaintext for
`make run`; change the port to your local server. Names that would clash
with the service's own settings (`redis`, `db`, `grpc`, `auth`, ...) are
refused. See [grpcx/USAGE.md](../grpcx/USAGE.md) for the defaults.

Add an RPC to an existing service, or a migration, by hand: see below.
`buf` and `goose` already do those, so there is no `zen add rpc` or
`zen add migration`.

## Advanced

### Add an RPC to a service

1. Edit `proto/<name>/v1/*.proto`. Validation rules go in the proto file;
   `server` enforces them before your method runs.
2. With Postgres, add a migration in `migrations/` and queries in
   `internal/store/pgstore/queries/<domain>.sql`.
3. Run `make generate`. It runs `buf generate` and `sqlc generate`.
4. Implement the method in `internal/service`.
5. Run `make lint` (golangci-lint, `buf lint`, `sqlc vet`) and
   `make breaking` to compare the API with `main`.

`gen/` and `sqlcdb/` are committed. CI runs `make generate` and fails if
they are stale. A new gRPC service type is registered in the `services`
list of `internal/app/app.go` (`zen add domain` does it for you):

```go
services := []server.Service{
	service.NewItemService(items),
	// zen:services
}
```

### Consumers, jobs and tasks

Task workers must be idempotent too: a task can run twice. Keep a task's
`Kind` stable while jobs of it are queued.

Consumer handlers must be idempotent: return `nil` to ack, an error to
retry and `pubsub.Permanent(err)` to dead-letter. Keep a job's `Name`
stable (it names the lease) and stop the run when its context is
cancelled.

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

The simplest way is to generate a second project with `-db postgres` (or
`-db mysql`) and copy `internal/app/migrate.go`, `internal/store/pgstore/`
(or `mysqlstore/`), `migrations/`, `sqlc.yaml`, and the `Database` config
and `openDB` wiring from [service-postgres](testdata/golden/service-postgres)
(or [service-mysql](testdata/golden/service-mysql)).

For MongoDB there is no template. Implement the store interfaces and open
the client in `internal/app` with `mongox.Open`; never call the driver
directly (the lint config denies it). See [mongox](../mongox).

### Auth

A service needs `AUTH_ISSUER`, `AUTH_JWKS_URL` (https, http only for
localhost) and `AUTH_AUDIENCE`. An unreachable JWKS stops startup. Only
`/grpc.health.v1.Health/` is public. To require scopes per method, set
`Authorizer: auth.RequireScopes(...)` in the `auth.Config` built by
`newAuthenticator` in `internal/app/app.go`.

With `-auth introspection` a service needs `AUTH_INTROSPECTION_URL` (https,
http only for localhost), `AUTH_CLIENT_ID`, `AUTH_CLIENT_SECRET` and
`AUTH_AUDIENCE`; with Redis also `AUTH_INTROSPECTION_CACHE_KEY` (at least 32
random bytes, the same on every instance). Answers are cached for up to
30s, so a revoked token stops working within that time. An unreachable
endpoint makes requests fail with 503, never pass. JWT access tokens are
rejected in this mode; use `-auth jwks` for them.

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
- `memstore` is the fake for the store interfaces; there are no mocks to
  generate.
- Config tests cover the environment parsing and validation.
- Lint: `make lint` runs golangci-lint v2.15.0 with `go run` in both
  generated projects, so nothing has to be installed.

The generator itself, from `zen`:

```bash
go test ./...            # golden files, validation, exit codes, generated builds
go test -short ./...     # skip the builds of generated projects (no network)
go test ./... -update    # rewrite testdata/golden after a template change
```

The check tests generate a service and edit, delete, own and age its
managed files; the build tests also run `zen check` on every generated
service after code generation. The add tests compare what `zen add`
writes with `testdata/golden/add-*`, check every refusal, and that a missing
marker writes nothing; the build tests run the adds on the events, Kafka,
SQS and MySQL services (two clients on MySQL) before building them.

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
- Keep `.golangci.yml` and the SHA pins in CI; `zen check` fails when
  a managed file is edited.
- `make docker` builds a distroless nonroot image. For private modules pass
  `--build-arg GOPRIVATE=...` and `--secret id=netrc,src=$HOME/.netrc`.

## Migrating from the old kitchen

The old generators (the interactive MCP scaffolder, the service scaffolder
and `gen`) are replaced by `zen new mcp` and `zen new service`;
`gen` has no replacement. See
[docs/migration/codegen-mcp.md](../docs/migration/codegen-mcp.md).
