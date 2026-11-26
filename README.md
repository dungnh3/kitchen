# kitchen

Shared Go libraries for building microservices: logging and telemetry, gRPC/HTTP
server, errors, auth, rate limiting, messaging, SSE streaming, storage helpers,
MCP tooling and project scaffolding.

It is a clean-break redesign of an earlier internal kitchen library.

- Conventions and module layout: [docs/decisions.md](docs/decisions.md)
- Per-area designs: [docs/design/](docs/design/)
- Migration guides from the old library: `docs/migration/`

## Packages

Each package has a `USAGE.md`. It starts from the defaults, moves on to
advanced setups, and shows how the package fits with the rest of kitchen.
Start with [server](server/USAGE.md), which wires up a complete service.

| Concern | Package | Usage |
|---|---|---|
| gRPC + REST server, health, admin port | `server` | [USAGE](server/USAGE.md) |
| Logging | `log` (+ `log/gormlog`) | [USAGE](log/USAGE.md) |
| Errors (AIP-193), HTTP/gRPC status | `errs` (+ `errs/validation`) | [USAGE](errs/USAGE.md) |
| i18n of error messages | `i18n` | [USAGE](i18n/USAGE.md) |
| Request metadata (request/user id, client IP) | `context` | [USAGE](context/USAGE.md) |
| Telemetry setup and exporters | `otelx` | [USAGE](otelx/USAGE.md) |
| Authentication (JWT, JWKS, introspection) | `auth` | [USAGE](auth/USAGE.md) |
| Rate limiting | `ratelimit` | [USAGE](ratelimit/USAGE.md) |
| HTTP client | `httpx` | [USAGE](httpx/USAGE.md) |
| Messaging (GCP, SQS, Kafka, outbox, reply) | `pubsub` | [USAGE](pubsub/USAGE.md) |
| Server-sent events | `sse` | [USAGE](sse/USAGE.md) |
| Redis client | `redisx` | [USAGE](redisx/USAGE.md) |
| Cache (L1 + Redis) | `cache` | [USAGE](cache/USAGE.md) |
| Distributed lock | `lease` | [USAGE](lease/USAGE.md) |
| Object storage (GCS, S3) | `objstore` | [USAGE](objstore/USAGE.md) |
| PostgreSQL | `pg` (+ `pg/gorm`) | [USAGE](pg/USAGE.md) |
| MySQL | `mysqlx` (+ `mysqlx/gorm`) | [USAGE](mysqlx/USAGE.md) |
| MongoDB | `mongox` | [USAGE](mongox/USAGE.md) |
| Database migrations | `migrate` (+ `kmigrate`) | [USAGE](migrate/USAGE.md) |
| Backoff and jitter | `timex` | [USAGE](timex/USAGE.md) |
| Web page metadata | `webmeta` | [USAGE](webmeta/USAGE.md) |
| MCP servers | `mcpkit` | [USAGE](mcpkit/USAGE.md) |
| Project scaffolding (`zen new service`, `zen new mcp`) | `zen` | [USAGE](zen/USAGE.md) |

There is one canonical package per concern; see D7 in
[docs/decisions.md](docs/decisions.md).

## Requirements

Go 1.26 or newer.

## Development

```bash
make test   # go test -race for every module
make lint   # golangci-lint v2 for every module
make tidy
```

Use an uncommitted `go.work` to develop across the core module and submodules.

Messaging (`pubsub`, `lease`): see [docs/design/messaging.md](docs/design/messaging.md).
Set `KITCHEN_TEST_PG_DSN` to run the Postgres outbox tests.
