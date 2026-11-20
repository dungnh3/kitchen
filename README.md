# kitchen

Shared Go libraries for building microservices: logging and telemetry, gRPC/HTTP
server, errors, auth, rate limiting, messaging, SSE streaming, storage helpers,
MCP tooling and project scaffolding.

This is a clean-break redesign of `github.com/vulcanlabsvn/kitchen`.

- Conventions and module layout: [docs/decisions.md](docs/decisions.md)
- Review of the old packages (what was outdated and why): [docs/review/](docs/review/)
- Per-area designs: [docs/design/](docs/design/)
- Migration guides from the old library: `docs/migration/`

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
