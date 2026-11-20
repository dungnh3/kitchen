# Design: codegen-mcp (`mcpkit`, `cmd/kitchen`)

Status: accepted 2026-11-20. Old packages covered: `mcpkit`, `nest`, `gen`,
and the two one-shot scaffolders (MCP server, gRPC service). Review of the old
code: [docs/review/07-codegen-mcp.md](../review/07-codegen-mcp.md). Migration:
[docs/migration/codegen-mcp.md](../migration/codegen-mcp.md).

## 1. Summary

| Old | New | Module |
|---|---|---|
| `mcpkit` (builder, registries, own `http.Server`, SSE) | `mcpkit`: thin typed layer over go-sdk, `http.Handler` only | `github.com/dungnh3/kitchen/mcpkit` (submodule) |
| – | `mcpkit/jwtauth`: JWT/JWKS verifier for an external IdP | same submodule |
| – | `mcpkit/mcpkittest`: in-memory client for tool tests | same submodule |
| `nest` (fluent HTTP client) | dropped; use `net/http` + `golang.org/x/oauth2` + `otelhttp` | – |
| `gen` (add CRUD model) | dropped | – |
| MCP scaffolder + service scaffolder | one CLI: `kitchen new mcp` (now), `kitchen new service` (later PR) | `github.com/dungnh3/kitchen/cmd/kitchen` (submodule) |

Versions verified on 2026-11-20: go-sdk v1.8.0 (latest stable; v1.9.0-pre.1
exists), MCP spec 2026-07-28 (SDK falls back to 2025-11-25 automatically),
huh v2.0.3 (`charm.land/huh/v2`), go-jose v4.1.9, OTel Go v1.47.0 (semconv
v1.41.0 has `mcpconv`), Go 1.27.4 latest / 1.26 supported, Alpine 3.24,
`gcr.io/distroless/static-debian13:nonroot`, actions/checkout v7.0.1,
actions/setup-go v7.1.0, golangci-lint-action v9.3.0, golangci-lint v2.15.0,
docker/setup-buildx-action v4.4.1, docker/build-push-action v7.4.0 (SHAs from
`git ls-remote`).

## 2. `mcpkit`

### API

```go
srv, err := mcpkit.New(&mcp.Implementation{Name: "notes", Version: v},
    mcpkit.WithLogger(logger),                 // nil -> slog.Default()
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

### `jwtauth`

`jwtauth.New(Config{Issuer, Audience, JWKSURL?, Algorithms?, ...})` returns
a verifier whose `Verify` method is an `auth.TokenVerifier`.

- **Claims checked:** signature against the JWKS, the asymmetric algorithm
  allow-list (HS* and `none` are rejected at `New`), `iss`, `aud` equal to the
  resource URL (RFC 8707), and `exp`, which is required (with `nbf`/`iat` and
  30s leeway). A `typ` header, if present, must be `at+jwt` or `JWT`. Scopes
  come from `scope` (space-separated) or `scp` (array).
- **JWKS location:** taken from `JWKSURL`, or discovered from the issuer's
  authorization-server metadata.
- **Key cache:** refreshed when the TTL (default 1h) expires or on an unknown
  `kid`. Refresh attempts are rate-limited to one per `MinRefreshInterval`
  (default 1m). Stale keys keep being served while the issuer is unreachable.
  No goroutines.
- **Errors:** an invalid token wraps `auth.ErrInvalidToken` (401). Unreachable
  keys give a different error (500), so an IdP outage is not reported as a bad
  token.

## 3. `cmd/kitchen`

```
kitchen new mcp -module github.com/acme/mcp-notes [-dir] [-name] [-description]
                [-ci standalone|reusable] [-ci-workflow owner/repo/.github/workflows/x.yml@<sha>]
                [-mcpkit-version v0.1.0] [-yes]
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
cmd/server/main.go           env config, slog JSON to stderr, http|stdio, graceful shutdown
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

## 4. Sequencing

1. **This PR:** mcpkit, jwtauth, mcpkittest, the CLI framework and `new mcp`
   (it needs only mcpkit, slog and stdlib).
2. **After transport, observability, errors-auth, data-utils and messaging
   merge:** `kitchen new service`. The stack is gRPC + grpc-gateway, buf v2 +
   protovalidate, pgx/v5 + sqlc, and optional Pub/Sub via kitchen `pubsub`;
   it is built on the new kitchen packages. Until then `kitchen new service`
   prints a clear "not available yet" error.
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
| A6 | `jwtauth` uses **go-jose v4**, although go-sdk already pulls `golang-jwt/jwt/v5` transitively | golang-jwt v5 + hand-written JWK parsing; reuse errors-auth `auth/` later | Mature JWK/JWKS parsing; no hand-rolled crypto key decoding | One extra direct dependency in the mcpkit module |
| A7 | `PassThrough` needs an explicit `AllowMissingExpiration: true`; JWTs must carry `exp` | PassThrough fakes an expiry | Never weakens expiry checks silently | One more field to set (shown in the examples and the template) |
| A8 | A tool panic returns a generic "internal error"; details go only to the log | return the panic text | No information leak to the model or client | Callers must read logs to debug |
| A9 | **No default tool timeout** in the library; the generated server sets `MCP_TOOL_TIMEOUT=2m` | a library default | The library has no hidden policy; the app decides | A server built without the template has no deadline unless it sets one |
| A10 | Timeout errors become a tool error naming the tool and the duration | keep the raw `context deadline exceeded` | The model can understand and retry | – |
| A11 | CORS uses an exact-origin allow-list combined with stdlib `CrossOriginProtection`; a denied preflight gets 403; no wildcard | reflect any origin (old behaviour) | Closes the old "empty list reflects every origin" hole; defends against cross-site writes | Browser clients must be listed explicitly |
| A12 | `PropagateRequestCancellation: true` | SDK default false | A tool stops when the client goes away (2026-07-28 clients); no wasted work under load | A tool that must finish after a disconnect has to detach its context itself |
| A13 | `jwtauth`: asymmetric algorithms only; `exp` required; `aud` must equal the resource URL; JWKS refresh rate-limited; stale keys served during outages; key-fetch errors are 500, not 401 | allow HS256; optional `exp` | Defends against algorithm confusion, token replay to other resources and fetch storms | Shared-secret IdPs are not supported (by design) |
| A14 | The `-ci reusable` workflow must be pinned to a 40-character commit SHA | allow tags/branches | Supply-chain safety (the old template used `@master`) | Updating means changing the SHA (use Renovate) |
| A15 | Dockerfile: private modules via `GOPRIVATE` build-arg + a BuildKit `netrc` secret; no `GOSUMDB=off`; `CGO_ENABLED=0`, `-trimpath`, nonroot | git token in a layer (old) | No credential in any layer; checksum DB stays on | The build needs BuildKit (default in Docker 23+) |
| A16 | The generated config requires an `https` `MCP_RESOURCE_URL` (http only for localhost) plus at least one issuer when auth is enabled | accept any URL | Prevents leaking tokens over plain HTTP | Local non-localhost HTTP testing needs auth off |
| A17 | The generated stdio mode uses `UPSTREAM_TOKEN` when set; otherwise it runs without a token | require the token | Tools without upstream calls work out of the box | A missing token surfaces in the tool, not at startup |
| A18 | The CLI does not run `go mod tidy`; it prints next steps | run tidy automatically | Works offline and before mcpkit is tagged; no network side effects | One manual step |
| A19 | `kitchen new service` exists and returns "not available yet" until the service template lands | hide the subcommand | Discoverable; the help text explains the plan | – |
| A20 | Telemetry uses semconv **v1.41.0** `mcpconv` (the newest with MCP metrics in OTel Go v1.47) | v1.43 attribute keys with hand-written metric | Generated instrument definitions | Bump when a newer semconv ships `mcpconv` |
| A21 | `mcpkittest.Connect(t, srv, token)` is part of the public API | tests build in-memory transports by hand | Generated projects and users test tools in three lines | One more public package to keep stable |

## 7. Not carried over (and why)

- Builder, `Config`/`HTTPTransportConfig`, registries and `deprecated.go`: the
  SDK and functional options cover them.
- Own `http.Server`, TLS, gzip and the 60s `WriteTimeout`: these belong to
  the app/server; gzip was dead code for SSE.
- Resource TTL cache: keyed by URI only, so it leaked one user's data to
  another. Cache per principal inside the handler if needed.
- MCP logging capability: deprecated in 2026-07-28; use OTel and slog.
- Legacy SSE transport: deprecated.
- `nest`: data race, broken retry, no tests; the stdlib + `x/oauth2` cover the
  real use (Google OAuth token exchange).
- `gen`: in 65 consumer services no file it generates exists; AST-based
  `add model` is not worth maintaining.
