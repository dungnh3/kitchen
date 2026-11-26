# Migration: codegen-mcp (`mcpkit`, `nest`, generators)

Design and rationale: [docs/design/codegen-mcp.md](../design/codegen-mcp.md).
This is a clean break: there are no compatibility shims. In the commands
below `OLD` stands for the old kitchen module path.

```bash
go get github.com/dungnh3/kitchen/mcpkit@latest github.com/modelcontextprotocol/go-sdk@v1.8.0
gofmt -w -r 'OLD/mcpkit -> github.com/dungnh3/kitchen/mcpkit' .   # then fix call sites below
```

## 1. `mcpkit`: symbol map

| Old | New |
|---|---|
| `mcpkit.NewServer().WithName(n).WithVersion(v)...Build()` | `mcpkit.New(&mcp.Implementation{Name: n, Version: v}, opts...)` |
| `.WithInstructions(s)` | `mcpkit.WithInstructions(s)` |
| `.WithLogger(xlog.LoggerInterface)` | `mcpkit.WithLogger(*log.Logger)` (kitchen `log`; nil → `log.Default()`, also used by the MCP SDK) |
| `.WithKeepAlive(d)` | `mcpkit.WithServerOptions(func(o *mcp.ServerOptions) { o.KeepAlive = d })` |
| `.WithServerOptions(fn)` | `mcpkit.WithServerOptions(fn)` |
| `.WithProtocolVersion(v)` | removed; the SDK negotiates 2026-07-28 and falls back to 2025-11-25 |
| `.EnableTools/Resources/Prompts[WithOptions]()` | removed; capabilities follow what you register |
| `.EnableLogging()` | removed (MCP logging is deprecated); use kitchen `log` + OTel |
| `.WithStdioTransport()` + `Start/Serve/Wait` | `srv.RunStdio(ctx, mcpkit.EnvToken("UPSTREAM_TOKEN"))` |
| `.WithHTTPTransport(cfg)`, `WithHTTPPort/Address`, `WithTLS`, `Start/Stop` | `server.New(server.WithServices(srv.Service("/mcp")), server.WithGRPCAddr(""))` (kitchen `server`, HTTP only), or mount `srv.Handler()` on your own `http.Server` (see §3) |
| `.WithSSETransport(cfg)` | removed (HTTP+SSE is deprecated); clients use Streamable HTTP |
| `.WithCORS(origins...)` | `mcpkit.WithAllowedOrigins(origins...)` |
| `HTTPTransportConfig.Stateless` | default; `mcpkit.WithStateful(idleTimeout)` to opt out |
| `HTTPTransportConfig.MaxBodyBytes` | `mcpkit.WithMaxRequestBodyBytes(n)` |
| `srv.HTTPHandler()` / `srv.ServeHTTP` | `srv.Handler()` |
| `srv.SDK()` | `srv.SDK()` |
| `mcpkit.NewTool(def, rawHandler)` + `RegisterTool` | `mcpkit.AddTool(srv, def, typedHandler)` |
| `mcpkit.SimpleTool(name, desc, map[string]any{...}, h)` | `mcpkit.AddTool` with `In` struct + `json`/`jsonschema` tags |
| `tool.WithTimeout(d)` / `WithTimeoutSeconds` | `mcpkit.AddTool(..., mcpkit.Timeout(d))` or `mcpkit.WithToolTimeout(d)` |
| `tool.WithMiddleware(ToolMiddleware...)` | wrap the typed handler (see below) or `mcpkit.WithMiddleware(mcp.Middleware)` |
| `tool.WithTags(...)` | removed |
| `NewResource`/`SimpleResource` + `RegisterResource` | `srv.SDK().AddResource(def, handler)` |
| `resource.WithCache(ttl)` | removed (see §4); cache per principal inside the handler |
| `NewPrompt`/`SimplePrompt` + `RegisterPrompt` | `srv.SDK().AddPrompt(def, handler)` |
| `PanicError` | removed; panics become `IsError` "internal error" and are logged |
| hand-written `BearerAuthMiddleware` + token middleware | `mcpkit.WithAuth(mcpkit.Auth{Verifier: mcpkit.PassThrough(), AllowMissingExpiration: true, Metadata: ...})` |
| `auth.TokenFromContext(ctx)` (per-repo helper) | `mcpkit.TokenFrom(ctx)` |
| token in `params._meta.token` (stdio) | `UPSTREAM_TOKEN` env via `mcpkit.EnvToken` |

### Tool handler rewrite

```go
// Before
mcpkit.NewTool(&mcpsdk.Tool{Name: "search", InputSchema: obj(map[string]any{"query": str("...")})},
    func(ctx context.Context, req *mcpsdk.CallToolRequest) (*mcpsdk.CallToolResult, error) {
        var in struct{ Query string `json:"query"` }
        if err := json.Unmarshal(req.Params.Arguments, &in); err != nil { return nil, err }
        ...
        return &mcpsdk.CallToolResult{Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: string(b)}}}, nil
    })

// After
type SearchIn struct {
    Query string `json:"query" jsonschema:"Gmail search query"`
}
type SearchOut struct {
    Messages []Message `json:"messages"`
}
mcpkit.AddTool(srv, &mcp.Tool{Name: "search", Description: "..."},
    func(ctx context.Context, _ *mcp.CallToolRequest, in SearchIn) (*mcp.CallToolResult, SearchOut, error) {
        msgs, err := gmail.Search(ctx, mcpkit.TokenFrom(ctx), in.Query)
        return nil, SearchOut{Messages: msgs}, err // err -> IsError result
    })
```

A per-tool middleware becomes a generic wrapper:

```go
func requireToken[In, Out any](h mcp.ToolHandlerFor[In, Out]) mcp.ToolHandlerFor[In, Out] {
    return func(ctx context.Context, r *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
        if mcpkit.TokenFrom(ctx) == "" {
            var zero Out
            return nil, zero, errors.New("not signed in")
        }
        return h(ctx, r, in)
    }
}
```

## 2. Behaviour changes, including every old bug that is now fixed

| Area | Old behaviour (bug or change) | New behaviour |
|---|---|---|
| Schemas | Tools registered raw; hand-written `map[string]any` schemas; no input validation | Schemas inferred from Go types; input validated before the handler; output schema + `structuredContent` |
| Tool errors | Handler errors and timeouts became JSON-RPC errors (-32603) the model could not read | Returned errors and timeouts become `isError: true` results; the model sees the message of client-fault `errs` errors only, anything else becomes "internal error" (logged); `*jsonrpc.Error` stays a protocol error |
| Tool timeout | One goroutine per call; leaked when the handler ignored ctx | `context.WithTimeoutCause`, no goroutine; error text `tool "x" timed out after d` |
| Panics | Recovered per kind; the panic text could reach the client | Recovered for every method by middleware; client sees "internal error", log gets value + stack |
| HTTP `WriteTimeout` | 60s default on mcpkit's own `http.Server` cut streamed responses and tools longer than 60s | mcpkit owns no server; the template sets no `WriteTimeout` and uses `MCP_TOOL_TIMEOUT` |
| Resource cache | Unbounded map keyed by URI only; served one user's per-token data to another | Removed |
| CORS | Empty origin list reflected any origin; OPTIONS answered 204 for disallowed origins | No CORS unless origins are listed; exact match (rs/cors); disallowed preflight gets no CORS headers; stdlib cross-origin protection blocks cross-site writes |
| gzip | Never ran for spec clients (they send `Accept: text/event-stream`); wrote trailer bytes on 202/204 | Removed |
| Tool order | Registry iterated a map: `tools/list` order changed between restarts (hurt prompt caching) | Sorted by name |
| MCP logging | Enabled by default though deprecated | Not enabled; use kitchen `log`/OTel |
| Auth | None built in; each server trusted any `Authorization` header and continued without a token | `WithAuth` requires a token: 401 + `WWW-Authenticate` (`resource_metadata`, scopes), 403 on missing scope, RFC 9728 metadata endpoint |
| JWT | – | kitchen `auth` (`auth.NewJWKS`, introspection, ...) through `mcpkit.Verifier(v, resourceURL)`: `aud` must contain the resource URL; principal via `auth.FromContext` |
| Tool error text | the raw error string reached the model | only `errs` messages reach the model; return `errs.ErrInvalidArgument.With(errs.Message("..."))` for errors the model should read, anything else becomes "internal error" (and is logged) |
| HTTP hosting | mcpkit's own `http.Server` (`Start/Stop/Wait`, `HTTPTransportConfig`) | `server.New(server.WithServices(srv.Service("/mcp")), server.WithGRPCAddr(""))` from kitchen's `server` (HTTP only; timeouts, drain, `/livez`, `/readyz`), or mount `srv.Handler()` yourself |
| stdio token | Read from `params._meta.token` in each call (credential inside the JSON-RPC payload) | Read once from the environment (`TokenSource`) |
| Transport | Legacy HTTP+SSE offered | Removed; stdio + Streamable HTTP (stateless default) |
| Cancellation | Tool kept running after the HTTP client disconnected | `PropagateRequestCancellation` cancels the tool's ctx (2026-07-28 clients) |
| Telemetry | Custom request log | OTel span `{method} {target}` + `mcp.server.operation.duration` (MCP semconv) |

## 3. Serving over HTTP

Preferred: kitchen's `server` (HTTP only), as the generated template does:

```go
hs, err := server.New(
	server.WithServices(srv.Service("/mcp")), // + RFC 9728 metadata when configured
	server.WithGRPCAddr(""),                  // no public gRPC port
	server.WithHTTPAddr(":8080"),
	server.WithLogger(logger),
)
err = hs.Run(ctx) // /livez, /readyz, drain, access log, OTel; no WriteTimeout
```

Or your own `http.Server`:

```go
mux := http.NewServeMux()
mux.Handle("/mcp", srv.Handler())
if p := srv.MetadataPath(); p != "" {
    mux.Handle("GET "+p, srv.MetadataHandler())
}
hs := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
// No WriteTimeout. Shut down with hs.Shutdown(ctx) on SIGTERM.
```

## 4. `nest` is removed

Its only real use was exchanging Google OAuth tokens. It had a request-body
data race (a pooled buffer was returned before the request was sent), a lost
cancel func, a retry that failed on the first attempt for any request with a
body, and a 401 refresh path that re-sent a consumed body. Replace it with
`golang.org/x/oauth2` for the token flow and kitchen's `httpx` for calls
(owner, 2026-11-21: one HTTP client standard — retries, circuit breaker,
`errs` error mapping and OTel come with it):

```go
cfg := &oauth2.Config{ClientID: id, ClientSecret: secret, Endpoint: google.Endpoint, RedirectURL: cb, Scopes: scopes}
tok, err := cfg.Exchange(ctx, code) // authorization code -> token
ts := cfg.TokenSource(ctx, tok)     // auto-refresh

api, err := httpx.New(
	httpx.WithBaseURL("https://gmail.googleapis.com"),
	httpx.WithTransportMiddleware(func(rt http.RoundTripper) http.RoundTripper {
		return &oauth2.Transport{Source: ts, Base: rt} // Authorization header per request
	}),
	httpx.WithRetry(httpx.RetryPolicy{}), // rewinds bodies itself
)
var out Messages
err = api.Get("/gmail/v1/users/me/messages").Query("q", q).Do(ctx, &out)
```

Circuit breakers and rate limiters were unused interfaces in `nest`;
`httpx.WithCircuitBreaker` covers the former when needed.

## 5. Generators

| Old | New |
|---|---|
| MCP scaffolder (interactive, `bufio`) | `zen new mcp -module <path> [-yes]` (huh prompts on a TTY) |
| gRPC service scaffolder | `zen new service -module <path> [-db postgres\|none] [-yes]` (§6) |
| `gen` (add CRUD model) | removed |

Install: `go install github.com/dungnh3/kitchen/zen@latest`.

Fixed in the generated output:

- **Overwriting:** the old scaffolder overwrote existing files without
  checking. The new one refuses a non-empty directory and writes only inside
  the target (`os.Root`).
- **Dead options:** the old prompts for auth, resources and prompts did
  nothing. Every flag now changes the output and is covered by golden tests.
- **Formatting:** the old output was not gofmt'd and template errors surfaced
  only at build time. Output is gofmt'd, and a golden test builds, vets and
  tests the generated project.
- **Org rewriting:** the old service scaffolder replaced every occurrence of
  the source org, which broke kitchen and CI imports for any other org. The
  new templates are org-neutral; only the module path you pass is used.
- **Name mangling:** `user_profile` became `User_profile`, and the `ms-`
  prefix stripping was inconsistent. The name is validated as
  `^[a-z][a-z0-9-]*[a-z0-9]$`, and the module path with `module.CheckPath`.
- **Go versions:** go 1.21 in the generator, 1.25/1.26 in the output, and the
  Dockerfile's Go version not updated. The output now uses `go 1.26.0` in
  go.mod and a `golang:1.27-alpine3.24` builder; the generator module is go
  1.26.0.
- **Runtime image:** `alpine:3.12`/`3.18` (both end-of-life) is replaced by
  `gcr.io/distroless/static-debian13:nonroot`, which runs as non-root.
- **Private modules:** `GOSUMDB=off`, `GONOSUMCHECK` (a variable that does not
  exist) and `GITHUB_TOKEN` without an `insteadOf` rule are gone. Private
  modules now use `GOPRIVATE` plus a BuildKit `netrc` secret, and the
  checksum DB stays on.
- **Pinning:** CI used `@master` workflows and an unpinned `go get -u`. Every
  action is now pinned to a commit SHA, and a reusable workflow must be
  pinned to a full SHA.
- **Dependency versions:** mixed kitchen pseudo-versions and a `kitchen/log`
  marked indirect are replaced by explicit `mcpkit` and `go-sdk` versions.
- **Prompt library:** survey is archived; prompts now use huh v2 and can be
  skipped with `-yes` for CI.

## 6. Service scaffolder → `zen new service`

```bash
zen new service -module github.com/acme/orders        # Postgres (default)
zen new service -module github.com/acme/edge -db none
```

The CLI writes the project and runs `go mod tidy -e`, `buf generate` and
`go mod tidy` (pass `-no-generate` to do it yourself). Then `make db-up`,
`make run`, `make test`.

| Old generated project | New generated project |
|---|---|
| `config/default.yaml` + viper, passwords in the defaults | env only (caarlos0/env), validated at startup, no secret defaults |
| five `<service>_*.proto` files, public + `Internal*Service` split by name | one `proto/<name>/v1/item_service.proto`; authorization by `auth.RequireScopes`, not by service name |
| PGV (`protoc-gen-validate`), remote plugins unpinned | protovalidate, enforced by `server`; local plugins pinned in go.mod, `buf.lock` committed |
| openapiv2 + redocly HTML | openapiv2 JSON in `gen/openapi/` |
| `internal/middleware` (recovery, tracing, auth selector regex, App Check, guest, mock user, blacklist) | kitchen `server` (recovery, request info, errors, validation, tracing) + `auth` + `ratelimit` |
| Auth through gRPC calls to the user service | `auth.NewJWKS` (fail-closed); for opaque tokens add `auth.NewIntrospection` |
| `localization` + `internal/errors/langs/en.json` | `errs` + `i18n` (built-in catalog; add messages with `i18n.WithFS`) |
| gorm + dbresolver + lib/pq, `internal/repositories` errors | `pgx` (pgx, TLS required) + sqlc; `pgx.MapError` gives `errs` sentinels |
| `migrate` CLI in the Makefile | `server migrate up\|down\|status` (goose, embedded, advisory lock) or `DB_MIGRATE_ON_START=true` |
| zap global logger, Coralogix tracing code | `log` (slog) and `otelx/sdk` (standard `OTEL_*` variables) |
| `pkg/gcppubsub` publisher + retry-go | `pubsub` adapters; transactional outbox `pubsub/outbox/pgx` |
| `internal/cache` (Redis cache + locker) | `cache`, `redisx`, `lease` when needed |
| `/pb.HealthService` Liveness/Readiness RPCs | `/livez`, `/readyz` (with a Postgres check) and `grpc.health.v1` |
| Dockerfile with a token in a layer, `GOSUMDB off`, `alpine:3.12`, root | BuildKit netrc secret, checksum DB on, distroless nonroot |
| CI: reusable workflow `@master`, docs workflow | SHA-pinned actions (or a SHA-pinned reusable workflow), race tests, golangci-lint, buf lint/breaking, generated-code drift check |
| No tests | unit tests, config tests, and an integration test of the whole service with testcontainers Postgres |

Not carried over:

- **Scheduler** (`internal/scheduler`): every pod ran every task. Use a
  Kubernetes CronJob, or a goroutine with `timex` guarded by `lease`.
- **mockery mocks:** the service depends on a small `store.Store`
  interface; `memstore` is the fake.
- **App Check, guest validators:** write an `auth.Verifier` for those tokens
  and add it to `auth.Config`.
- **Mock-user header:** a header that impersonates a user is a bypass;
  use `AUTH_DISABLED=true` locally, or real test tokens.
- **Method blacklist:** use an `auth.Authorizer`.
- **PR template:** kept, without org-specific links.
