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
| `.WithLogger(xlog.LoggerInterface)` | `mcpkit.WithLogger(*slog.Logger)` |
| `.WithKeepAlive(d)` | `mcpkit.WithServerOptions(func(o *mcp.ServerOptions) { o.KeepAlive = d })` |
| `.WithServerOptions(fn)` | `mcpkit.WithServerOptions(fn)` |
| `.WithProtocolVersion(v)` | removed; the SDK negotiates 2026-07-28 and falls back to 2025-11-25 |
| `.EnableTools/Resources/Prompts[WithOptions]()` | removed; capabilities follow what you register |
| `.EnableLogging()` | removed (MCP logging is deprecated); use slog + OTel |
| `.WithStdioTransport()` + `Start/Serve/Wait` | `srv.RunStdio(ctx, mcpkit.EnvToken("UPSTREAM_TOKEN"))` |
| `.WithHTTPTransport(cfg)`, `WithHTTPPort/Address`, `WithTLS`, `Start/Stop` | mount `srv.Handler()` on your own `http.Server` (see §3) |
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
| Tool errors | Handler errors and timeouts became JSON-RPC errors (-32603) the model could not read | Returned errors and timeouts become `isError: true` results with the message; `*jsonrpc.Error` stays a protocol error |
| Tool timeout | One goroutine per call; leaked when the handler ignored ctx | `context.WithTimeoutCause`, no goroutine; error text `tool "x" timed out after d` |
| Panics | Recovered per kind; the panic text could reach the client | Recovered for every method by middleware; client sees "internal error", log gets value + stack |
| HTTP `WriteTimeout` | 60s default on mcpkit's own `http.Server` cut streamed responses and tools longer than 60s | mcpkit owns no server; the template sets no `WriteTimeout` and uses `MCP_TOOL_TIMEOUT` |
| Resource cache | Unbounded map keyed by URI only; served one user's per-token data to another | Removed |
| CORS | Empty origin list reflected any origin; OPTIONS answered 204 for disallowed origins | No CORS unless origins are listed; exact match; disallowed preflight → 403; stdlib cross-origin protection blocks cross-site writes |
| gzip | Never ran for spec clients (they send `Accept: text/event-stream`); wrote trailer bytes on 202/204 | Removed |
| Tool order | Registry iterated a map: `tools/list` order changed between restarts (hurt prompt caching) | Sorted by name |
| MCP logging | Enabled by default though deprecated | Not enabled; use slog/OTel |
| Auth | None built in; each server trusted any `Authorization` header and continued without a token | `WithAuth` requires a token: 401 + `WWW-Authenticate` (`resource_metadata`, scopes), 403 on missing scope, RFC 9728 metadata endpoint |
| JWT | – | `mcpkit/jwtauth`: asymmetric algs only, `exp` required, `aud` = resource URL, JWKS cache with rate-limited refresh |
| stdio token | Read from `params._meta.token` in each call (credential inside the JSON-RPC payload) | Read once from the environment (`TokenSource`) |
| Transport | Legacy HTTP+SSE offered | Removed; stdio + Streamable HTTP (stateless default) |
| Cancellation | Tool kept running after the HTTP client disconnected | `PropagateRequestCancellation` cancels the tool's ctx (2026-07-28 clients) |
| Telemetry | Custom request log | OTel span `{method} {target}` + `mcp.server.operation.duration` (MCP semconv) |

## 3. Serving over HTTP

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
the standard library:

```go
cfg := &oauth2.Config{ClientID: id, ClientSecret: secret, Endpoint: google.Endpoint, RedirectURL: cb, Scopes: scopes}
tok, err := cfg.Exchange(ctx, code)                 // authorization code -> token
ts := cfg.TokenSource(ctx, tok)                     // auto-refresh
client := &http.Client{
    Transport: otelhttp.NewTransport(&oauth2.Transport{Source: ts, Base: http.DefaultTransport}),
    Timeout:   30 * time.Second,
}
```

For retries, use `req.GetBody` (set automatically by `http.NewRequest` for
`bytes.Reader`/`strings.Reader` bodies) to rewind the body. Circuit breakers
and rate limiters were unused interfaces and are not replaced.

## 5. Generators

| Old | New |
|---|---|
| MCP scaffolder (interactive, `bufio`) | `kitchen new mcp -module <path> [-yes]` (huh prompts on a TTY) |
| gRPC service scaffolder | `kitchen new service`: coming after the transport, observability, errors, data and messaging packages ship |
| `gen` (add CRUD model) | removed |

Install: `go install github.com/dungnh3/kitchen/cmd/kitchen@latest`.

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
