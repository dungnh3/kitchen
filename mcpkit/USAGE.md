# mcpkit

`mcpkit` builds Model Context Protocol servers on the official MCP Go SDK. It
adds typed tools with safe errors, panic recovery and timeouts, a hardened
Streamable HTTP handler, bearer auth through kitchen `auth`, stdio serving
and MCP telemetry. It is the canonical MCP package ([D7](../docs/decisions.md)).
`kitchen new mcp` generates a project wired like the snippets below
([cmd/kitchen](../cmd/kitchen/USAGE.md)).

## Install

```bash
go get github.com/dungnh3/kitchen/mcpkit github.com/modelcontextprotocol/go-sdk@v1.8.0
```

`mcpkit` is its own module (with `mcpkittest`). It requires core and `auth`.

## Quick start

One typed tool, served over Streamable HTTP on kitchen's `server`, HTTP only:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

opts, _ := log.OptionsFromEnv() // LOG_LEVEL, LOG_FORMAT, LOG_OUTPUT, ...
logger, _ := log.Setup(opts)

srv, err := mcpkit.New(&mcp.Implementation{Name: "greeter", Version: "v1.0.0"},
	mcpkit.WithLogger(logger))
if err != nil {
	logger.ErrorContext(ctx, "mcp server", log.Err(err))
	return
}

type GreetIn struct {
	Name string `json:"name" jsonschema:"the name of the person to greet"`
}
type GreetOut struct {
	Greeting string `json:"greeting"`
}
mcpkit.AddTool(srv, &mcp.Tool{Name: "greet", Description: "Greet someone by name."},
	func(_ context.Context, _ *mcp.CallToolRequest, in GreetIn) (*mcp.CallToolResult, GreetOut, error) {
		return nil, GreetOut{Greeting: "Hello, " + in.Name + "!"}, nil
	})

hs, err := server.New(
	server.WithServices(srv.Service("/mcp")),
	server.WithGRPCAddr(""), // HTTP only: no public gRPC port
	server.WithLogger(logger),
)
if err != nil {
	logger.ErrorContext(ctx, "server", log.Err(err))
	return
}
if err := hs.Run(ctx); err != nil {
	logger.ErrorContext(ctx, "server stopped", log.Err(err))
}
```

The defaults:

- **Schemas from structs** (`json` and `jsonschema` tags). Arguments are
  validated; `Out` is structured content. `AddTool` panics on a bad tool.
- **Safe errors.** Only client-fault `errs` messages reach the model.
- **Stateless HTTP**, so any replica serves any request; a 4 MiB body limit;
  no cross-origin browser access.
- **No auth and no tool deadline.** Set `WithAuth` and `WithToolTimeout`
  before you expose the server.

## Configuration

Options of `mcpkit.New` (full API: `go doc github.com/dungnh3/kitchen/mcpkit`).
`mcpkit` reads no environment variables; `EnvToken` reads the one you name.

| Option | Default | When to change it |
|---|---|---|
| `WithLogger(l)` | `log.Default()` | Always pass the `log.Setup` logger. The SDK uses it too. |
| `WithErrors(h)` | `errs.NewHandler` with the logger | Pass the handler you give `server.WithErrors`. |
| `WithInstructions(s)` | none | Tell clients how to use the server. |
| `WithToolTimeout(d)` | none | Production. Per tool: `mcpkit.Timeout(d)`. |
| `WithAuth(Auth{...})` | no auth | Any shared server. HTTP only. |
| `WithAllowedOrigins(o...)` | none | Browser clients on another origin. |
| `WithMaxRequestBodyBytes(n)` | 4 MiB | Tools with large arguments. |
| `WithStateful(idle)` | stateless | Sessions (see Advanced). |
| `WithTracerProvider`, `WithMeterProvider` | OTel globals | Pass the `otelx/sdk` providers. |
| `WithMiddleware(m...)` | none | Logic on every MCP request. |
| `WithServerOptions(fn)` | none | SDK options not wrapped (`KeepAlive`, `CompletionHandler`). |

## Integration with kitchen

### Tool errors and errs

A returned error becomes a tool result with `isError` set, so the model can
recover. To keep internals away from clients, the model sees the public
message of a client-fault `*errs.Error`. `Internal`, `Unknown` and
`DataLoss` errs and any other error show as `"internal error"` and are
logged with the tool name. Return a `*jsonrpc.Error` only for protocol
failures.

```go
srv, err := mcpkit.New(&mcp.Implementation{Name: "calendar"})
if err != nil {
	panic(err)
}
type In struct {
	Date string `json:"date" jsonschema:"day to book, YYYY-MM-DD"`
}
book := func(context.Context, time.Time) error { return errors.New("pq: connection refused") }

mcpkit.AddTool(srv, &mcp.Tool{Name: "book_day"},
	func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
		day, err := time.Parse(time.DateOnly, in.Date)
		if err != nil {
			// The model reads this message and can retry with a fixed argument.
			return nil, nil, errs.New(codes.InvalidArgument, "BAD_DATE",
				errs.Message("date must be YYYY-MM-DD"))
		}
		if err := book(ctx, day); err != nil {
			// Logged with the tool name; the model sees "internal error".
			return nil, nil, fmt.Errorf("book %s: %w", in.Date, err)
		}
		return nil, map[string]string{"booked": in.Date}, nil
	})
```

### Tool timeouts

`WithToolTimeout` puts a deadline on every tool's context. `mcpkit.Timeout`
overrides it for one tool; `Timeout(0)` removes it. It only works if the
handler honours `ctx`. When it fires, the model sees
`tool "build_report" timed out after 5m0s`.

```go
srv, err := mcpkit.New(&mcp.Implementation{Name: "reports"},
	mcpkit.WithToolTimeout(30*time.Second)) // default for every tool
if err != nil {
	panic(err)
}
buildReport := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() } // slow work

mcpkit.AddTool(srv, &mcp.Tool{Name: "build_report"},
	func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		return nil, nil, buildReport(ctx) // must honour ctx
	},
	mcpkit.Timeout(5*time.Minute)) // this tool may take longer
```

### Server wiring

`srv.Service(pattern)` is a `server.Service` that mounts `srv.Handler()` and
the auth metadata. kitchen's `server` owns the listener, timeouts, drain,
probes, access logs and HTTP telemetry; `server.WithGRPCAddr("")` drops the
unused gRPC port. Full wiring: [server](../server/USAGE.md).

Never set a `WriteTimeout`: it cuts streamed tool responses. Without
kitchen's `server`, mount `srv.Handler()` and `srv.MetadataHandler()` on
your own mux (`ExampleServer_Handler`).

### Auth with your own IdP

`mcpkit.Verifier` adapts a kitchen `auth.Verifier` (`auth.NewJWKS`,
`auth.NewStatic`, `auth.NewIntrospection`, `auth.All`). The audience is this
server's resource URL. It is required, and every token must list it in
`aud`, so tokens issued for other APIs are rejected. Tools read the caller
with `auth.FromContext`.

```go
ctx := context.Background()
logger := log.Default()

jwks, err := kauth.NewJWKS(ctx, kauth.JWKSConfig{
	Issuer: "https://idp.example.com",
	URL:    "https://idp.example.com/.well-known/jwks.json",
	Types:  []string{"at+jwt"},
})
if err != nil {
	logger.ErrorContext(ctx, "jwks", log.Err(err))
	return
}
defer func() { _ = jwks.Close(ctx) }()

v, err := mcpkit.Verifier(jwks, "https://mcp.example.com/mcp") // aud must name this server
if err != nil {
	logger.ErrorContext(ctx, "verifier", log.Err(err))
	return
}

eh := errs.NewHandler(errs.Config{Logger: logger, Domain: "mcp.example.com"})
srv, err := mcpkit.New(&mcp.Implementation{Name: "agent-tools"},
	mcpkit.WithLogger(logger),
	mcpkit.WithErrors(eh),
	mcpkit.WithAuth(mcpkit.Auth{
		Verifier: v,
		Scopes:   []string{"tools.call"},
		Metadata: &oauthex.ProtectedResourceMetadata{
			Resource:             "https://mcp.example.com/mcp",
			AuthorizationServers: []string{"https://idp.example.com"},
			ScopesSupported:      []string{"tools.call"},
		},
	}))
if err != nil {
	logger.ErrorContext(ctx, "mcp server", log.Err(err))
	return
}

mcpkit.AddTool(srv, &mcp.Tool{Name: "whoami"},
	func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		p, ok := kauth.FromContext(ctx) // set by mcpkit.Verifier
		if !ok {
			return nil, nil, errs.ErrTokenMissing
		}
		return nil, map[string]string{"subject": p.Subject}, nil
	})

hs, err := server.New(server.WithServices(srv.Service("/mcp")), server.WithGRPCAddr(""),
	server.WithErrors(eh), server.WithLogger(logger)) // same errs handler
if err == nil {
	err = hs.Run(ctx)
}
if err != nil {
	logger.ErrorContext(ctx, "server", log.Err(err))
}
```

- **Missing or rejected token:** 401 with a `WWW-Authenticate` header that
  points at the metadata; the reason is never sent. **Missing scope:** 403.
- **IdP unreachable** (`auth.ErrAuthUnavailable`): fails closed with the errs
  UNAVAILABLE response, HTTP 503, written and logged by the `WithErrors`
  handler. Share it with `server.WithErrors` so every route answers alike.

`Auth.Metadata` is the RFC 9728 document that lets clients find the
authorization server. Its `Resource` is the endpoint's absolute URL. It is
served at `srv.MetadataPath()`: `/.well-known/oauth-protected-resource/mcp`.

`mcpkit` authenticates its own endpoint. If the kitchen server also has an
`auth.Authenticator`, list `srv.Service("/mcp").Patterns()` in its `Public`
config, or drop `mcpkit.WithAuth` and let the server authenticate.

## Advanced

### stdio servers

Use stdio when a client launches the server as a subprocess. stdout carries
JSON-RPC, so **nothing else may write to it**. Send logs to stderr with
`Output` in `log.Setup`, or `LOG_OUTPUT=stderr`, which also covers logs
written before `Setup`. The upstream credential comes from the environment,
never from the JSON-RPC payload.

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

opts, _ := log.OptionsFromEnv() // LOG_OUTPUT=stderr also works
if opts.Output == nil {
	opts.Output = os.Stderr // never stdout in stdio mode
}
logger, _ := log.Setup(opts)

srv, err := mcpkit.New(&mcp.Implementation{Name: "slack"}, mcpkit.WithLogger(logger))
if err != nil {
	logger.ErrorContext(ctx, "mcp server", log.Err(err))
	return
}
// The token is read once at start and returned by mcpkit.TokenFrom in tools.
if err := srv.RunStdio(ctx, mcpkit.EnvToken("SLACK_TOKEN")); err != nil {
	logger.ErrorContext(ctx, "stdio", log.Err(err))
}
```

`WithAuth` does not apply here. `srv.Run` serves any other `mcp.Transport`.

### Forwarding an upstream token

When clients send an upstream API's OAuth token (Google, Slack, Atlassian),
use `PassThrough` with `AllowMissingExpiration: true`. It accepts any
non-empty token and checks nothing, so never use it for tokens your own
tools trust; the upstream rejects bad ones. Tools forward the raw token from
`mcpkit.TokenFrom` (on stdio: the `TokenSource` value).

```go
srv, err := mcpkit.New(&mcp.Implementation{Name: "mail"},
	mcpkit.WithAuth(mcpkit.Auth{
		Verifier:               mcpkit.PassThrough(),
		AllowMissingExpiration: true,
		Metadata: &oauthex.ProtectedResourceMetadata{
			Resource:             "https://mcp.example.com/mcp",
			AuthorizationServers: []string{"https://accounts.google.com"},
		},
	}))
if err != nil {
	panic(err)
}
listMail := func(context.Context, string) ([]string, error) { return []string{"m1"}, nil } // Gmail API via httpx

mcpkit.AddTool(srv, &mcp.Tool{Name: "list_mail"},
	func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		ids, err := listMail(ctx, mcpkit.TokenFrom(ctx)) // the caller's Google token
		return nil, map[string][]string{"ids": ids}, err
	})

fmt.Println(srv.Service("/mcp").Patterns())
```

### Browser clients, large payloads and sessions

Set on `New` (`ExampleNew_http`):

- `WithAllowedOrigins` takes exact `scheme://host[:port]` origins. They get
  CORS headers and pass cross-origin protection. Any listed site can call
  your tools from the user's browser, so keep the list short.
- `WithMaxRequestBodyBytes` raises or lowers the 4 MiB limit (413 above it).
- `WithStateful` adds `Mcp-Session-Id`, the GET notification stream and
  server-to-client requests for pre-2026-07-28 clients. Sessions live in one
  process, so the load balancer must route by session.

### Middleware and SDK features

`WithMiddleware` adds SDK receiving middleware, inside `mcpkit`'s telemetry
and panic recovery (`ExampleWithMiddleware`). `srv.SDK()` returns the
`*mcp.Server` for what `mcpkit` does not wrap: resources, prompts,
completions, notifications (`ExampleServer_SDK`). Telemetry and recovery
cover them too.

## Testing

`mcpkittest.Connect` serves the server in memory and returns a connected
client, shut down when the test ends.

```go
type In struct {
	Name string `json:"name"`
}
greet := func(_ context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
	if in.Name == "" {
		return nil, nil, errs.ErrInvalidArgument.With(errs.Message("name must not be empty"))
	}
	return nil, map[string]string{"greeting": "Hello, " + in.Name}, nil
}

testGreet := func(t *testing.T) {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "test"}, mcpkit.WithLogger(log.Discard()))
	if err != nil {
		t.Fatal(err)
	}
	mcpkit.AddTool(srv, &mcp.Tool{Name: "greet"}, greet)
	cs := mcpkittest.Connect(t, srv, "") // a token here is seen by mcpkit.TokenFrom

	res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "greet", Arguments: map[string]any{"name": ""}})
	if err != nil || !res.IsError {
		t.Fatalf("want a tool error, got %v, %v", res, err)
	}
	if msg := res.Content[0].(*mcp.TextContent).Text; msg != "name must not be empty" {
		t.Errorf("message = %q", msg)
	}
}
_ = testGreet // in a _test.go file: func TestGreet(t *testing.T) { ... }
```

A tool error is a result with `IsError` set, not an error from `CallTool`;
structured output is in `res.StructuredContent`. Test HTTP concerns (auth,
CORS, body limit) with `httptest` on `srv.Handler()`.

## Observability

Every request gets one SERVER span named `{mcp.method.name} {target}` (for
example `tools/call greet`) and one `mcp.server.operation.duration` sample,
in seconds, per the MCP semantic conventions (semconv v1.41). Pass the
`otelx/sdk` providers to `mcpkit` and `server`, as the generated
[main.go](../cmd/kitchen/templates/mcp/cmd/server/main.go.tmpl) does.

| Attribute | When |
|---|---|
| `mcp.method.name` | always |
| `gen_ai.tool.name`, `gen_ai.operation.name=execute_tool` | `tools/call` |
| `gen_ai.prompt.name` | `prompts/get` |
| `mcp.resource.uri` | `resources/read` |
| `mcp.session.id` | stateful sessions |
| `error.type` | `tool_error` (an `isError` result), the JSON-RPC code, or `_OTHER` |
| `rpc.response.status_code` | JSON-RPC errors |

Logs: `mcpkit: tool failed` (`tool`, `error`) and `mcpkit: handler panicked`
(`method`, `panic`, `stack`).

## Production checklist

- `WithAuth` with `mcpkit.Verifier` and `Auth.Metadata`.
- One `errs.Handler` for `mcpkit.WithErrors` and `server.WithErrors`.
- `WithToolTimeout`, and handlers that honour `ctx`.
- `errs` errors for what the model should read, with no secrets in them.
- kitchen's `server` with `server.WithGRPCAddr("")`; no `WriteTimeout`.
- Stateless unless you need sessions; only trusted origins allowed.
- stdio: logs to stderr, credentials from the environment.

## Migrating from the old kitchen

The builder, the owned `http.Server` and SSE are gone: use `New`, `AddTool`
and kitchen's `server`. Symbol map:
[docs/migration/codegen-mcp.md](../docs/migration/codegen-mcp.md).
