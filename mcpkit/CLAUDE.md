# mcpkit: package rules

`mcpkit` is a thin layer over the official MCP Go SDK. It adds only what
every MCP server in the fleet needs. When the SDK already does something,
use the SDK; reach the rest through `Server.SDK`. Read `doc.go` and
`docs/design/codegen-mcp.md` first.

- **stdio servers.** In stdio mode, stdout carries JSON-RPC, so nothing else
  may write to it.
  - Logs go to stderr: `log.Setup` with `LOG_OUTPUT=stderr`.
  - The upstream credential comes from the environment (`EnvToken`), never
    from the JSON-RPC payload.
- **Auth.**
  - Bearer tokens are verified by kitchen `auth` through `Verifier`. Do not
    add a JWT library here; D7 lint enforces this.
  - An unreachable authority maps to `errs` UNAVAILABLE, which is HTTP 503.
  - Token and verifier errors are never echoed back to the caller.
- **The HTTP handler is hardened. Keep these defaults:**
  - stateless by default;
  - a request body limit;
  - cross-origin protection with an explicit CORS allow-list;
  - RFC 9728 protected-resource metadata.
- **The HTTP server is not owned here.** `mcpkit` never creates an
  `http.Server`. Host it with `server.WithServices`, using
  `server.WithGRPCAddr("")` for HTTP-only. Never set `WriteTimeout`: it cuts
  long-running streamed tool responses.
- **Tools.**
  - Only `*errs.Error` messages reach the model. Any other error is
    sanitised.
  - Tools keep panic recovery and the optional per-tool timeout.
  - Spans and metrics follow the MCP semantic conventions, through `otelx`.
- **The scaffold depends on this package.** `cmd/kitchen` generates MCP
  servers on top of `mcpkit`. After an API change, regenerate the scaffold's
  golden files there.
