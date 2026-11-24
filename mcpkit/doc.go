// Package mcpkit is a thin layer over the official MCP Go SDK
// (github.com/modelcontextprotocol/go-sdk) for building Model Context
// Protocol servers.
//
// It adds only what every server in a fleet needs and the SDK leaves to the
// caller:
//
//   - typed tool registration ([AddTool]) that keeps the SDK's schema
//     inference, input validation, structured output and IsError results,
//     plus panic recovery and an optional per-tool timeout;
//   - a hardened Streamable HTTP [http.Handler] ([Server.Handler]):
//     stateless by default, request-body limit, browser cross-origin
//     protection with an explicit CORS allow-list, bearer-token auth and
//     RFC 9728 protected-resource metadata ([Server.MetadataHandler]);
//   - stdio serving ([Server.RunStdio]) where the upstream credential comes
//     from the environment ([EnvToken]) instead of the JSON-RPC payload;
//   - OpenTelemetry spans and the mcp.server.operation.duration metric,
//     following the MCP semantic conventions.
//
// mcpkit never owns an [http.Server]: pass [Server.Service] to kitchen's
// server package (server.WithServices), or mount [Server.Handler] on your own
// mux. Tokens from your own IdP are verified by kitchen's auth package
// through [Verifier]. Only the public messages of client-fault *errs.Error
// values from tools reach the model (see [AddTool]).
// Do not set http.Server.WriteTimeout: it would cut long-running streamed
// tool responses; rely on ReadHeaderTimeout, IdleTimeout and per-tool
// timeouts instead.
//
// For anything not wrapped here, use [Server.SDK] to reach the underlying
// *mcp.Server.
package mcpkit
