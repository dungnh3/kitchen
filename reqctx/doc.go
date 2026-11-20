// Package reqctx carries per-request client information through a
// context.Context.
//
// A server (see package server) builds an [Info] once per request from the
// incoming gRPC metadata or HTTP headers and stores it with [NewContext].
// Handlers read it with [FromContext]; they never re-parse metadata.
//
// The authenticated user id is not part of Info: only an authentication
// interceptor may set it, with [WithUserID], after it has verified a
// credential. Client-sent identity headers (see [IdentityHeaders]) are
// stripped by the server unless the caller is trusted.
//
// Outgoing gRPC calls propagate an explicit allowlist of values (request id,
// user id, client info) with [UnaryClientInterceptor] and
// [StreamClientInterceptor], or manually with [AppendToOutgoing].
package reqctx
