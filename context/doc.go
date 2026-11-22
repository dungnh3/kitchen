// Package context carries per-request client information through a
// context.Context. Import it as xcontext to keep the standard library's
// context package available:
//
//	import xcontext "github.com/dungnh3/kitchen/context"
//
// A server (see package server) builds an [Info] once per request from the
// HTTP headers or the incoming gRPC metadata and stores it with
// [NewContext]. Handlers read it with [FromContext] or [Header]; they never
// re-parse headers, and the answer is the same for gRPC, the REST gateway and
// plain HTTP handlers.
//
// Info holds a few generic fields (request id, client IP, user agent,
// language, platform, app version, device id, time zone) and the values of
// the allowlisted headers ([DefaultForwardHeaders] plus the ones the
// application adds). Credentials (Authorization, cookies) are never
// captured; authentication belongs to package auth.
//
// The authenticated user id is not part of Info: only an authentication step
// or a trusted peer sets it, with [WithUserID]. Client-sent identity headers
// ([IdentityHeaders]) are stripped by the server unless the caller is
// trusted.
//
// Outgoing gRPC calls propagate an explicit allowlist (request id, user id,
// client info) with [UnaryClientInterceptor] and [StreamClientInterceptor],
// or manually with [AppendToOutgoing].
package context
