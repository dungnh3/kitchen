# context (xcontext)

Package `context` carries per-request client information through a
`context.Context`. That information includes the request id, the real client
IP, user agent, language, platform, app version, device id, time zone, and
allowlisted headers. It also carries the authenticated user id. It is the
canonical package for request metadata: see D7 in
[docs/decisions.md](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22).
Handlers read this information here and never parse headers themselves.

Import it as `xcontext`, so that the standard `context` package stays
available:

```go
import xcontext "github.com/dungnh3/kitchen/context"
```

## Install

```sh
go get github.com/dungnh3/kitchen
```

## Quick start

`kitchen/server` builds an `Info` once per request, for gRPC, the REST
gateway and plain HTTP handlers alike. Handlers only read it:

```go
// A server stores Info once per request; handlers only read it.
ctx := xcontext.NewContext(context.Background(), xcontext.Info{RequestID: "r-1", Platform: xcontext.PlatformIOS})
ctx = xcontext.WithUserID(ctx, "user-42") // done by the auth interceptor

info, _ := xcontext.FromContext(ctx)
fmt.Println(info.RequestID, info.Platform, xcontext.UserID(ctx))
// r-1 ios user-42
```

In a real handler you skip the first two lines: `ctx` already carries the
`Info`.

The defaults are safe:

- **Client IP.** `ClientIP` is the connection address. No proxy is trusted,
  so a client cannot set its IP with `X-Forwarded-For`.
- **Request id.** `RequestID` is the client's `X-Request-Id` when it is
  valid: 1 to 128 printable ASCII characters, no spaces. Otherwise the
  server generates one.
- **Credentials.** `Authorization`, `Proxy-Authorization`, `Cookie` and
  `Set-Cookie` are never captured or returned.
- **Identity headers.** `User-Id` and `X-Kitchen-Client-Ip` are stripped from
  untrusted callers. `UserID` is set only by authentication.
- **Size.** Every captured value is capped at 4 KiB.

## Configuration

You configure this package through `server` options (or through
`xcontext.Options` when you build an `Info` yourself):

| Option | Default | Change it when |
|---|---|---|
| `server.WithTrustedProxies(p)` | none: the connection address is the client | the service is behind a load balancer, CDN or ingress |
| `server.WithForwardHeaders(h...)` | `DefaultForwardHeaders()` | handlers need another header (a tenant id, for example) |
| `server.WithTrustedIdentityPeers(p)` | none: identity metadata is stripped | internal services call you with a user id |
| `NewTrustedProxiesHeader(header, ...)` | `X-Forwarded-For` | your proxies append to `Forwarded` (RFC 7239) |

`DefaultForwardHeaders()` returns `X-Request-Id`, `User-Id`, `X-Device-Id`,
`X-Platform`, `X-App-Version`, `X-Timezone` and `X-Locale`. The gateway
forwards `User-Agent` and `Accept-Language` on its own.

## Integration with kitchen

**Server.** Pass the trusted proxies and any extra headers once:

```go
// The server builds Info for every request. Trust only the hops that
// append to X-Forwarded-For: here a GCP load balancer and the cluster.
proxies, err := xcontext.NewTrustedProxies("35.191.0.0/16", "130.211.0.0/22", "10.0.0.0/8")
if err != nil {
	return
}
srv, err := server.New(
	server.WithTrustedProxies(proxies),
	server.WithForwardHeaders("X-Tenant-Id"), // also captured in Info.Headers
)
```

See [server/USAGE.md](../server/USAGE.md) for the full service wiring.

**Logs.** `NewContext` stores `request_id`, and `WithUserID` stores
`user_id`, as `kitchen/log` context attributes. Every record logged with the
request context carries them, with no logger setup:

```go
ctx := xcontext.NewContext(context.Background(), xcontext.Info{RequestID: "r-1"})
ctx = xcontext.WithUserID(ctx, "u-42")
logger.InfoContext(ctx, "order placed", log.String("order_id", "o1"))
// {"message":"order placed","level":"INFO","order_id":"o1","request_id":"r-1","user_id":"u-42"}
```

**Auth.** The `auth` interceptor calls `xcontext.WithUserID` after it
verifies a token. Read the user with `xcontext.UserID(ctx)`. An empty
string means the request is not authenticated. Never call `WithUserID`
with a value taken from a header.

**Rate limiting.** The `ratelimit` identities `ByIP` and `ByAnyIP` read
`Info.ClientIP`. Their keys are only as good as your trusted-proxy list.
Without it, every request behind a load balancer shares the load balancer's
IP. See [ratelimit/USAGE.md](../ratelimit/USAGE.md).

**Errors and i18n.** `i18n` picks the request language from
`Info.AcceptLanguage`. `errs` uses it to localize error messages
(`server.WithI18n`). See [i18n/USAGE.md](../i18n/USAGE.md).

**Outgoing gRPC.** Propagate the request to other kitchen services with the
client interceptors. Combine them with `otelx` for trace propagation:

```go
// Calls to other kitchen services carry the request id, user id and
// client info; the callee accepts the user id only from trusted peers
// (server.WithTrustedIdentityPeers).
conn, err := grpc.NewClient("orders:9090",
	grpc.WithTransportCredentials(insecure.NewCredentials()),
	grpc.WithStatsHandler(otelx.GRPCClientHandler()),
	grpc.WithChainUnaryInterceptor(xcontext.UnaryClientInterceptor()),
	grpc.WithChainStreamInterceptor(xcontext.StreamClientInterceptor()),
)
```

Only an explicit allowlist is sent: request id, user id, client IP,
Accept-Language, platform, app version, device id and time zone. Incoming
`authorization` is never copied. If the caller already set a value, that
value wins.

## Advanced

### The real client IP behind proxies

`TrustedProxies` walks `X-Forwarded-For` from the right and skips trusted
hops. The first untrusted address is the client:

```go
lb, _ := xcontext.NewTrustedProxies("10.0.0.0/8")
remote := addr("10.1.2.3")
fmt.Println(lb.ClientIP(remote, []string{"203.0.113.7, 10.9.9.9"}))
// 203.0.113.7
```

**Security warning.** Never trust `X-Forwarded-For` from a peer you did not
list. A direct client can write anything into it, and then it can choose its
own rate-limit key and audit IP. That is why the default trusts no proxy.

List every hop between the client and the server. Otherwise the client IP is
the first unlisted hop. Behind Cloudflare and a GCP external Application Load
Balancer, list:

- the Google Front End ranges (35.191.0.0/16, 130.211.0.0/22);
- the load balancer's own IP;
- the Cloudflare ranges;
- any in-cluster ingress.

`CF-Connecting-IP` is deliberately ignored, because a client that reaches the
load balancer directly can set it.

### Building Info outside the server

A plain `net/http` service, or a test, can build the same `Info`:

```go
proxies, _ := xcontext.NewTrustedProxies("10.0.0.0/8")
r := httptest.NewRequest(http.MethodGet, "/v1/orders", nil)
r.RemoteAddr = "10.0.0.5:51000" // the load balancer
r.Header.Set("X-Forwarded-For", "203.0.113.7")
r.Header.Set("X-Request-Id", "r-9")
r.Header.Set("X-Tenant-Id", "acme")
r.Header.Set("User-Agent", "MyApp/2.1 (iPhone; iOS 18.0)")

info := xcontext.FromHTTP(r, xcontext.Options{TrustedProxies: proxies, Headers: []string{"X-Tenant-Id"}})
ctx := xcontext.NewContext(r.Context(), info)

fmt.Println(info.ClientIP, info.RequestID, info.Platform, xcontext.Header(ctx, "X-Tenant-Id"))
// 203.0.113.7 r-9 ios acme
```

`FromMetadata` does the same for gRPC metadata. It leaves `ClientIP` unset,
because the server resolves the IP from the peer address.

### Reading headers safely

`xcontext.Header` reads a captured header. When no `Info` is stored, it
falls back to incoming gRPC metadata. It never returns credentials or
identity headers:

```go
// Credentials and identity headers are never returned, even when sent.
ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
	"x-tenant-id", "acme", "authorization", "Bearer secret", "user-id", "admin"))
fmt.Printf("%q %q %q\n", xcontext.Header(ctx, "X-Tenant-Id"), xcontext.Header(ctx, "Authorization"), xcontext.Header(ctx, "User-Id"))
// "acme" "" ""
```

Read the bearer token through `auth`, never through this package.

### Service-to-service identity

A gateway or BFF that authenticates users can pass the user id to internal
services. The internal service must list its callers in
`server.WithTrustedIdentityPeers`. From any other peer, `User-Id` is removed
before any interceptor runs, and a warning is logged at most once a minute.
`server.WithTrustAllIdentityHeaders()` accepts identity metadata from every
peer.

**Security warning.** Use `WithTrustAllIdentityHeaders` only when no
untrusted client can reach either port. Otherwise any caller can claim to be
any user.

## Testing

Store an `Info` with `NewContext`, and a user with `WithUserID`, then call the
handler directly. No server is needed. To check outgoing propagation, read
the outgoing metadata:

```go
ctx := xcontext.NewContext(context.Background(), xcontext.Info{RequestID: "r-1", DeviceID: "d-1"})
ctx = xcontext.WithUserID(ctx, "u-42")

ctx = xcontext.AppendToOutgoing(ctx)
md, _ := metadata.FromOutgoingContext(ctx)
fmt.Println(md.Get("x-request-id"), md.Get("user-id"), md.Get("x-device-id"))
// [r-1] [u-42] [d-1]
```

## Observability

- Log attributes: `request_id` (`LogKeyRequestID`) and `user_id`
  (`LogKeyUserID`) on every record logged with the request context.
- The server echoes `X-Request-Id` on HTTP responses, and its access log uses
  the same id.
- This package emits no spans or metrics of its own.

## Production checklist

- Set `server.WithTrustedProxies` to every hop in front of the service.
  Check that `Info.ClientIP` is the real client once deployed.
- Use the header your proxies actually append to. A header they pass through
  untouched is client-controlled.
- Set `server.WithTrustedIdentityPeers` on internal services. Avoid
  `WithTrustAllIdentityHeaders`.
- Add extra headers with `WithForwardHeaders`. Read them with
  `xcontext.Header`, never with raw metadata.
- Add the client interceptors to every gRPC client that calls kitchen
  services.

## Migrating from the old kitchen

The old `kitchen/context` helpers map to `FromContext`, `Header`, `UserID` and
`WithUserID`. The old pass-through header decider is now an allowlist. See
[docs/migration/transport.md](../docs/migration/transport.md).
