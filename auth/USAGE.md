# auth

Bearer-token authentication for gRPC, REST and plain HTTP, failing closed:

- every token is verified (signature, `exp`, `nbf`, `iss`, `aud` and an
  algorithm allow-list) by the verifier registered for its issuer;
- anything that cannot be verified is rejected;
- every method requires a token unless you list it as public.

It is the canonical auth and JWT package (D7 in
[docs/decisions.md](../docs/decisions.md)); do not parse JWTs with a library
directly.

## Install

```sh
go get github.com/dungnh3/kitchen/auth
```

`auth` is a submodule; it builds on `kitchen/server`, `errs`, `cache`,
`httpx` and `log` from the core module.

## Quick start

One issuer with a shared HMAC secret, a public health service and one
scope-protected method (from `Example`):

```go
static, err := auth.NewStatic(auth.StaticConfig{
	Issuer:   "https://issuer-a.example.com",
	Audience: "api.example.com",
	Keys:     []auth.Key{{ID: "2026-11", Alg: jwa.HS256(), Material: []byte("0123456789abcdef0123456789abcdef")}},
})
if err != nil {
	panic(err)
}
authn, err := auth.New(auth.Config{
	Verifiers: []auth.Verifier{static},
	Public:    []string{"/grpc.health.v1.Health/"},
	Authorizer: auth.RequireScopes(map[string][]string{
		"/agent.v1.AgentService/Delete": {"agents:admin"},
	}),
})
if err != nil {
	panic(err)
}

// An unsigned ("alg":"none") token is rejected, never passed through.
_, err = authn.Authenticate(ctx, "eyJhbGciOiJub25lIn0.eyJpc3MiOiJodHRwczovL2lzc3Vlci1hLmV4YW1wbGUuY29tIn0.")
fmt.Println(errors.Is(err, auth.ErrTokenInvalid), status.Code(err))
// Output: true Unauthenticated
```

The safe defaults:

- **Deny by default.** Only methods in `Config.Public` skip
  authentication.
- **One verifier per issuer.** A JWT whose `iss` has no verifier is
  rejected; there is no pass-through and no "trust upstream" mode.
- **Strict keys.**
  - Each key accepts exactly one algorithm, and the key type must match it.
    An HMAC algorithm can never be used with a public key.
  - HMAC secrets must be at least 32 bytes.
  - `alg: none` is always rejected.
- **Bounded input.** Tokens longer than 8192 bytes are rejected before
  decoding. The clock leeway is 30s.
- **Streams end with their token.** A gRPC stream is cancelled at the
  token's expiry and ends with `TOKEN_EXPIRED`.
- **Outages fail closed.** When a key set or introspection endpoint is
  unreachable, requests are rejected with `AUTH_UNAVAILABLE` (503), never
  let through.
- **No leaks.** Why a token was rejected is logged at Debug level and never
  returned to the client.

## Configuration

| `auth.Config` field | Default | Change it when |
|---|---|---|
| `Verifiers` | none | always: one per JWT issuer (`NewStatic`, `NewJWKS`, `NewIntrospection` with `Issuer`) |
| `Opaque` | nil (opaque tokens rejected) | your IdP issues opaque tokens |
| `Audience` | empty | every principal must carry this audience, whatever verifier produced it |
| `Public` | none | health checks, login, public pages |
| `Optional` | none | pages that personalise when signed in |
| `Authorizer` | none | per-method scopes (`RequireScopes`) or your own `AuthorizerFunc` |
| `MaxTokenBytes` | 8192 | your IdP issues very large tokens |
| `StreamOutlivesToken` | false | see the warning below |
| `WriteError` | `errs.WriteHTTP` | pass your `errs.Handler`'s `WriteHTTP` to localize |
| `Logger` | `log.Default()` | pass the service logger |
| `MeterProvider` | global | tests |

Entries in `Public` and `Optional` are an exact gRPC method
(`/pkg.Svc/Method`) or HTTP route pattern (`GET /healthz`), or a service
prefix ending in `/`. A method may not be in both lists.

> **Warning.** `StreamOutlivesToken: true` lets a stream run after its token
> has expired or been revoked. Keep the default unless a stream must survive
> token refresh and you re-check the caller another way.

## Integration with kitchen

- **Server.** `server.WithAuth(authn)` covers gRPC, REST (grpc-gateway) and
  plain HTTP routes on one server, after recovery and before validation and
  rate limiting. See [server/USAGE.md](../server/USAGE.md) for the full
  service wiring.
- **Identity.** On success the principal is in the context
  (`auth.FromContext`), and its subject is stored with
  `xcontext.WithUserID`, so access logs and `kitchen/log` records carry
  `user_id`.
- **Errors.** Rejections are `kitchen/errs` sentinels, rendered as AIP-193
  statuses by the server's `errs.Handler`:
  - `TOKEN_MISSING`, `TOKEN_INVALID` and `TOKEN_EXPIRED` → 401;
  - `FORBIDDEN` and `INSUFFICIENT_SCOPE` → 403, the latter with
    `missing_scope` metadata;
  - `AUTH_UNAVAILABLE` → 503.

  The HTTP response carries an RFC 6750 `WWW-Authenticate` header.
- **HTTP clients.** JWKS and RFC 7662 calls go through `kitchen/httpx`, so
  they are traced and metered and their bodies are bounded. Pass your own
  client with `JWKSConfig.Client` / `RFC7662Config.Client`.
- **Redis.** Introspection results are shared across instances through
  `kitchen/cache` on a client from `redisx.Open`.
- **mcpkit.** An MCP server verifies tokens with the same verifiers (from
  `mcpkit.ExampleVerifier`):

  ```go
  v, err := mcpkit.Verifier(jwks, "https://mcp.example.com/mcp") // aud must name this server
  if err != nil {
  	log.Fatal(err)
  }
  ```

  See [mcpkit/USAGE.md](../mcpkit/USAGE.md).

## Advanced

### OIDC providers (JWKS)

Use it when an identity provider publishes its keys. They are fetched once
at start (failure aborts start-up), refreshed in the background (honouring
`Cache-Control`, between 5m and 1h) and refetched, rate limited, when an
unknown key ID shows up after a rotation. From `ExampleNewJWKS`:

```go
jwks, err := auth.NewJWKS(ctx, auth.JWKSConfig{
	Issuer:   "https://idp.example.com",
	Audience: "orders-api",
	URL:      "https://idp.example.com/.well-known/jwks.json",
	Types:    []string{"at+jwt"}, // reject ID tokens used as access tokens
})
if err != nil {
	logger.ErrorContext(ctx, "jwks", log.Err(err)) // fails if the first fetch fails
	return
}
defer func() { _ = jwks.Close(ctx) }()

authn, err := auth.New(auth.Config{
	Verifiers: []auth.Verifier{jwks},
	Public:    []string{"/grpc.health.v1.Health/", "GET /healthz"},
	Logger:    logger,
})
if err != nil {
	logger.ErrorContext(ctx, "auth", log.Err(err))
	return
}
srv, err := server.New(server.WithAuth(authn), server.WithLogger(logger))
```

- JWKS only accepts asymmetric algorithms (RS256, ES256 and EdDSA by
  default); HS* is refused.
- Set `Types` to the provider's access-token type (RFC 9068 `at+jwt`)
  whenever it sends one. Without it, an ID token from the same issuer is
  accepted as an access token.

### Opaque tokens and revocation (introspection)

Use it when tokens are opaque, or when revocation must take effect within
seconds. From `ExampleNewIntrospection`:

```go
in, err := auth.NewIntrospection(auth.IntrospectionConfig{
	Introspect: auth.RFC7662(auth.RFC7662Config{
		Endpoint:     "https://idp.example.com/oauth2/introspect",
		ClientID:     "orders-api",
		ClientSecret: os.Getenv("INTROSPECTION_CLIENT_SECRET"),
	}),
	Redis:    rdb,
	CacheKey: []byte(os.Getenv("INTROSPECTION_CACHE_KEY")), // ≥ 32 random bytes, from a secret
	Prefix:   "orders:introspection:idp:",
	Logger:   logger,
})
if err != nil {
	logger.ErrorContext(ctx, "introspection", log.Err(err))
	return
}
defer in.Close()

authn, err := auth.New(auth.Config{Opaque: in, Audience: "orders-api", Logger: logger})
```

- **Caching.** Active results are cached for 30s (never past the token's
  expiry) and inactive ones for 5s. Concurrent checks of one token share a
  single call. An endpoint failure is never cached.
- **Redis is optional; it shares answers across the fleet.**
  - Keys are SHA-256 hashes of the tokens, never the tokens themselves.
  - Every entry is HMAC-signed with `CacheKey` and bound to its token, so
    whoever can write to Redis still cannot forge a principal. A bad entry
    is ignored and the endpoint is asked again.
  - Give every verifier its own `Prefix`.
- **JWT issuers too.** Set `Issuer` to route JWTs of that issuer through
  introspection, for example to check revocation.
- **Combining verifiers.** `auth.All(a, b)` requires every verifier to
  accept, for example signature (JWKS) and revocation (introspection).

### Key rotation with static keys

Give `StaticConfig.Keys` several entries with distinct `ID`s, matched
against the token's `kid`:

1. Add the new key next to the old one.
2. Move the issuer to the new key.
3. Drop the old key after the longest token lifetime.

Public keys (`*rsa.PublicKey`, `*ecdsa.PublicKey`, `ed25519.PublicKey`) work
the same way.

### Public and optional routes

From `ExampleFromContext`:

```go
cfg := auth.Config{
	Public:   []string{"GET /healthz"},
	Optional: []string{"GET /catalog"}, // personalised when signed in
}
_ = cfg

catalog := func(w http.ResponseWriter, r *http.Request) {
	if p, ok := auth.FromContext(r.Context()); ok {
		_, _ = w.Write([]byte("hello " + p.Subject))
		return
	}
	_, _ = w.Write([]byte("hello guest"))
}
```

On an `Optional` route a request without a token runs anonymously, and the
`Authorizer` is not called. A request with a bad token is still rejected: a
bad token never degrades to anonymous.

### Custom authorization

`auth.RequireScopes` maps methods to required scopes. For anything else,
pass an `auth.AuthorizerFunc(func(ctx, method, p) error)`. Any error denies the
request with 403 (`FORBIDDEN`); return `auth.ErrInsufficientScope` or
`auth.ErrAuthUnavailable` to get those reasons instead.

## Testing

- Sign test tokens with an HMAC `Static` verifier and a fixed `Now`, as in
  `Example`. `Authenticate` checks a token without any transport.
- For JWKS, serve a key set from `httptest.NewServer`.
- For Redis-backed introspection, use `miniredis`.
- Assert with `errors.Is(err, auth.ErrTokenInvalid)` (or the other
  sentinels) and `status.Code(err)`.

## Observability

- **Metric**: `kitchen.auth.results` (counter) with `result` set to
  `token_missing`, `token_invalid`, `token_expired`, `forbidden`,
  `insufficient_scope`, `auth_unavailable`, or `ok` and `anonymous`
  on success.
- **Logs**: rejections at Debug (`rpc.method`, `reason`, `error`); outages
  at Warn (`authentication unavailable`); tampered introspection cache
  entries at Warn.
- **errs reasons**: `TOKEN_MISSING`, `TOKEN_INVALID`, `TOKEN_EXPIRED`,
  `FORBIDDEN`, `INSUFFICIENT_SCOPE`, `AUTH_UNAVAILABLE`.
- JWKS and introspection calls appear as `httpx` client spans.

## Production checklist

- List only what must be anonymous in `Public`. Never add a whole service
  prefix to make one method public.
- Set `Audience` (on the verifier or on `Config`) so tokens for other APIs
  are rejected.
- Set `JWKSConfig.Types` to your provider's access-token type.
- Load HMAC secrets, `ClientSecret` and `CacheKey` from a secret store.
  `CacheKey` must be at least 32 random bytes and the same across the fleet.
- Alert on `kitchen.auth.results{result="auth_unavailable"}`: it means
  callers are being turned away.
- Keep `StreamOutlivesToken` off.
- Close JWKS and introspection verifiers on shutdown.

## Migrating from the old kitchen

The old `guard` package becomes `auth`; see
[docs/migration/errors-auth.md](../docs/migration/errors-auth.md).
