# auth: package rules

Security-critical. Read `doc.go` and `docs/design/errors-auth.md` before
changing anything here.

- **Fail closed, always.** Every request without a valid token is rejected
  with `codes.Unauthenticated`. Do not add a "trust upstream" mode, a
  pass-through for unknown issuers, or a debug bypass. A method is public only
  if it is listed in `Config.Public`.
- **Verify everything.** Every JWT is checked for:
  - signature;
  - `exp` and `nbf`, with bounded leeway;
  - `iss` and `aud`;
  - an algorithm allow-list. Reject `none`, and reject HMAC/RSA key
    confusion.

  Do not relax any of these checks to make a test pass.
- **Status codes:**
  - authority (IdP, JWKS or introspection) unreachable →
    `ErrAuthUnavailable` (503). A transient failure must never look like an
    invalid token, and must never let a request through.
  - invalid or expired token → 401 / `Unauthenticated`.
- **Introspection cache:**
  - cache only active principals;
  - never keep a principal past its `exp`;
  - return copies, never shared pointers;
  - a Redis cache requires `IntrospectionConfig.Prefix`, so services never
    share keys.
- **Error messages:** never echo a token, a claim value, or a verifier error
  to the client. Clients get `errs` reasons; details go to the logs only,
  without the token.
- This is the only package allowed to import JWT/JWKS libraries (jwx v3), per
  D7. Outbound calls (JWKS, introspection) go through `httpx`.
- For every bug fixed, add an attack-style test: expired token, wrong `aud`,
  alg swap, unknown `kid`, authority down.
