# auth: package rules

Security-critical. Read `doc.go` and `docs/design/errors-auth.md` before
changing anything here.

- **Fail closed, always.** Every request without a valid token is rejected
  with `codes.Unauthenticated`. Do not add a "trust upstream" mode, a
  pass-through for unknown issuers, or a debug bypass. A method is public only
  if it is listed in `Config.Public`.
- **Audience is required** (`Config.Audience`, or an explicit
  `AnyAudience`). Never make it optional again.
- **Verify everything.** Every JWT is checked for:
  - signature;
  - `exp` and `nbf`, with bounded leeway;
  - `iss` and `aud`;
  - an algorithm allow-list. Reject `none`, and reject HMAC/RSA key
    confusion;
  - no `crit` header (RFC 7515 section 4.1.11).

  Do not relax any of these checks to make a test pass.
- **Status codes:**
  - authority (IdP, JWKS or introspection) unreachable →
    `ErrAuthUnavailable` (503). A transient failure must never look like an
    invalid token, and must never let a request through.
  - invalid or expired token → 401 / `Unauthenticated`.
- **JWKS:** lookups read an immutable snapshot (no lock, no I/O); refreshes
  never overlap and requests that need one wait for it (rotation must not
  401 a burst of new-kid tokens); an unknown kid while refreshes fail is
  503; `Verify` after `Close` fails fast; RFC 7518 key minimums apply to
  JWKS keys too.
- **Introspection cache:**
  - cache principals only while active; inactive/expired answers are cached
    for `NegativeTTL` only (a DoS guard), authority failures never;
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
