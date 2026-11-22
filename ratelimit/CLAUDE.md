# ratelimit: package rules

Read `doc.go` and `docs/design/errors-auth.md` first.

## Algorithm

- The algorithm is GCRA. The memory store and the Redis store, which runs a
  single atomic Lua script, must return the same decisions for the same
  inputs. When you change either store, test both.
- The GCRA store is custom code on purpose. The reason is in the design doc.
  Do not replace it with a library without asking the owner.

## Identity

- A request resolves to its most trusted identity, in this order: verified
  user, verified anonymous subject, unverified device ID, then client IP.
- A request must pass every Limit that applies to it. Rotating device IDs
  must never earn extra quota.
- User and device come from `xcontext`. The client IP comes from `server`,
  resolved through its trusted proxies. Never read raw `X-Forwarded-For`
  here.

## Store failure

- When the remote store fails, a circuit breaker stops calling it.
- `OnStoreError` then decides what happens. The default is a per-instance
  in-memory fallback. Allow and deny are explicit choices.

## Denials

- A denied request returns `codes.ResourceExhausted` with reason
  `RATE_LIMITED` and `RetryInfo`. Over HTTP this is a 429 with
  `Retry-After`.
- Responses carry the `RateLimit-Policy` / `RateLimit` headers, written with
  httpsfv, and `X-RateLimit-*`.
- These codes and headers are client contracts. Record any change to them in
  `docs/migration/errors-auth.md`.

## Redis

Redis clients come from `redisx`.
