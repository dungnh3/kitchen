# httpx: package rules

This is kitchen's only HTTP client (D7). Every outbound call in kitchen and
in services goes through it, including the calls made by `auth`, `webmeta`
and `otelx`. Read `doc.go` and `docs/design/transport.md` first.

## Safe defaults

The defaults are safe. Each one can be replaced with an explicit option, but
none may be loosened silently:

- **Origin guard.** When a client has a base URL, a request to an absolute
  URL on another origin is refused, so credentials never leak.
- **Redirects** (`SafeRedirectPolicy`):
  - at most 10;
  - never from https to http;
  - only Accept*, Content-Type/Length and User-Agent survive a cross-origin
    hop.
- **SSRF guard.** `WithSSRFGuard` is opt-in and is required for any URL a user
  or an LLM supplies. It checks addresses after DNS resolution and again on
  every redirect.
- **Limits.**
  - Response bodies are capped at 32 MiB.
  - There is no cookie jar.
  - Query values are redacted from errors and logs.

## Errors and retries

- **Errors are `errs` errors.**
  - Non-2xx responses go through the error mapper, wrapping `*StatusError`.
  - Network failures become `ErrDependencyUnavailable`.
  - Attempt timeouts become `ErrUpstreamTimeout`.

  Never return raw resty or net errors to callers.
- **Retries.** Retries and the circuit breaker are opt-in. A request is
  retried only when it is idempotent or carries an `IdempotencyKey`.
- **Backoff.** Backoff and jitter come from `timex`. An upstream
  `Retry-After` is honoured but capped (`RetryPolicy.MaxRetryAfter`).

## resty v3

resty v3 is an implementation detail. Never expose resty types in the API,
and never let other packages import resty; D7 lint bans it.
