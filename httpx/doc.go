// Package httpx is kitchen's HTTP client for calling APIs from a service.
//
// It is built on resty v3, which stays an implementation detail: the API
// is httpx's own [Client], [Request] and [Response]. Build a Client once per
// upstream with [New] and share it; each call builds a Request:
//
//	c, err := httpx.New(httpx.WithBaseURL("https://api.example.com/v1"),
//		httpx.WithBearer(key), httpx.WithRetry(httpx.RetryPolicy{}))
//
//	var out Item
//	err = c.Get("/items/{id}").PathParam("id", id).Do(ctx, &out)
//	if errors.Is(err, errs.ErrNotFound) { ... }
//	if se, ok := errors.AsType[*httpx.StatusError](err); ok { ... se.Body ... }
//
// Streaming (SSE, downloads) uses [Request.Send], which returns the open
// body.
//
// Safe defaults, each replaceable:
//
//   - [OriginGuard]: with a base URL, an absolute request URL on another
//     origin is refused, so client credentials never reach a foreign host
//     ([WithoutOriginGuard]).
//   - [SafeRedirectPolicy]: at most 10 redirects, no https→http downgrade,
//     and only Accept*, Content-Type/Length and User-Agent survive a
//     cross-origin redirect ([WithRedirectPolicy], [NoRedirects]).
//   - [WithSSRFGuard] (opt-in, for user-supplied URLs): [SSRFControl]
//     refuses non-public addresses after DNS resolution, so DNS rebinding
//     and redirects are covered.
//   - A non-2xx response returns an [errs] error wrapping a [*StatusError]
//     ([DefaultErrorMapper], [WithErrorMapper]); network failures map to
//     errs.ErrDependencyUnavailable and attempt timeouts to
//     errs.ErrUpstreamTimeout.
//   - Retries ([RetryPolicy]) and the circuit breaker ([BreakerConfig]) are
//     opt-in; retries only apply to idempotent requests or
//     [Request.IdempotencyKey].
//   - Bodies are limited to 32 MiB, there is no cookie jar, query values are
//     redacted from errors, calls are traced through [otelx.HTTPTransport]
//     and logs go to a *log.Logger.
//
// Services add their own rules with [WithRequestHook], [WithResponseHook]
// and [WithTransportMiddleware].
package httpx
