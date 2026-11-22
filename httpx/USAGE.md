# httpx

`httpx` is kitchen's HTTP client for calling APIs from a service. Every
outbound HTTP call goes through it, not through `http.Client` or resty. It is
the canonical package for HTTP clients
([D7](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22)).

## Install

```sh
go get github.com/dungnh3/kitchen
```

`httpx` lives in the core module. resty v3 is used inside, but its types never
appear in the API.

## Quick start

Build one `Client` per upstream and share it across goroutines. Each call
starts a `Request` with `Get`, `Head`, `Post`, `Put`, `Patch`, `Delete` or
`NewRequest`, then ends with `Do` (decode JSON) or `Send` (stream).

```go
// Build once per upstream, share across goroutines.
c, err := httpx.New(httpx.WithBaseURL(api.URL+"/v1"), httpx.WithBearer("token"),
	httpx.WithRetry(httpx.RetryPolicy{}))
if err != nil {
	panic(err)
}
ctx := context.Background()

var out struct {
	ID string `json:"id"`
}
err = c.Post("/orders").IdempotencyKey("order-1").JSON(map[string]int{"qty": 1}).Do(ctx, &out)
fmt.Println(out.ID, err)

err = c.Get("/items/{id}").PathParam("id", "missing").Do(ctx, nil)
fmt.Println(errors.Is(err, errs.ErrNotFound), httpx.StatusCode(err))
```

`Do(ctx, &out)` decodes a 2xx JSON body into `out`. Pass `nil` to ignore the
body, or a `*[]byte` to get the raw bytes. An empty 2xx body leaves `out`
unchanged. A non-2xx response returns an error.

The defaults are safe:

- **Origin guard.** With a base URL, an absolute request URL on another
  origin is refused (`ErrDestinationNotAllowed`). Client credentials never
  reach a foreign host.
- **Safe redirects.** At most 10 redirects. An https to http redirect is
  refused. On a cross-origin hop only `Accept*`, `Content-Type`,
  `Content-Length` and `User-Agent` survive, so `Authorization` is dropped.
- **Body limit.** Response bodies are capped at 32 MiB.
- **No cookie jar.** A shared client never replays one caller's cookies.
- **Redaction.** Query values and passwords are redacted from errors,
  `StatusError.URL` and `Response.URL()`, so `?key=...` stays out of logs.
- **Timeout.** Each attempt, body included, is bounded to 30s.
- **User-Agent** is `kitchen-httpx` unless you set one.
- **Errors are `errs` errors**, so a kitchen server answers with a sensible
  code without leaking the upstream body.
- **Tracing** is always on, through `otelx.HTTPTransport`.

Retries, the circuit breaker and the SSRF guard are opt-in.

## Configuration

| Option | Default | When to change it |
|---|---|---|
| `WithBaseURL(u)` | none | Almost always: one client per upstream. Enables the origin guard. |
| `WithBearer`, `WithBasicAuth`, `WithHeader` | none | Credentials and headers sent on every request. Per call: `Request.Bearer`, `Request.Header`. |
| `WithTimeout(d)` | 30s per attempt | Lower for fast APIs. `0` for streams; then rely on the ctx deadline. Per call: `Request.Timeout`. |
| `WithRetry(RetryPolicy{})` | off | Idempotent reads of a flaky upstream. |
| `WithCircuitBreaker(BreakerConfig{})` | off | An upstream that fails for minutes at a time. |
| `WithSSRFGuard(SSRFConfig{})` | off | **Required** for any URL a user or an LLM supplies. |
| `WithMaxResponseBody(n)` | 32 MiB | Raise for known large payloads. Per call: `Request.MaxResponseBody`. |
| `WithMaxErrorBody(n)` | 64 KiB | How much of a non-2xx body `StatusError.Body` keeps. |
| `WithErrorMapper(m)` | `DefaultErrorMapper` | The upstream gives a status a meaning of its own. |
| `WithRedirectPolicy(p)` | `SafeRedirectPolicy(10)` | `NoRedirects` to see the 3xx itself. |
| `WithTransportSettings(s)` | dial 30s, TLS 10s, idle 90s, 100 idle conns | Client certificates (`TLS`), connection caps. |
| `WithTransport(rt)` | kitchen transport | Rare. Cannot be combined with `WithSSRFGuard` or `WithTransportSettings`. |
| `WithLogger(l)` | `log.Default()` | Pass the service's `*log.Logger`. |
| `WithTracerProvider`, `WithMeterProvider`, `WithPropagator` | OTel globals | Tests, or a non-global provider. |

`httpx` reads no environment variables.

## Integration with kitchen

### Logger, errors and tracing

Pass the logger from `main` (where `log.Setup` runs once). resty's own
warnings and errors go through it. Errors come back as `errs` errors that a
handler can return as is.

```go
logger := log.Default() // after log.Setup in main
payments, err := httpx.New(
	httpx.WithBaseURL("https://payments.example.com/v2"),
	httpx.WithBearer(os.Getenv("PAYMENTS_API_KEY")),
	httpx.WithLogger(logger),
	httpx.WithTimeout(5*time.Second), // per attempt
	httpx.WithRetry(httpx.RetryPolicy{}),
	httpx.WithCircuitBreaker(httpx.BreakerConfig{}),
	// Traces and metrics use the global providers that otelx/sdk.Setup
	// installs; WithTracerProvider / WithMeterProvider override them.
)
if err != nil {
	logger.Error("payments client", log.Err(err))
	return
}
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // whole call, retries included
defer cancel()

var charge struct {
	ID string `json:"id"`
}
err = payments.Get("/charges/{id}").PathParam("id", "ch_1").Do(ctx, &charge)
switch {
case errors.Is(err, errs.ErrNotFound):
	// 404 upstream: the handler can return err as is (NotFound).
case err != nil:
	// Returning err keeps the errs code: Unavailable, DeadlineExceeded or
	// Internal. The upstream body stays in the cause, never in the reply.
}
```

`DefaultErrorMapper` maps a non-2xx status to an `errs` error that wraps a
`*StatusError`:

| Upstream | errs error | gRPC code |
|---|---|---|
| 404 | `errs.ErrNotFound` | NotFound |
| 408, 504 | `errs.ErrUpstreamTimeout` | DeadlineExceeded |
| 429, 502, 503 | `errs.ErrDependencyUnavailable`, with `Retry-After` as RetryInfo | Unavailable |
| anything else, 401 and 403 included | `errs.ErrInternal` | Internal |

A 401 from an upstream is our credential problem, not the caller's, so it is
Internal. Network failures map to `ErrDependencyUnavailable` and attempt
timeouts to `ErrUpstreamTimeout`. Cancellation and TLS verification failures
pass through unchanged.

Read the details with `errors.AsType[*httpx.StatusError]`, `httpx.StatusCode`
or `ErrorInto`:

```go
var apiErr struct {
	Code string `json:"code"`
}
err = c.Post("/orders").JSON(map[string]string{"sku": "x"}).ErrorInto(&apiErr).Do(ctx, nil)
if se, ok := errors.AsType[*httpx.StatusError](err); ok {
	fmt.Println(se.StatusCode, string(se.Body))
}
fmt.Println(apiErr.Code, errs.Code(err)) // unmapped statuses are Internal
```

`StatusError` also has `Header` and `Decode(v)`.

### Server wiring

A service builds its clients in `main` and passes them to its handlers. For
the full service setup (logger, telemetry, servers, shutdown), see
[server/USAGE.md](../server/USAGE.md).

## Advanced

### Retries

Use retries for upstreams with brief failures. Zero fields take the defaults:
3 attempts, 100ms to 5s backoff, `Retry-After` capped at 30s.

```go
c, err := httpx.New(httpx.WithBaseURL(api.URL), httpx.WithRetry(httpx.RetryPolicy{
	MaxAttempts:   3,                     // first attempt included
	MinBackoff:    10 * time.Millisecond, // timex.Backoff with jitter
	MaxBackoff:    time.Second,
	MaxRetryAfter: 10 * time.Second, // a longer Retry-After returns at once
}))
if err != nil {
	panic(err)
}
ctx := context.Background()

// GET is idempotent: retried.
err = c.Get("/status").Do(ctx, nil)
fmt.Println(err, calls.Load())

// A POST is retried only with an idempotency key the server deduplicates on.
calls.Store(0)
err = c.Post("/payments").IdempotencyKey("pay-7f3a").JSON(map[string]int{"amount": 100}).Do(ctx, nil)
fmt.Println(err, calls.Load())
```

- Only GET, HEAD, PUT, DELETE, OPTIONS and TRACE are retried. POST and PATCH
  need `IdempotencyKey`, which sets the `Idempotency-Key` header.
- `DefaultRetryable` retries network failures, attempt timeouts and 429, 502,
  503, 504. It never retries a refused destination, a TLS failure or a
  cancelled request. Replace it with `RetryPolicy.Retryable`.
- Backoff comes from `timex.Backoff`, the one formula kitchen uses.
- On 429 and 503 the upstream `Retry-After` is honoured. If it is longer than
  `MaxRetryAfter`, the response is returned at once instead of blocking.
- `WithTimeout` bounds one attempt. Bound the whole call with a ctx deadline.
- A `Body` reader that cannot be rewound (not an `io.ReadSeeker`) disables
  retries for that request. `JSON`, `*bytes.Buffer` and `*strings.Reader`
  bodies are retryable.

### Circuit breaker

Use it when an upstream can be down for minutes, so callers fail fast instead
of queueing on timeouts. One client has one breaker.

```go
c, err := httpx.New(httpx.WithBaseURL(api.URL), httpx.WithCircuitBreaker(httpx.BreakerConfig{
	FailureThreshold: 2,                // 5xx and transport errors count
	ResetTimeout:     30 * time.Second, // window, and how long it stays open
	OnStateChange: func(from, to string) {
		fmt.Println("breaker:", from, "->", to)
	},
}))
```

While open, calls return `errs.ErrDependencyUnavailable` wrapping
`httpx.ErrCircuitOpen`, without sending anything. Set `FailureRatio` and
`MinRequests` for a ratio breaker, and `IsFailure` to change what counts.

### SSRF guard for user-supplied URLs

Any URL that comes from a user, a webhook config or an LLM must go through a
client with `WithSSRFGuard`. Without a base URL, the origin guard does not
apply, so this guard is the only protection.

```go
// No base URL: every request names its own URL, so guard the dialer.
c, err := httpx.New(httpx.WithSSRFGuard(httpx.SSRFConfig{}))
if err != nil {
	panic(err)
}
userURL := internal.URL + "/admin" // 127.0.0.1: refused after DNS resolution
err = c.Get(userURL).Do(context.Background(), nil)
fmt.Println(errors.Is(err, httpx.ErrDestinationNotAllowed), errs.Code(err))
```

The guard checks the resolved address of every connection, redirects
included, so DNS rebinding does not get past it. It refuses loopback,
private, link-local, CGNAT (cloud metadata), NAT64 and other special ranges,
and allows ports 80 and 443 only. It also ignores proxy environment
variables, since a proxy would dial past it. `SSRFConfig.Allow` and
`SSRFConfig.Ports` open specific prefixes and ports; keep them as narrow as
possible.

### Streaming responses

Use `Send` for SSE, downloads or non-JSON bodies. It returns the open body;
you must close it.

```go
// Streams outlive one attempt: rely on the ctx deadline, not WithTimeout.
c, err := httpx.New(httpx.WithBaseURL(api.URL), httpx.WithTimeout(0))
if err != nil {
	panic(err)
}
res, err := c.Get("/events").Header("Accept", "text/event-stream").Send(context.Background())
if err != nil {
	panic(err) // a non-2xx status is an error here too; res is nil then
}
defer func() { _ = res.Body().Close() }()

line, _ := bufio.NewReader(res.Body()).ReadString('\n')
fmt.Print(res.StatusCode(), " ", line)
```

The 32 MiB limit still applies to a `Send` body. For a large download, raise
it per request with `Request.MaxResponseBody(n)`. `0` removes the limit:
only do that for a trusted upstream, because the limit stops a hostile or
broken server from filling memory or disk.

### Custom error mapping

Use `WithErrorMapper` when an upstream status has a meaning of its own. Wrap
`se` so `errors.AsType` still finds it, and fall back to the default.

```go
c, err := httpx.New(httpx.WithBaseURL(api.URL), httpx.WithErrorMapper(func(se *httpx.StatusError) error {
	if se.StatusCode == http.StatusConflict {
		return errs.Wrap(se, errs.ErrResourceConflict) // keep se in the chain
	}
	return httpx.DefaultErrorMapper(se)
}))
```

`WithErrorMapper(nil)` returns the bare `*StatusError`.

### Hooks and transport middleware

Use request hooks for signatures or per-call headers, response hooks for
audit logs or metrics, and transport middleware for anything at the
`http.RoundTripper` level.

```go
logger := log.Default() // main calls log.Setup once; pass the logger down
c, err := httpx.New(httpx.WithBaseURL(api.URL),
	httpx.WithLogger(logger), // resty's warnings go through kitchen/log
	httpx.WithRequestHook(func(r *httpx.Request) error {
		r.Header("X-Signature", "sig-"+r.Method()) // runs after OriginGuard
		return nil
	}),
	httpx.WithResponseHook(func(res *httpx.Response) error {
		logger.Debug("upstream call", log.Int("status", res.StatusCode()), log.String("url", res.URL()))
		return nil
	}),
	httpx.WithTransportMiddleware(func(next http.RoundTripper) http.RoundTripper {
		return roundTripFunc(func(r *http.Request) (*http.Response, error) {
			r = r.Clone(r.Context())
			r.Header.Set("X-Via", "mw") // traced and error-mapped like the base transport
			return next.RoundTrip(r)
		})
	}),
)
```

- A request hook error aborts the call and is returned as is.
- A response hook runs once per call, after retries, whatever the status. Its
  error is joined with the call's error.
- The first middleware is the innermost. Tracing and error mapping wrap all
  of them.

### Turning off a safe default

`WithoutOriginGuard()` lets a client with a base URL call absolute URLs on
other origins. **Warning:** the client's `Authorization` and other headers
then go to those hosts. The guard exists so a URL from a response or a user
cannot steal your API key. Prefer a second client without credentials.

## Testing

- Point the client at an `httptest.Server` with `WithBaseURL(srv.URL)`. All
  defaults stay on, so tests see real behaviour.
- Pass `httpx.WithLogger(log.Discard())` to silence logs.
- Assert errors with `errors.Is(err, errs.ErrNotFound)`, `errs.Code(err)`,
  `httpx.StatusCode(err)` and `errors.AsType[*httpx.StatusError]`.
- `WithSSRFGuard` refuses `httptest` servers (loopback, random port). Test the
  guard itself that way, as `ExampleWithSSRFGuard` does. Do not loosen
  `SSRFConfig` in production code to make tests pass.
- Use small `MinBackoff` values in retry tests.

## Observability

- **Spans and metrics:** OTel HTTP client spans and metrics from
  `otelx.HTTPTransport`, one span per attempt, with trace context injected
  into the request. The providers are the OTel globals unless set with
  `WithTracerProvider` / `WithMeterProvider`.
- **Logs:** resty's messages are logged as `httpx: resty` with a `detail`
  attribute, at their own level. `httpx` logs nothing per request; add a
  response hook for that.
- **errs reasons:** `DESTINATION_NOT_ALLOWED` (origin guard, redirect
  downgrade, SSRF guard), `DEPENDENCY_UNAVAILABLE`, `UPSTREAM_TIMEOUT`, and
  the generic NotFound and Internal errors from `DefaultErrorMapper`.

## Production checklist

- One `Client` per upstream, built in `main`, shared. Never one per request.
- Set `WithBaseURL` so the origin guard is on.
- Set `WithTimeout` to what the upstream needs, and a ctx deadline per call.
- Enable `WithRetry` for idempotent calls; use `IdempotencyKey` for writes
  only when the upstream deduplicates on it.
- Use `WithSSRFGuard` for every user- or LLM-supplied URL.
- Pass the service's `*log.Logger` with `WithLogger`.
- Never put secrets in the path; query values are redacted, paths are not.
- Do not use `WithoutOriginGuard`, `MaxResponseBody(0)` or a loose
  `SSRFConfig` without a written reason.
- Close every `Send` body.

## Migrating from the old kitchen

`nap`, `kresty` and resty v2 become `httpx`. See the mapping table in
[docs/migration/transport.md](../docs/migration/transport.md).
