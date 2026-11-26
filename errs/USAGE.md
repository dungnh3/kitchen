# errs

`errs` is kitchen's error model. One `*errs.Error` becomes the same gRPC
status, REST JSON body and HTTP status on every transport. The format follows
[AIP-193](https://google.aip.dev/193).

`errs` is the canonical package for errors and status mapping
([D7](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22)). Build every
client-facing error here. Never build gRPC `status` values directly, which
the D7 lint forbids. Subpackage `errs/validation` turns protovalidate failures
into `errs` errors.

## Install

```sh
go get github.com/dungnh3/kitchen
```

`errs` and `errs/validation` live in the core module.

## Quick start

Declare a sentinel per failure the client can act on, then wrap the cause:

```go
var ErrAgentNotFound = errs.New(codes.NotFound, "AGENT_NOT_FOUND",
	errs.Domain("agents.example.com"), errs.Message("agent not found"))

func getAgent(id string) error {
	// The database error is kept for logs but never sent to clients.
	return errs.Wrap(sql.ErrNoRows, ErrAgentNotFound, errs.Meta("agent_id", id))
}

func Example() {
	err := getAgent("a1")

	fmt.Println(errors.Is(err, ErrAgentNotFound), errors.Is(err, sql.ErrNoRows))
	fmt.Println(errs.Code(err), errs.HTTPStatus(err))
	st := status.Convert(err)
	fmt.Println(st.Message())
	// Output:
	// true true
	// NotFound 404
	// agent not found
}
```

Return it from a gRPC method or a handler behind `kitchen/server`, and the
client receives:

```json
{"error":{"code":404,"message":"agent not found","status":"NOT_FOUND","details":[
  {"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"AGENT_NOT_FOUND",
   "domain":"agents.example.com","metadata":{"agent_id":"a1"}}]}}
```

What the defaults give you:

- **The cause stays inside.** `Wrap` keeps `sql.ErrNoRows` for `errors.Is`
  and for logs. The cause never appears in the status or the JSON body.
- **Unknown errors become INTERNAL.** A plain Go error, a panic or a
  downstream fault is sent as `INTERNAL "internal error"`. It is logged with
  its cause.
- **Sentinels are immutable.** `With` and `Wrap` return copies, so shared
  sentinels can be used from any goroutine.
- **`errors.Is` works on reasons.** A code sentinel (`errs.ErrNotFound`)
  matches every error with that gRPC code. Any other sentinel matches by
  reason and domain.

## Configuration

Errors are configured with options:

| Option | Effect |
|---|---|
| `errs.Domain(d)` | `ErrorInfo.domain`. When an error has none, the `Handler`'s `Config.Domain` is used. |
| `errs.Message(m)` | Developer-facing English message. Default: the generic message of the code. |
| `errs.Meta(k, v)` | `ErrorInfo.metadata`. Also the template data for localized messages. |
| `errs.Details(msgs...)` | Extra standard details (`google.rpc.*`). |
| `errs.RetryAfter(d)` | `google.rpc.RetryInfo`, and the `Retry-After` header over HTTP. |

`errs.Config` configures the `Handler`. Its zero value is valid.

| Field | Default | When to change |
|---|---|---|
| `Logger` | `log.Default()` | Pass the service logger if it is not the process logger. |
| `Localizer` | none | Set an `*i18n.Bundle` to add a `LocalizedMessage` in the request's language. |
| `Domain` | none | Always set it in a service: AIP-193 requires a domain. |

There are no environment variables.

## Integration with kitchen

### server

`kitchen/server` installs an `errs.Handler` on every path. That covers gRPC
interceptors, the REST gateway, panics, plain HTTP handlers and oversized
bodies. Use `server.WithI18n` for the default handler with localization, or
`server.WithErrors` for a handler you configure:

```go
bundle, err := i18n.New(language.English, i18n.WithDefaults())
if err != nil {
	panic(err)
}
// Default handler, localized: the usual setup.
srv, err := server.New(server.WithI18n(bundle))
if err != nil {
	panic(err)
}
_ = srv

// A handler you configure yourself (domain, logger, localizer).
eh := errs.NewHandler(errs.Config{Localizer: bundle, Domain: "agents.example.com"})
srv, err = server.New(server.WithErrors(eh))
```

The two options cannot be combined. See [server/USAGE.md](../server/USAGE.md)
for the full service wiring.

### Plain HTTP handlers

In a handler outside the gateway, call `WriteHTTP` with the same `Handler`.
It sets `Content-Type`, `Retry-After` (from `RetryInfo`) and, for 401,
`WWW-Authenticate` (RFC 6750):

```go
eh := errs.NewHandler(errs.Config{Domain: "svc.example.com"})
rec := httptest.NewRecorder()
req := httptest.NewRequest(http.MethodGet, "/v1/agents/a1", nil)
eh.WriteHTTP(rec, req, errs.Wrap(errors.New("db down"), ErrAgentNotFound, errs.Meta("agent_id", "a1")))
```

The package-level `errs.WriteHTTP` uses a handler with no domain and no
localization. It is meant for tests and small tools.

### i18n

An `*i18n.Bundle` is an `errs.Localizer`. Message IDs are reasons, and
metadata is the template data:

```go
bundle, err := i18n.New(language.English, i18n.WithDefaults(),
	i18n.WithMessages(language.English, map[string]string{"AGENT_NOT_FOUND": "Agent {{.agent_id}} does not exist."}))
if err != nil {
	panic(err)
}
eh := errs.NewHandler(errs.Config{
	Logger:    log.Discard(),
	Localizer: bundle,
	Domain:    "agents.example.com",
})
st := eh.Status(context.Background(), ErrAgentNotFound.With(errs.Meta("agent_id", "a1")))
```

`Status.Message` stays in English, for developers. The text for users is in
the `LocalizedMessage` detail. See [i18n/USAGE.md](../i18n/USAGE.md).

### Databases, HTTP clients and other kitchen packages

The adapters map library errors to `errs` sentinels. The SQL text, table and
column names, and upstream bodies stay in the wrapped cause:

- **`pgx.MapError(err)`**:
  - `jpgx.ErrNoRows` → `ErrNotFound`;
  - unique violation → `ErrAlreadyExists`;
  - other integrity errors → `ErrFailedPrecondition`;
  - serialization failure or deadlock → `ErrAborted`;
  - query canceled → `ErrDeadlineExceeded`;
  - connection errors → `ErrUnavailable`.
- **`mysqlx.MapError(err)`** and **`mongox.MapError(err)`**: the same mapping
  for MySQL and MongoDB.
- **`httpx`**: a non-2xx upstream answer becomes an `errs` error
  (`DefaultErrorMapper`). For example, 404 → `ErrNotFound`, 429/502/503 →
  `ErrDependencyUnavailable`, and timeouts → `ErrUpstreamTimeout`. Use
  `httpx.WithErrorMapper` to map an API's own errors.
- **`auth`**, **`ratelimit`** and **`server`** return catalog sentinels:
  - `ErrTokenMissing`, `ErrTokenExpired` and `ErrInsufficientScope`;
  - `ErrRateLimited` with `RetryAfter`;
  - `ErrServiceDraining`, `ErrPayloadTooLarge` and `ErrMalformedRequest`.

`errs.Catalog()` lists every catalog sentinel. Each has English and
Vietnamese text in `i18n.WithDefaults()`.

## Advanced

### Copy a sentinel with request data

Use `With` when there is no cause to keep:

```go
// With returns a copy: the shared sentinel is never modified.
err := errs.ErrRateLimited.With(
	errs.Meta("limit", "100"),
	errs.RetryAfter(30*time.Second),
)
fmt.Println(errors.Is(err, errs.ErrRateLimited), errs.Code(err), errs.HTTPStatus(err))
fmt.Println(err.Metadata()["limit"], len(errs.ErrRateLimited.Metadata()))
// Output:
// true ResourceExhausted 429
// 100 0
```

### Field errors

Use `BadRequest` for request checks that do not come from protovalidate:

```go
err := errs.BadRequest(
	errs.FieldViolation{Field: "agent.name", Description: "must not be empty", Reason: "string.min_len"},
)
```

This produces `INVALID_ARGUMENT` with a `google.rpc.BadRequest` detail that
lists each field.

### HTTP status mapping

The HTTP status comes from the gRPC code. The only exceptions are
`PAYLOAD_TOO_LARGE` (413) and `UNSUPPORTED_MEDIA_TYPE` (415):

```go
fmt.Println(errs.HTTPStatus(errs.ErrInvalidArgument))
fmt.Println(errs.HTTPStatus(errs.ErrPayloadTooLarge))
fmt.Println(errs.HTTPStatus(errs.ErrUnsupportedMediaType))
fmt.Println(errs.HTTPStatus(errors.New("boom")))
// Output:
// 400
// 413
// 415
// 500
```

### Errors from other services

When you call another gRPC service and return its error, `ToStatus` decides
what your client may see:

```go
// A plain Go error never reaches the client: it becomes INTERNAL.
st, sanitized := errs.ToStatus(fmt.Errorf("query users: %w", errors.New("dial tcp 10.0.0.7:5432: refused")))
fmt.Println(st.Code(), st.Message(), sanitized)

// A downstream service's client error keeps its code and message.
st, sanitized = errs.ToStatus(status.Error(codes.NotFound, "user u1 not found"))
fmt.Println(st.Code(), st.Message(), sanitized)

// A downstream server fault keeps its code with the generic message.
st, sanitized = errs.ToStatus(status.Error(codes.Unavailable, "redis 10.0.0.9:6379 down"))
fmt.Println(st.Code(), st.Message(), sanitized)

// A downstream 401 is about this service's credentials, not the client's.
st, _ = errs.ToStatus(status.Error(codes.Unauthenticated, "bad service token"))
fmt.Println(st.Code())
// Output:
// Internal internal error true
// NotFound user u1 not found false
// Unavailable service unavailable true
// Internal
```

This is a security default, so do not work around it. Downstream messages
can carry addresses, SQL and credentials. To forward a specific downstream
failure on purpose, map it to your own sentinel with `errs.Wrap`.

### Match errors received from a kitchen service

On the client side, `FromError` rebuilds the `*errs.Error`, so shared
sentinels work with `errors.Is`:

```go
// A client receives a status error and matches it against a shared sentinel.
received := status.Convert(ErrAgentNotFound).Err()
fmt.Println(errors.Is(errs.FromError(received), ErrAgentNotFound))
// Output: true
```

### Retry hints

`RetryAfter` sets `RetryInfo`, and over HTTP also the `Retry-After` header:

```go
rec := httptest.NewRecorder()
errs.WriteHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil),
	errs.ErrDependencyUnavailable.With(errs.RetryAfter(5*time.Second)))
fmt.Println(rec.Code, rec.Header().Get("Retry-After"))
// Output: 503 5
```

## Backends and adapters

### errs/validation

`kitchen/server` validates every request with protovalidate by default.
Turn this off with `server.WithoutValidation()`. A failure reaches the client
as `INVALID_ARGUMENT` with a `BadRequest`: one violation per rule, with the
field path and the rule id as the reason.

Call `validation.FromError` yourself for messages that do not arrive through
the server, such as queue payloads or files:

```go
func checkRequest(req proto.Message) error {
	err := protovalidate.Validate(req)
	if err == nil {
		return nil
	}
	if e, ok := validation.FromError(err); ok {
		return e
	}
	return err // a CompilationError: the rules themselves are broken
}
```

It is a separate package so that importing `errs` does not pull in
protovalidate and CEL.

## Testing

Assert with `errors.Is` against your sentinels, and with `errs.Code` for
codes. In HTTP tests, decode the envelope the way a client would:

```go
rec := httptest.NewRecorder()
errs.WriteHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/agents/a1", nil), ErrAgentNotFound)

var body struct {
	Error struct {
		Code    int    `json:"code"`
		Status  string `json:"status"`
		Details []struct {
			Type   string `json:"@type"`
			Reason string `json:"reason"`
		} `json:"details"`
	} `json:"error"`
}
if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
	panic(err)
}
fmt.Println(body.Error.Code, body.Error.Status, body.Error.Details[0].Reason)
// Output: 404 NOT_FOUND AGENT_NOT_FOUND
```

Pass `log.Discard()` as `Config.Logger` to keep test output quiet.

## Observability

The `Handler` logs at Error level, with attributes `code`, `error` (the full
cause chain) and `operation` (the HTTP method and path when known):

- `"error sanitized to INTERNAL"`: a non-`errs` error was hidden from the
  client;
- `"server error"`: an `errs` error with code `INTERNAL`, `UNKNOWN` or
  `DATA_LOSS`.

Client errors such as `NOT_FOUND` are not logged by `errs`. The access log in
`kitchen/server` records them with their code, so each request produces one
line. `errs` emits no spans or metrics of its own. The otelgrpc and otelhttp
instrumentation in `server` records the status code.

## Production checklist

- Set `Config.Domain`, or `errs.Domain` on every sentinel.
- Use stable `UPPER_SNAKE` reasons. Never rename or reuse one: clients
  switch on them.
- Add English and Vietnamese text for every new reason.
- Never put SQL, tokens, stack traces or upstream bodies in `Message` or
  `Meta`. Wrap them as the cause instead.
- Map database errors with `pgx.MapError`, `mysqlx.MapError` or
  `mongox.MapError` before returning them.
- Do not forward downstream statuses by hand; let `ToStatus` sanitize them.

## Migrating from the old kitchen

`ecode`, `errors` and `localization` are replaced by `errs` and `i18n`. See
[docs/migration/errors-auth.md](../docs/migration/errors-auth.md).
