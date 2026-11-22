# webmeta

`webmeta` fetches a web page and extracts its title, description, preview
image and icon (Open Graph, Twitter cards, HTML fallbacks). It is safe to
point at untrusted URLs, such as URLs an end user or an LLM supplies. It is
the canonical package for web page metadata (D7 row "Web page metadata" in
[`docs/decisions.md`](../docs/decisions.md)): do not parse pages with
goquery or call `x/net/html` yourself.

## Install

`webmeta` is in the core module:

```sh
go get github.com/dungnh3/kitchen
```

## Quick start

```go
f, err := webmeta.New(webmeta.Config{}) // SSRF guard, 10s timeout, 2 MiB cap
if err != nil {
	panic(err)
}
defer f.Close() // build one Fetcher and share it

m, err := f.Fetch(context.Background(), "https://go.dev/")
if err != nil {
	fmt.Println("fetch:", err)
	return
}
fmt.Println(m.Title, m.Description, m.Image, m.Icon)
```

A `Fetcher` is safe for concurrent use. Build one at startup and share it.
`Close` releases idle connections.

The zero `Config` is the safe default:

- only public addresses are dialled, on ports 80 and 443 only;
- addresses are checked after DNS resolution and again on every redirect,
  so DNS rebinding and redirects to localhost are refused;
- proxy environment variables are ignored;
- at most 5 redirects, and never from https to http;
- the whole fetch, redirects included, times out after 10 s;
- at most 2 MiB of the body is read;
- URLs with credentials (`http://user:pass@host/`) are refused.

### What you get back

`Fetch` returns a `*Metadata`. Each field takes the first non-empty source:

| Field | Sources, in order |
|---|---|
| `URL` | final URL after redirects, query values redacted |
| `Title` | `og:title`, `<title>`, `twitter:title` |
| `Description` | `og:description`, `<meta name="description">`, `twitter:description`, first body `<p>` |
| `SiteName` | `og:site_name` |
| `Image` | `og:image:secure_url`, `og:image` / `og:image:url`, `twitter:image` |
| `Icon` | `apple-touch-icon`, `rel=icon`, then `/favicon.ico` |
| `CanonicalURL` | `<link rel="canonical">`, `og:url` |

Parse rules:

- Relative URLs are resolved against the final URL, or `<base href>` when
  present. Only http and https URLs are returned; `javascript:` and other
  schemes become empty.
- Whitespace in text values is collapsed.
- Parsing stops at `<body>` when the head has a description.
- Otherwise it reads on to the first `<p>` with at least 30 runes of text.
  Shorter paragraphs (menus, captions) are skipped. The paragraph is cut to
  300 runes at a word boundary, with a trailing `…`.
- Text inside `<script>`, `<style>`, `<template>` and `<noscript>` is never
  used.
- A truncated body still parses: metadata lives in `<head>`.
- The charset is decoded from the `Content-Type` header or the document.

Every value comes from a page you do not control. Escape it before you
render it, and treat `Image` and `Icon` as untrusted URLs.

If you already have the HTML, call `webmeta.Parse(base, r)` directly; see
`ExampleParse` in godoc.

## Configuration

`Config` fields. The zero value of each is the default.

| Field | Default | When to change it |
|---|---|---|
| `Timeout` | 10 s | Slow sites, or a tighter request budget. |
| `MaxBytes` | 2 MiB | Rarely. Metadata is in `<head>`. |
| `UserAgent` | `kitchen-webmeta/1 (+https://github.com/dungnh3/kitchen)` | Sites that block unknown agents. |
| `MaxRedirects` | 5 | Rarely. |
| `Ports` | 80, 443 | Only together with `Allow`, for a trusted target. |
| `Allow` | none | Tests, or one known internal site. Read the warning below. |
| `ClientOptions` | none | Logger, tracer/meter providers, retries. |

There are no environment variables.

## Integration with kitchen

### httpx and the SSRF guard

Every fetch goes through the kitchen `httpx` client. `New` applies these
options last, so `ClientOptions` cannot loosen them:

- `httpx.WithSSRFGuard(httpx.SSRFConfig{Ports, Allow})`;
- `httpx.WithTimeout`;
- `httpx.WithMaxResponseBody`;
- `httpx.WithRedirectPolicy(httpx.SafeRedirectPolicy(MaxRedirects))`.

The guard refuses loopback, private, link-local, multicast and unspecified
addresses, plus special ranges such as CGNAT (`100.64.0.0/10`), NAT64,
6to4 and Teredo. Cloud metadata endpoints (`169.254.169.254`) are covered.

### Logger, telemetry, retries

Pass httpx options through `ClientOptions`:

```go
f, err := webmeta.New(webmeta.Config{
	ClientOptions: []httpx.Option{
		httpx.WithLogger(log.Discard()),
		httpx.WithRetry(httpx.RetryPolicy{}),
	},
})
fmt.Println(err == nil)
f.Close()

// A custom transport would bypass the SSRF guard, so New refuses it.
_, err = webmeta.New(webmeta.Config{
	ClientOptions: []httpx.Option{httpx.WithTransport(http.DefaultTransport)},
})
fmt.Println(err != nil)
// Output:
// true
// true
```

In a service, pass the `*log.Logger` from `main` (where `log.Setup` ran)
instead of `log.Discard()`. Use `httpx.WithTracerProvider` and
`httpx.WithMeterProvider` to override the global OTel providers.

`httpx.WithTransport` is rejected on purpose: it would replace the dialer
that runs the guard. There is no other way to turn the guard off.

### Errors

Every error is an `errs` error, so a kitchen server maps it to a gRPC and
HTTP status without extra code:

| Error | Code | When |
|---|---|---|
| `webmeta.ErrInvalidURL` | InvalidArgument | not an absolute http(s) URL, or has credentials |
| `httpx.ErrDestinationNotAllowed` | InvalidArgument | the guard refused the address, port or an https→http redirect |
| `webmeta.ErrNotHTML` | InvalidArgument | `Content-Type` is not `text/html` or `application/xhtml+xml` |
| `errs.ErrNotFound` | NotFound | upstream 404 |
| `errs.ErrUpstreamTimeout` | DeadlineExceeded | timeout, upstream 408 or 504 |
| `errs.ErrDependencyUnavailable` | Unavailable | network failure, upstream 429, 502 or 503 |
| `errs.ErrInternal` | Internal | any other upstream status |

```go
f, err := webmeta.New(webmeta.Config{})
if err != nil {
	panic(err)
}
defer f.Close()

ctx := context.Background()
for _, u := range []string{"ftp://example.com/file", "http://10.0.0.1/admin"} {
	_, err := f.Fetch(ctx, u)
	info, _ := errs.Info(err)
	fmt.Println(info.GetReason(), errs.Code(err), errs.HTTPStatus(err))
}
// Output:
// INVALID_URL InvalidArgument 400
// DESTINATION_NOT_ALLOWED InvalidArgument 400
```

`httpx.StatusCode(err)` returns the upstream HTTP status, or 0. A missing
`Content-Type` is accepted and parsed as HTML.

### Inside a service handler

Pass the request `ctx`, so the client's deadline and cancellation stop the
fetch and the trace continues. Return the error as is: the server sends its
code and safe message, and the wrapped cause stays in the logs.

```go
f, err := webmeta.New(webmeta.Config{
	ClientOptions: []httpx.Option{httpx.WithLogger(log.Default())},
})
if err != nil {
	panic(err)
}
defer f.Close()

// preview is the body of a service handler. It passes the request ctx
// (deadline, cancellation, trace) and returns the errs error as is: the
// server sends its code and safe message, the cause stays in the logs.
preview := func(ctx context.Context, rawURL string) (*webmeta.Metadata, error) {
	m, err := f.Fetch(ctx, rawURL)
	if err != nil {
		return nil, err
	}
	return m, nil
}

_, err = preview(context.Background(), "http://169.254.169.254/latest/meta-data/")
fmt.Println(errors.Is(err, httpx.ErrDestinationNotAllowed), errs.Code(err))
// Output: true InvalidArgument
```

In a real service, build the `Fetcher` in `main`, inject it into your
service struct, and close it on shutdown. Full server wiring is in
[`server/USAGE.md`](../server/USAGE.md).

## Advanced

### Many URLs at once

Use `FetchAll` for citation previews and similar best-effort work:

```go
f, err := webmeta.New(webmeta.Config{})
if err != nil {
	panic(err)
}
defer f.Close()

urls := []string{"https://go.dev/", "http://127.0.0.1/admin", "https://pkg.go.dev/"}
for _, r := range f.FetchAll(context.Background(), urls, 4) {
	if r.Err != nil {
		fmt.Println(r.URL, "skipped:", r.Err) // one failure does not stop the others
		continue
	}
	fmt.Println(r.URL, r.Metadata.Title)
}
```

- At most `concurrency` fetches run at once; 4 when it is 0 or less.
- It returns one `Result` per URL, in input order. Each has either
  `Metadata` or `Err`.
- A failed URL never cancels the others.
- Once `ctx` is done, no new fetch starts and the remaining results carry
  `ctx.Err()`.
- It returns after every started fetch finishes. No goroutine outlives it.

For other concurrent work, use `errgroup` with `SetLimit`.

### Fetching a trusted internal site

> **Security warning.** `Allow` opens holes in the SSRF guard. Every URL the
> Fetcher receives can then reach the allowed range, including URLs an
> attacker or an LLM chose. Never allow broad ranges such as `10.0.0.0/8`,
> `127.0.0.0/8` or `169.254.0.0/16`. Never allow anything on a Fetcher that
> handles user or LLM input. If you need an internal target, build a
> separate Fetcher for it, allow a single host prefix (`/32` or `/128`) and
> only the port it serves.

`Allow` alone is not enough: the port must also be in `Ports`. Setting
`Ports` replaces the 80/443 default, so list every port you need.

## Testing

`httptest` servers listen on loopback, which the guard refuses. Open exactly
that address and port, in tests only:

```go
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	_, _ = fmt.Fprint(w, `<title>Local page</title>`)
}))
defer srv.Close()

// TESTS ONLY: open exactly the test server's address and port.
ap := netip.MustParseAddrPort(srv.Listener.Addr().String())
f, err := webmeta.New(webmeta.Config{
	Allow: []netip.Prefix{netip.PrefixFrom(ap.Addr(), ap.Addr().BitLen())},
	Ports: []int{int(ap.Port())},
})
if err != nil {
	panic(err)
}
defer f.Close()

m, err := f.Fetch(context.Background(), srv.URL)
fmt.Println(m.Title, err)
// Output: Local page <nil>
```

- Keep this setup in a test helper (see `newLocalFetcher` in
  `webmeta_test.go`). Never copy it into production code.
- Also test that a default `Config{}` refuses your server, with
  `errors.Is(err, httpx.ErrDestinationNotAllowed)`.
- Assert on errors with `errors.Is` against the sentinels in the table
  above, and on status with `httpx.StatusCode(err)`.
- Test pure parsing with `webmeta.Parse` and a `strings.Reader`; no server
  is needed.
- Close the Fetcher and the server, and run `goleak.VerifyTestMain`.

## Observability

`webmeta` emits nothing of its own. The `httpx` client underneath emits the
standard OTel HTTP client spans and metrics through `otelx`, and logs
through the logger passed in `ClientOptions`. Errors carry the reasons
`INVALID_URL`, `NOT_HTML` and `DESTINATION_NOT_ALLOWED`, plus the generic
upstream reasons from the error table.

## Production checklist

- Build one `Fetcher` at startup, share it, and `Close` it on shutdown.
- Pass the request `ctx` to every `Fetch`.
- Pass your `*log.Logger` through `ClientOptions`.
- Keep `Allow` empty on any Fetcher that sees user or LLM input.
- Never try to inject a transport or proxy; `New` refuses it.
- Escape every returned value before rendering it.
- Bound `FetchAll` input size and set a small `concurrency`.

## Migrating from the old kitchen

`crawl` became `webmeta.Fetch`, and `parallel.RunWithConcurrency` over
crawls became `FetchAll`. See
[`docs/migration/data-utils.md`](../docs/migration/data-utils.md).
