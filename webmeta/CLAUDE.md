# webmeta: package rules

`webmeta` fetches URLs that it must treat as hostile, such as URLs an LLM or
an end user supplies. Security comes before extraction quality.

## Fetching

All fetches go through `httpx` with the SSRF guard on. The fetcher must:
- dial only public addresses, on ports 80 and 443 only;
- check every address after DNS resolution and on every redirect;
- ignore proxies;
- never follow a redirect from https to http;
- cap the response body size;
- apply a timeout.

Never build an `http.Client` here, and never add an option that turns the
guard off.

## Parsing

- Parse HTML only with `x/net/html`, which is the canonical parser (D7).
- Treat every extracted value (title, description, image URL, icon URL) as
  untrusted. Resolve relative URLs against the final URL, and allow only
  http and https URLs to be returned.

## Errors and tests

Every error is an `errs` error:
- `httpx.ErrDestinationNotAllowed` when an address is refused;
- the `httpx` status mapping for non-2xx responses;
- `ErrInvalidURL`;
- `ErrNotHTML`.

Tests must cover these attack cases:
- a private IP;
- DNS rebinding;
- a redirect to localhost;
- a huge body;
- a non-HTML content type.
