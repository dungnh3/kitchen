// Package httpx is a small HTTP client for calling JSON APIs.
//
// A [Client] is immutable and safe for concurrent use: build it once with
// [New] and share it. Each call builds its own [Request]:
//
//	var out Resp
//	err := c.Post("/v1/items").Bearer(tok).JSON(in).Do(ctx, &out)
//
// Non-2xx responses return a [*StatusError] that keeps (a bounded prefix of)
// the body. Requests are traced and measured with otelhttp. Retries are
// opt-in ([WithRetry]) and only apply to idempotent requests.
package httpx
