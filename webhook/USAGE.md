# webhook

`webhook` signs, verifies and receives webhooks in the
[Standard Webhooks](https://www.standardwebhooks.com) format, and
`webhook/delivery` sends them durably on top of [`jobs`](../jobs/USAGE.md).
It is the canonical webhook package (D7 row "Webhooks" in
[decisions](../docs/decisions.md)): do not hand-roll HMAC signatures or
pull in another webhook library. Design: [docs/design/webhook.md](../docs/design/webhook.md).

## Install

```bash
go get github.com/dungnh3/kitchen
```

Both packages are in the core module; there is no submodule.

## Quick start

### Receive

Wrap the handler with the middleware. Only verified requests reach it:

```go
verifier, err := webhook.NewVerifier(webhook.VerifierConfig{
	Secrets: []string{cfg.BillingWebhookSecret}, // "whsec_..."
})
verified, err := webhook.NewMiddleware(webhook.MiddlewareConfig{
	Verifier: verifier,
	Errors:   errHandler, // the server's errs.Handler
	Logger:   logger,
})
mux.Handle("POST /webhooks/billing", verified(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	msg, _ := webhook.FromContext(r.Context())
	body, _ := io.ReadAll(r.Body) // the raw body, as signed
	// act on it; answer 2xx only when done
	w.WriteHeader(http.StatusNoContent)
})))
```

The defaults are safe:

- the body is read up to 1 MiB; a larger one gets 413 `PAYLOAD_TOO_LARGE`
  before anything is verified;
- `webhook-id`, `webhook-timestamp` and `webhook-signature` are all
  required; the timestamp must be within 5 minutes of the clock, in either
  direction, which bounds replays of a captured request;
- signatures are compared in constant time (`hmac.Equal`, `ed25519.Verify`);
  at most 10 signatures per request are tried;
- every failure is 401 `WEBHOOK_SIGNATURE_INVALID` with no hint of which
  check failed; the precise cause (`ErrMissingHeader`,
  `ErrTimestampTolerance`, `ErrNoMatchingSignature`) is only logged, at
  debug level;
- an error from a custom `Verifier` is also turned into that 401.

### Send

Enqueue a delivery in the transaction that changes the business data, so the
webhook is sent if and only if the change commits:

```go
args, err := delivery.NewArgs(endpointID, "invoice.paid", payload)
_, err = jobsClient.InsertTx(ctx, tx, args, nil)
```

The worker process registers the delivery worker in its jobs registry:

```go
worker, err := delivery.NewWorker(delivery.Config{
	Endpoints: endpoints, // your lookup: URL, secrets, disabled
	Logger:    logger,
})
defer worker.Close(ctx)
err = jobs.Register(reg, worker)
client, err := jobs.NewClient(drv, jobs.WithRegistry(reg),
	jobs.WithQueues(jobs.Queue{Name: "default", Workers: 10},
		jobs.Queue{Name: delivery.DefaultQueue, Workers: 10}))
```

Delivery defaults:

- one job per message and endpoint in the `webhooks` queue, unique on
  `webhook:<endpoint>:<message id>` (enqueueing twice sends once);
- the SSRF guard of `httpx` is always on: only public addresses on ports 80
  and 443, checked after DNS resolution; `https://` URLs only;
- redirects are not followed; a 3xx counts as a failure;
- 2xx completes the job; 410 Gone cancels it; anything else is retried
  after 5s, 5m, 30m, 2h, 5h and 10h (7 attempts over about 17.6 hours,
  ±10% jitter), and the job is discarded after the last one;
- a `Retry-After` from the endpoint raises the next delay, capped at the last
  entry of the schedule;
- each attempt signs again with a fresh timestamp and the same message id.

## Configuration

`webhook.VerifierConfig`:

| Field | Default | When to change |
|---|---|---|
| `Secrets` | required | list two during a rotation |
| `Tolerance` | 5 minutes | rarely; a larger one widens the replay window |
| `Now` | `time.Now` | tests |

`webhook.MiddlewareConfig`:

| Field | Default | When to change |
|---|---|---|
| `Verifier` | required | a provider with its own scheme |
| `MaxBodyBytes` | 1 MiB | senders with large payloads |
| `Dedup` | off | handlers that are not idempotent |
| `Errors` | `errs.WriteHTTP` | pass the server's `errs.Handler` |
| `Logger` | `log.Default()` | always pass the service's |

`webhook.RedisDedupConfig`:

| Field | Default | When to change |
|---|---|---|
| `Prefix` | `webhook:dedup:` | one per sender, so message ids cannot collide |
| `TTL` | 72h | keep it longer than the sender's retry window |
| `LockTTL` | 2 minutes | handlers that take longer |

`delivery.Config`:

| Field | Default | When to change |
|---|---|---|
| `Endpoints` | required | |
| `Schedule` | 5s, 5m, 30m, 2h, 5h, 10h | a different retry window (also set `MaxAttempts` at insert) |
| `Timeout` | 15s per attempt | slow receivers |
| `SSRF` | public addresses, ports 80 and 443 | an endpoint on another port |
| `AllowHTTP` | false | local development only |
| `HTTPOptions` | none | transport tuning; cannot disable the SSRF guard |
| `OnResult` | none | delivery logs, disabling failing endpoints |
| `Logger`, `MeterProvider`, `TracerProvider` | `log.Default()`, OTel globals | |

## Integration with kitchen

- **server**: mount the route in `RegisterHTTP` (`server.HTTPRegistrar`). It
  gets request info, logging, tracing and recovery from the server:

  ```go
  func (b *Billing) RegisterHTTP(mux *http.ServeMux) {
  	mux.Handle("POST /webhooks/billing", b.verified(http.HandlerFunc(b.handle)))
  }
  ```

- **auth**: with `server.WithAuth`, list the route pattern
  (`"POST /webhooks/billing"`) in `auth.Config.Public`. The signature
  authenticates the sender; the bearer check would refuse every webhook.
- **errs**: refusals are AIP-193 errors rendered by `MiddlewareConfig.Errors`:
  401 `WEBHOOK_SIGNATURE_INVALID`, 413 `PAYLOAD_TOO_LARGE`, 409
  `REQUEST_IN_PROGRESS`, 503 `DEPENDENCY_UNAVAILABLE`. The reasons are
  localized by `i18n`.
- **redisx**: `NewRedisDedup` takes the client from `redisx.Open`.
- **jobs**: deliveries are ordinary jobs: `InsertTx` in the business
  transaction, retries through `jobs.RetryAfter`, discarded jobs kept for
  inspection, the jobs spans and metrics.
- **httpx**: every POST goes through `httpx` with the SSRF guard; `httpx`
  traces it and redacts query strings from errors.
- **otelx**: `kitchen.webhook.delivery.attempts{kitchen.webhook.outcome,
  http.response.status_code}` counts attempts.

## Advanced

### Rotating a secret

The sender signs with both secrets for a while; the receiver accepts either:

```go
signer, err := webhook.NewSigner(oldSecret, newSecret)   // sender
verifier, err := webhook.NewVerifier(webhook.VerifierConfig{
	Secrets: []string{oldSecret, newSecret},               // receiver
})
```

With `delivery`, return both from `Endpoints` (`Endpoint.Secrets`); the next
attempt uses them.

### Asymmetric signatures

`GenerateKeyPair` returns an ed25519 secret key (`whsk_`, signs, `v1a`) and
public key (`whpk_`, verifies). Publish the public key; receivers cannot
forge messages with it. A verifier refuses a `whsk_` secret and a signer
refuses a `whpk_` key, so the two cannot be mixed up.

### Running the handler once per message

Senders retry until they get a 2xx, so a message can arrive more than once.
Make the handler idempotent, or give the middleware a `Dedup`:

```go
dedup, err := webhook.NewRedisDedup(rdb, webhook.RedisDedupConfig{Prefix: "webhook:billing:"})
verified, err := webhook.NewMiddleware(webhook.MiddlewareConfig{Verifier: verifier, Dedup: dedup})
```

- a message already handled gets 200 without running the handler;
- a message being handled by another request gets 409, and the sender
  retries;
- a handler that answers non-2xx or panics releases the id, so the retry
  runs it again;
- if Redis is down, the request gets 503 (fail closed) and the sender
  retries later.

### Other providers

A provider with its own scheme (Stripe, GitHub) plugs in by implementing
`webhook.Verifier`. Return an error wrapping `webhook.ErrSignatureInvalid`
for every failure and a `Message` whose `ID` is the provider's delivery id
(used by `Dedup`).

### Delivery logs and failing endpoints

`OnResult` runs after every attempt with the outcome (`Delivered`,
`Retrying`, `Failed`, `Gone`, `Cancelled`), the HTTP status and the error.
Store it for a delivery log, and disable the endpoint on `Gone` or after
several `Failed` results; the next attempts to a disabled endpoint are
cancelled.

### Fan-out

One event to several endpoints keeps one message id:

```go
args, err := delivery.NewArgs(endpoints[0], "invoice.paid", payload)
params := []jobs.InsertParams{{Args: args}}
for _, ep := range endpoints[1:] {
	params = append(params, jobs.InsertParams{Args: args.To(ep)})
}
_, err = jobsClient.InsertManyTx(ctx, tx, params)
```

## Testing

- The verifier is tested against the specification's test vector;
  `ExampleSigner_Headers` shows it.
- In tests, sign requests with `Signer.Headers` and serve them with
  `httptest`; use `miniredis` for `RedisDedup`.
- For `delivery`, run the worker on `jobstest.New()` and an `httptest`
  receiver. The SSRF guard refuses loopback, so tests pass
  `SSRF: httpx.SSRFConfig{Ports: []int{port}, Allow: loopback}` and
  `AllowHTTP: true`; never do that in production.
- `Worker.Work` can be called directly with a `jobs.Job[delivery.Args]` to
  check one attempt's outcome.
