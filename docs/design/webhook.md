# Design: `webhook`, `webhook/delivery` and the zen webhook commands

Status: accepted. The owner chose the recommended option of every question
(wire format, implementation, receiving, sending, zen, dedup failure).
Migration notes are not needed: kitchen had no webhook package before.

## 1. Why

Services both receive webhooks (payment providers, other teams) and send
them (to customers' endpoints). Each side is easy to get subtly wrong:

- receiving: comparing signatures with `==`, forgetting to sign the
  timestamp (replays), parsing the body before verifying it, no body limit,
  running a handler twice when the sender retries;
- sending: losing a webhook when the process dies between the commit and
  the POST, retrying in a tight loop, following redirects or resolving a
  customer URL to `169.254.169.254` (SSRF), no way to rotate secrets.

`webhook` is the canonical package for the concern (D7), so these are solved
once.

## 2. API

### Core: `webhook`

```go
signer, _ := webhook.NewSigner(secret)                 // whsec_ (v1) or whsk_ (v1a)
h, _ := signer.Headers(id, time.Now(), body)            // webhook-id, -timestamp, -signature

verifier, _ := webhook.NewVerifier(webhook.VerifierConfig{Secrets: []string{secret}})
mw, _ := webhook.NewMiddleware(webhook.MiddlewareConfig{
	Verifier: verifier,
	Dedup:    dedup,  // optional: webhook.NewRedisDedup(rdb, webhook.RedisDedupConfig{})
	Errors:   errHandler,
})
msg, ok := webhook.FromContext(r.Context())             // ID, Timestamp, Body
```

- `Verifier` is an interface (`Verify(ctx, header, body) (Message, error)`);
  `StandardVerifier` is the only implementation shipped.
- `Dedup` / `Ticket` are interfaces; `RedisDedup` is the implementation.
- `GenerateSecret`, `GenerateKeyPair`, `NewMessageID` create secrets and
  ids.
- Config structs throughout (one style per package).

### `webhook/delivery`

```go
args, _ := delivery.NewArgs(endpointID, "invoice.paid", payload)
client.InsertTx(ctx, tx, args, nil)                     // jobs, in the business tx

worker, _ := delivery.NewWorker(delivery.Config{Endpoints: endpoints})
jobs.Register(reg, worker)
```

- `Args` is a `jobs.Args` (kind `kitchen.webhook.delivery`) with insert
  defaults: queue `webhooks`, 7 attempts, unique key
  `webhook:<endpoint>:<message>` kept until the row is deleted.
- `Endpoints` is the business lookup (URL, secrets, disabled), called at
  every attempt. Secrets are never stored in the job row.
- `Config.OnResult` reports every attempt (`Delivered`, `Retrying`,
  `Failed`, `Gone`, `Cancelled`).

### `jobs.RetryAfter`

`jobs` retried with the client-wide exponential backoff only. Delivery needs
its own schedule, so `jobs` gained `RetryAfter(d, err)`: a failure that is
retried after `d` and counts as an attempt (unlike `Snooze`). It is additive
and useful to any kind with its own schedule.

## 3. Decisions

| # | Decision | Chosen | Alternatives | Why |
|---|---|---|---|---|
| W1 | Wire format | Standard Webhooks: `webhook-id`, `webhook-timestamp`, `webhook-signature`; `v1,` HMAC-SHA256 over `id.timestamp.body`; `whsec_` secrets; several signatures per request; `v1a` ed25519 with `whsk_`/`whpk_` | A custom HMAC header | An open spec used by many providers, with libraries for every language on the receiving side. The timestamp is signed (replay bound) and rotation is built in. |
| W2 | Implementation | Written from the spec with stdlib crypto only; tested against the spec's test vector | Wrap the Standard Webhooks Go library | About 300 lines; no new dependency in core; full control of fail-closed behaviour, ed25519, limits and errors. Lint denies the standard-webhooks and svix libraries. |
| W3 | Receiving | HTTP middleware for `server` plain routes: body limit (1 MiB) before verifying, raw body kept for the handler, constant-time comparison, ±5 minutes tolerance, every failure 401; `Verifier` interface for other schemes, only the standard one shipped | Ship Stripe and GitHub verifiers; no dedup | Small and safe. Providers with other schemes plug in through `Verifier` when a service needs them. |
| W4 | Dedup | Optional, by `webhook-id`, in Redis: `SET NX` claim (2 min) → `done` marker (72h) on 2xx, released on non-2xx or panic; duplicates of a handled id get 200, of an id in progress 409 | Single `SETNX` marker; no dedup | A plain marker set before the handler would swallow the retry of a failed delivery. Claims carry a token, so a late release cannot drop another request's claim. |
| W5 | Dedup failure | Fail closed: Redis error → 503, sender retries | Fail open: handle anyway | Never handle twice; the sender's retries make the delay harmless. Deduplication itself is opt-in (nil `Dedup`). |
| W6 | Sending | `webhook/delivery` on `jobs`: one job per message and endpoint, unique, enqueued in the business transaction; `httpx` with the SSRF guard always on, https only, no redirects | Use the jobs client's backoff; sign only, no worker | Durable (outbox semantics from `jobs`), customer URLs cannot reach internal addresses, and the retry window (5s, 5m, 30m, 2h, 5h, 10h) suits receivers that are down for hours. |
| W7 | Responses | 2xx done; 410 cancels; every other status and network error retried; `Retry-After` raises the next delay up to the schedule's last entry; invalid, disabled, deleted or SSRF-refused endpoints cancel | Treat 4xx as final | Receivers often answer 4xx during deploys or misconfiguration that gets fixed; the spec only defines 410 as "stop". An SSRF refusal will not fix itself by retrying. |
| W8 | Placement | Both in the core module: `jobs`, `httpx` and `redisx` are core, so no submodule is needed | A `webhook/delivery` submodule; inside `jobs` | Core never imports a submodule, and no heavy dependency is added. |
| W9 | zen | `zen add webhook <name>` (inbound handler, middleware, secret in config and ENV.md, public route) and `zen new service -with webhooks` (outbound worker, needs `-with jobs`) | `zen add webhook` only | Both directions get the same wiring, including the SSRF settings and the auth public route. |

## 4. zen

- `zen add webhook <name>` writes `internal/inbound/<name>.go` (a
  `server.Service` whose only route is `POST /webhooks/<name>` behind
  `webhook.NewMiddleware`) and its test; the first one also writes
  `internal/inbound/inbound.go`. It adds `<NAME>_WEBHOOK_SECRET` (required,
  parsed at start-up) to config, `docs/ENV.md` and the Makefile's local
  settings, and the route to `webhookRoutes` in `app.go`, which the
  authenticator treats as public; rate limits still apply. With `-with
  redis` the receiver deduplicates through Redis.
- `zen new service -with webhooks` (needs `-with jobs`) writes
  `internal/outbound`: `Deliveries` builds one job per endpoint with one
  message id and a `{type, timestamp, data}` payload, `Endpoints` is the
  lookup to implement, and the jobs runner registers the worker on its own
  `webhooks` queue (`WEBHOOK_DELIVERY_WORKERS`, `_TIMEOUT`, `_ALLOW_HTTP`,
  the last refused in production).
- Services generated before these markers existed get the "add the change
  by hand" error from `zen add webhook`.

## 5. Out of scope

Endpoint and subscription storage, secret storage and the management UI are
business code. The service implements `delivery.Endpoints` and, for logs and
auto-disabling, `Config.OnResult`.

## 6. Security notes

- Secrets need their prefix; a symmetric secret has 16 to 1024 bytes. A
  verifier refuses a `whsk_` key and a signer a `whpk_` key.
- Message ids are 1–256 printable ASCII bytes without spaces or dots (the
  dot separates the signed parts).
- Refusals do not say which check failed; the cause is logged at debug.
- `delivery.Config.HTTPOptions` cannot remove the SSRF guard: `httpx` refuses
  `WithTransport` together with `WithSSRFGuard`.
