# pubsub: package rules

The engine in this package (`Subscriber`) is shared by every backend:
`gcp`, `sqs`, `kafka`, `memory`, `outbox`. A change here changes delivery
for all of them. Read `doc.go` and `docs/design/messaging.md` first.

## Settlement contract

The handler result alone decides how a message is settled. Do not add other
ways to ack or nack a message.

| Handler result | What happens |
|---|---|
| `nil` | ack |
| error | retry in process, then nack |
| error wrapping `ErrPermanent` | `DeadLetter` hook, then ack |
| panic | recovered and treated as an error |

Delivery is at-least-once, so handlers must be idempotent. Never promise
exactly-once.

## Ordering

- Messages with the same key run in FIFO order, one at a time.
- After a keyed message's final nack, the messages of that key already
  queued in the subscriber are nacked (single and batch path).
- The key fence (`WithKeyFence`, default `KeyFenceAuto`) goes further: it
  nacks every other message of the key and awaits the redelivery of all
  nacked messages of the key, in arrival order, before the key runs again
  (or until the TTL). It advances only in queue order (`admit`); receive
  only `peek`s. `KeyFenceAuto` follows the source's `RedeliveryAdvisor`
  (Kafka/memory on, Pub/Sub ordered 5s, SQS FIFO and Pub/Sub unordered off).
- Never fence on the broker's last delivery (`DeliveryAttempt` ≥
  `Redelivery.MaxDeliveryAttempts`): that message goes to the broker's DLQ
  and will not come back, and a fence would burn the rest of the key into
  the DLQ.
- Batch isolation must skip later messages of a key whose earlier message
  failed, and must stop on a systemic failure (2 isolated failures in a row).
- Any change to concurrency, batching or nack handling needs tests that prove
  ordering still holds, including Kafka-style batches where messages polled
  together arrive before the nack. `memory.WithPrefetch()` plus the
  property tests in `review_test.go` (`TestKeyOrderUnderPrefetchAndFailures`,
  batch variant) must stay green.

## Lifecycle

- `Run(ctx)` drains in-flight work before it returns. No goroutine may
  outlive it; tests check this with goleak.
- Write timing tests with `testing/synctest`.

## Backends and shared packages

- Adapters implement `Source` and `Publisher` only. Engine behaviour (retry,
  ordering, dead-letter) stays in this package and is not copied into an
  adapter. An adapter tells the engine how its broker behaves through the
  optional interfaces instead: `RedeliveryAdvisor` (fence on/off, fence TTL,
  the broker's dead-letter limit), `ConsumerIdentifier` (consumer group or
  subscription for telemetry) and `KeyResumer` (paused keys on publish).
- `pubsub/gcp`: `RedeliveryAdvisor` is only correct when the Source knows
  the subscription's ordering and dead-letter settings. Examples and docs
  pass `gcp.WithSubscriptionConfig(ordered, maxDeliveryAttempts)`, because
  `roles/pubsub.subscriber` cannot read them (the fallback read needs
  `pubsub.subscriptions.get`). Keep the main `gcp` Example and the USAGE.md
  snippet showing it.
- Use the canonical kitchen packages:
  - backoff: `timex.Backoff`
  - errors: `errs`; a broker failure is UNAVAILABLE
  - logging: `log`
  - telemetry: the OTel messaging semantic conventions, through `otelx`
