# Migration: messaging

From `github.com/vulcanlabsvn/kitchen/{pubsub,syncmsg,syncbrokers,coordinators,scheduler,queue}`
to `github.com/dungnh3/kitchen/{pubsub,lease}`. Design:
[docs/design/messaging.md](../design/messaging.md).

This is a clean break: there are no compatibility shims. Handlers change shape
(they now take a `ctx` and return an `error`), so the migration is manual, but
each step below is mechanical.

## 1. Modules

| Old module | New import | Module |
|---|---|---|
| `kitchen/pubsub` (interfaces) | `github.com/dungnh3/kitchen/pubsub` | core |
| `kitchen/pubsub/gcp`, `gcp/multi_async`, `gcp/multi_batch` | `github.com/dungnh3/kitchen/pubsub/gcp` + `pubsub.NewSubscriber` / `NewBatchSubscriber` | `pubsub/gcp` |
| `kitchen/pubsub/kafka_sarama` | `github.com/dungnh3/kitchen/pubsub/kafka` (franz-go) | `pubsub/kafka` |
| `kitchen/pubsub/sqs` (aws-sdk-go v1) | `github.com/dungnh3/kitchen/pubsub/sqs` (aws-sdk-go-v2) | `pubsub/sqs` |
| `kitchen/syncmsg`, `syncmsg/gcp` | `pubsub.NewBatchSubscriber` + `pubsub/gcp` | core + `pubsub/gcp` |
| `kitchen/syncbrokers` | `pubsub/reply` + `pubsub/reply/redis` | core + submodule |
| `kitchen/coordinators` | `lease` + `lease/redis` | core + submodule |
| `kitchen/scheduler` | removed: use `lease.Elector` + `time.Ticker`, Cloud Scheduler → Pub/Sub, or a cron library | — |
| `kitchen/queue` | removed (it was empty) | — |
| — (new) | `pubsub/outbox` + `pubsub/outbox/pgx` (transactional outbox) | core + submodule |
| — (new) | `pubsub/memory` (in-memory broker for tests) | core |

Dependencies: `cloud.google.com/go/pubsub` v1 → `cloud.google.com/go/pubsub/v2`
(v1 support ends 2026-12-31); `IBM/sarama` → `twmb/franz-go`; `aws-sdk-go` v1
(end of support 2025-07-31) → `aws-sdk-go-v2`.

## 2. Symbols

### Publishing

| Old | New |
|---|---|
| `gcp.NewPublisher(c, opts...)` (`*pubsub.Client` v1) | `gcp.NewPublisher(c)` (`*pubsub.Client` from v2) |
| `gcp.WithDefaultTopic(t)`, `PublishDefault`, `PublishObjectDefault` | removed: pass the topic to `Publish`; build JSON with `pubsub.NewJSON(v)` |
| `gcp.WithMessageOrdering()` | removed: ordering is always enabled on topic handles |
| `Publish(ctx, topic, []byte)` | `Publish(ctx, topic, &pubsub.Message{Data: b})` |
| `PublishWithAttributes(ctx, topic, b, attrs)` | `Publish(ctx, topic, &pubsub.Message{Data: b, Attributes: attrs})` |
| `PublishWithOptions(ctx, topic, b, pubsub.WithKey(k), pubsub.WithAttribute(..))` | `Publish(ctx, topic, &pubsub.Message{Data: b, Key: k, Attributes: ...})` |
| `pubsub.PublishOption`, `PublishOptions`, `WithAttributes`, `WithAttribute`, `WithKey` | fields of `pubsub.Message` |
| `AsyncPublisher`, `PublishAsync`, `PublishResult` | removed: `Publish(ctx, topic, msgs...)` sends many messages in one call and waits once |
| `ResumePublish(topic, key)` | `(*gcp.Publisher).ResumeKey(topic, key)` / `pubsub.KeyResumer` |
| `ErrNoDefaultTopic`, `ErrPublisherClosed` | `pubsub.ErrClosed`; an empty topic is an error |
| `Publisher.Close()` | `Close(ctx)` |
| `syncmsg/gcp` publisher | `gcp.Publisher` (trace context is injected by default) |

### Subscribing

| Old | New |
|---|---|
| `pubsub.SubscriberHandler func(SubscriberMessage)` | `pubsub.Handler func(ctx, *pubsub.Message) error` |
| `pubsub.SubscriberMultiBatchHandler func([]SubscriberMessage)` | `pubsub.BatchHandler func(ctx, []*pubsub.Message) error` |
| `syncmsg.Handler[T]`, `Decoder[T]`, `Message[T]` | `pubsub.BatchHandler`; typed single handlers via `pubsub.JSON[T]` |
| `SubscriberMessage.ID()/Value()/Key()/Topic()/Timestamp()/Metadata()/ReceiveCount()` | `Message.ID/Data/Key/Topic/PublishTime/Attributes/DeliveryAttempt` |
| `Commit(meta)` | return `nil` |
| `(*gcp.SubscriberMessage).Nack()` (type assertion) | return an error |
| "always commit even on error" | return `pubsub.Permanent(err)` (dead-letter hook, then ack) |
| `multi_async.NewSubscriber(..., WithWorkerNum(n))` | `pubsub.NewSubscriber(gcp.NewSource(c, sub), h, pubsub.WithConcurrency(n))` |
| `multi_batch.NewSubscriber(..., WithWorkerNum, WithBufferSize, Config)` | `pubsub.NewBatchSubscriber(src, h, pubsub.WithConcurrency(n), pubsub.WithBatch(size, flush))` |
| `syncmsg.Config{MaxBufferSize, FlushInterval, WorkerNum, MaxRetries, RetryBaseDelay, HandlerTimeout, ShutdownTimeout}` | `WithBatch`, `WithConcurrency`, `WithRetry`, `WithHandlerTimeout`, `WithShutdownTimeout` |
| `syncmsg.Config.PreserveOrdering` | always on per key; unkeyed messages run in parallel |
| `syncmsg.ErrPermanentDecodeFailure` | `pubsub.ErrPermanent` / `pubsub.Permanent(err)` |
| `syncmsg.MetricsCollector`, `TraceExtractor` | built-in OpenTelemetry (`WithTelemetry(WithTracerProvider, WithMeterProvider, WithPropagator)`) |
| `syncmsg/gcp.NewGCPSource(sub, WithMaxExtension, WithMaxOutstandingMessages, WithMaxOutstandingBytes)` | `gcp.NewSource(client, subID, gcp.WithMaxExtension, gcp.WithMaxOutstanding)` |
| `Subscriber.Run(ctx)` + `Close(ctx)` | `Run(ctx)` only: cancel ctx to stop; it returns after draining |
| `kafka_sarama.NewConsumerGroup(...)`, `Handler(ctx, *ConsumerMessage) error` | `pubsub.NewSubscriber(kafka.NewSource(group, topics, kafka.WithClientOptions(kgo.SeedBrokers(...))), h)` |
| `sqs.NewSubscriber(...)` | `pubsub.NewSubscriber(sqs.NewSource(client, queueURL), h)` |

### Request/reply, leases

| Old | New |
|---|---|
| `syncbroker.Broker.Wait(ctx, id, timeout)` | `w, _ := broker.Expect(ctx, id)` **before** sending the request, then `w.Wait(ctx)` (timeout = ctx deadline) |
| (worker publishes to `reply:<id>` itself) | `broker.Reply(ctx, id, payload)` |
| caller-chosen IDs | `reply.NewID()` (128-bit random); IDs are validated |
| `coordinators.NewCoordinator(...)`, `SetCallbacks(onAcquired, onLost)`, `Start`, `Stop` | `lease.NewElector(leaseredis.New(rdb), name).Run(ctx, lead)`; lead's ctx is cancelled when the lease is lost |
| one shard per pod | one `Elector` (or `Locker.Acquire`) per shard name |

## 3. Behaviour changes

Wire format: Pub/Sub message data, attributes and ordering keys are unchanged.
New attributes are added by default: W3C `traceparent` (and `tracestate` when
present). Kafka attributes become record headers. SQS: attributes become
message attributes, and non-UTF-8 bodies are base64-encoded with the
`kitchen-body-encoding: base64` attribute.

Semantics:

- Handler result settles the message: `nil` → ack, error → in-process retry
  (3 retries, backoff 1s doubling to 30s with jitter, 60s per attempt) → nack;
  `Permanent` → dead-letter hook → ack. Panics are recovered and retried.
- Keyed messages are processed one at a time per key, in order. Different keys
  and unkeyed messages are processed in parallel (default concurrency 10,
  1000 messages in flight).
- Publishing a keyed message after a failed publish of the same key fails with
  `ErrKeyPaused` until `ResumeKey`. The old `gcp.Publisher` resumed keys
  automatically.
- Batch handlers report per-message failures with `*pubsub.BatchError`.
  Messages that succeeded are no longer re-run when a sibling fails.
- `Run` returns only after every received message is settled and every
  goroutine has exited; `Close` on subscribers is gone.
- Default flush interval for batches is 1s (syncmsg: 3s); handler timeout 60s
  (syncmsg: 2m, documented as 5m).
- OpenTelemetry spans and metrics are emitted by default via the global
  providers; consumers continue the producer's trace.

### Bugs fixed (old behaviour that is gone)

pubsub/gcp, multi_async, multi_batch:

- `multi_async` `Run` could deadlock on shutdown (unbuffered error channel,
  two senders, send-on-closed-channel panic in the receive callback); `Run`
  used a value receiver, so `s.ctx` was lost. → One engine with a bounded drain.
- `multi_async` lost ordering-key order by acking the GCP callback before
  processing. → Per-key FIFO in the engine.
- `multi_batch` retried only on panic and re-ran already-acked messages; its
  handler goroutine was orphaned on cancellation; the final flush selected on a
  done context and dropped the batch; the flusher could close the channel while
  a receive callback still sent on it; `ErrorChannelSize`/`MaxRetries` were
  reused for unrelated purposes. → Retries on error, per-message results,
  drain before return, no channel closing races.
- Nack was not part of the interface (type assertion on `*gcp.SubscriberMessage`).
  → The handler result decides; no type assertions.
- `Metadata() any` returned different types per backend. → `Attributes map[string]string`.
- No trace propagation in the GCP publisher. → Injected by default.
- Two GCP publishers with opposite ordering-key resume policies. → One
  publisher; keys stay paused until `ResumeKey`.
- Topic `""` without a default topic used to panic (later `ErrNoDefaultTopic`).
  → Empty topic is an error.

syncmsg:

- The GCP source claimed `fn` was called synchronously; the v1 client calls it
  concurrently, so `PreserveOrdering` did not preserve order. → The Source
  contract is per-key sequential and the engine enforces per-key FIFO.
- Trace context was taken from the first message of a batch only. → One span
  per batch with a link to every message's producer context.
- After exhausting retries, split messages each got a full new retry budget.
  → Each runs once alone.
- `HandlerTimeout` documented as 5m but defaulted to 2m; the Handler doc said
  "called once per batch after all retry attempts". → Documented defaults
  match the code.
- `GCPSource.Close` was a no-op. → No Close; `Run` owns the lifecycle.

kafka_sarama:

- `ConsumerGroup.Run` and `BatchConsumerGroup.Run` deadlocked on shutdown.
- `Commit` had an inverted type check (metadata always empty).
- N workers marked offsets out of order, committing past unprocessed records
  (at-most-once on crash, no per-partition order). → Only the contiguous
  settled prefix is committed; per-key order is kept.
- Handler errors were only logged (no retry). → Retry, then in-process
  redelivery in key order, then dead-letter hook.
- Batch consumer default worker count 0 (nothing processed); one ticker shared
  by every claim; send on an unbuffered channel under a mutex; buffering across
  rebalances committed through stale sessions; `Close` was a no-op. → Revoked
  partitions are drained and committed before the rebalance completes.

sqs:

- `Run` deadlocked on shutdown. `WithMaxNumMsg` was ignored (1 message per
  poll). A single receive error killed the subscriber. Empty-body messages were
  skipped and never deleted. No visibility extension, no batch delete, `Close`
  was a no-op. → 10 per poll, backoff on errors, empty bodies delivered,
  visibility heartbeats, batched deletes, drain on shutdown.

syncbrokers:

- `sync.Pool` recycling could steal or cross results between requests, and
  `Wait` could hang forever holding a semaphore slot; cleanup/Stop recycled
  entries without notifying waiters; `Stop` double-closed; the listener gave up
  after 5 reconnects for the life of the process; `ReplyKeyPattern` was ignored;
  a reply published before `Wait` was lost; every reply fanned out to every
  instance. → Per-ID Redis list with TTL, BLPOP, no pooling, no fan-out.

coordinators:

- No fencing token; lost/acquired callbacks raced as goroutines; a transient
  Redis error dropped the shard while the old lease stayed live; `EXPIRE` in
  whole seconds (sub-second TTL became 0); `Stop` did not release; `Start`
  slept 0–4s; `SetCallbacks` was unsynchronized. → Fencing tokens, owner-checked
  Lua renew/release with `PEXPIRE`, leader ctx cancelled before the lease can
  expire, leadership never overlaps locally, release after the leader returns.

scheduler (removed): one panic stopped all processors; zero processors blocked
forever; ran on every replica.

## 4. Recipes

Single-message subscriber (old `multi_async`):

```go
// old
sub := multi_async.NewSubscriber(client, "orders-sub", func(m pubsub.SubscriberMessage) {
    defer m.Commit(nil)
    if err := handle(m.Value()); err != nil { log.Error(err) }
}, multi_async.WithWorkerNum(20))

// new
src, err := gcp.NewSource(client, "orders-sub")
sub, err := pubsub.NewSubscriber(src, func(ctx context.Context, m *pubsub.Message) error {
    return handle(ctx, m.Data) // nil = ack, error = retry/nack, pubsub.Permanent(err) = dead-letter
}, pubsub.WithConcurrency(20), pubsub.WithLogger(logger))
go sub.Run(ctx) // cancel ctx to stop; Run returns after drain
```

Batch subscriber (old `multi_batch` / `syncmsg`):

```go
sub, err := pubsub.NewBatchSubscriber(src, func(ctx context.Context, ms []*pubsub.Message) error {
    failed := insertAll(ctx, ms) // map[index]error
    if len(failed) > 0 {
        return &pubsub.BatchError{Failed: failed}
    }
    return nil
}, pubsub.WithBatch(100, time.Second), pubsub.WithConcurrency(10))
```

Publishing (sed-able for the common call shapes):

```bash
# PublishWithAttributes(ctx, topic, b, attrs) -> Publish(ctx, topic, &pubsub.Message{Data: b, Attributes: attrs})
gofmt -r 'p.PublishWithAttributes(a, b, c, d) -> p.Publish(a, b, &pubsub.Message{Data: c, Attributes: d})' -w .
# Publish(ctx, topic, b) -> Publish(ctx, topic, &pubsub.Message{Data: b})  (review: only for []byte arguments)
gofmt -r 'p.Publish(a, b, c) -> p.Publish(a, b, &pubsub.Message{Data: c})' -d .
```

Note: `Publish` now returns `([]string, error)`; update `err := p.Publish(...)`
to `_, err := p.Publish(...)`.

Pub/Sub configuration checklist:

- Enable message ordering on subscriptions that receive keyed messages.
- Configure a dead-letter topic with `maxDeliveryAttempts` (≥ 5) on each
  subscription; the engine nacks after its in-process retries.
- Keep `gcp.WithMaxExtension` above `HandlerTimeout × (retries + 1)` plus
  backoff.
