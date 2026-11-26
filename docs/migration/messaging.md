# Migration: messaging

From the old library's `kitchen/{pubsub,syncmsg,syncbrokers,coordinators,scheduler,queue}`
packages to `github.com/dungnh3/kitchen/{pubsub,lease}`. Design:
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
| `syncmsg.Handler[T]`, `Decoder[T]`, `Message[T]` | `pubsub.JSONBatch[T](func(ctx, []T, []*pubsub.Message) error)` (undecodable messages are dead-lettered per message); typed single handlers via `pubsub.JSON[T]` |
| `SubscriberMessage.ID()/Value()/Key()/Topic()/Timestamp()/Metadata()/ReceiveCount()` | `Message.ID/Data/Key/Topic/PublishTime/Attributes/DeliveryAttempt` |
| `Commit(meta)` | return `nil` |
| `(*gcp.SubscriberMessage).Nack()` (type assertion) | return an error |
| "always commit even on error" | return `pubsub.Permanent(err)` (dead-letter hook, then ack) |
| `multi_async.NewSubscriber(..., WithWorkerNum(n))` | `pubsub.NewSubscriber(gcp.NewSource(c, sub), h, pubsub.WithConcurrency(n))` |
| `multi_batch.NewSubscriber(..., WithWorkerNum, WithBufferSize, Config)` | `pubsub.NewBatchSubscriber(src, h, pubsub.WithConcurrency(n), pubsub.WithBatch(size, flush))`; a handler that called `m.Commit(nil)` on bad messages returns `pubsub.Permanent` for them in a `*pubsub.BatchError` (or uses `pubsub.JSONBatch`) and sets `pubsub.WithDeadLetter` |
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
| ad-hoc `SetNX` lock + `Del` | `lease.Do(ctx, locker, name, ttl, fn)`; `lease.ErrNotHeld` (errs Aborted, 409) when busy |

## 3. Behaviour changes

Wire format: Pub/Sub message data, attributes and ordering keys are unchanged.
New attributes are added by default: W3C `traceparent` (and `tracestate` when
present). Kafka attributes become record headers. SQS: attributes become
message attributes, and bodies that are not valid UTF-8 within the SQS
character set (binary, protobuf, control characters) are base64-encoded with
the `kitchen-body-encoding: base64` attribute (see §3.1).

Semantics:

- Handler result settles the message: `nil` → ack, error → in-process retry
  (3 retries, backoff 1s doubling to 30s with jitter, 60s per attempt) → nack;
  `Permanent` → dead-letter hook → ack. Panics are recovered and retried.
- Keyed messages are processed one at a time per key, in order. Different keys
  and unkeyed messages are processed in parallel (default concurrency 10,
  2 × concurrency messages in flight; see §3.1).
- Publishing a keyed message after a failed publish of the same key fails with
  `ErrKeyPaused` until `ResumeKey`. The old `gcp.Publisher` resumed keys
  automatically.
- Batch handlers report per-message failures with `*pubsub.BatchError`.
  Messages that succeeded are no longer re-run when a sibling fails.
- `Run` returns only after every received message is settled and every
  goroutine has exited. `Subscriber.Close(ctx)` stops it and waits (§3.1).
- Default flush interval for batches is 1s (syncmsg: 3s); handler timeout 60s
  (syncmsg: 2m, documented as 5m).
- OpenTelemetry spans and metrics are emitted by default via the global
  providers; consumers continue the producer's trace.
- A handler cut short by `WithShutdownTimeout` is always nacked, never sent to
  the dead-letter hook, even when `WithMaxDeliveryAttempts` is reached.
- Outbox poison records: a publish error wrapping `pubsub.ErrPermanent`, or
  `outbox.WithMaxAttempts(n)` failed attempts (default 0 = unlimited), marks
  the record dead (`Store.Dead`; pgx sets `dead_at` and keeps the row). Dead
  rows are never claimed and release their ordering key. The pgx schema gained
  the `dead_at` column and both indexes became partial (`WHERE dead_at IS
  NULL`); add them when upgrading an existing table.
- Kafka: fetching a partition pauses when 10000 of its records are received
  but not committable (`kafka.WithMaxPendingPerPartition`) and resumes at half;
  per-partition commit errors are retried.
- SQS FIFO: a failed keyed publish pauses its MessageGroupId (later messages
  fail with `ErrKeyPaused` until `Publisher.ResumeKey`), as on Pub/Sub.
- Key fence: after a keyed message is finally nacked, the Subscriber nacks
  the key's other messages until the broker redelivers them in order
  (`WithKeyFence`, default `KeyFenceAuto`; `WithKeyFenceTTL` default 1m), in
  single and batch mode (§3.1).
- gcp: `gcp.SourceOption` now configures the Source (not ReceiveSettings);
  new `WithMaxAckExtension`, `WithSourceLogger`, `WithSubscriptionConfig`. Received
  messages carrying only `googclient_traceparent` get a `traceparent` copy.
- Logging (D5): every logger option (`pubsub.WithLogger`, `outbox.WithLogger`,
  `outbox/pgx.WithLogger`, `gcp`/`sqs.WithSourceLogger`, `kafka.WithLogger`)
  takes a `*log.Logger` from `kitchen/log` (an alias of `*slog.Logger`, so
  existing values still fit); nil means `log.Default()`, kitchen JSON, not
  Go's text handler. Errors are logged under `error` via `log.Err`.
- Errors (D7, kitchen/errs): `ErrKeyPaused` is `PUBSUB_KEY_PAUSED`
  (FailedPrecondition), `ErrClosed` is `PUBSUB_CLOSED` (Unavailable),
  `reply.ErrInvalidID` is `INVALID_REPLY_ID` (InvalidArgument); `errors.Is`
  keeps working. Publish validation errors (nil message, empty topic, SQS
  FIFO without Key, too many attributes) carry InvalidArgument; any other
  broker failure from Publish, and reply/redis Redis failures, are wrapped as
  `errs.ErrDependencyUnavailable` (503) with the broker error as the cause.
  outbox/pgx maps database errors with `pgx.MapError`. `ErrPermanent` stays a
  plain marker between handlers and the engine.
- sqs: `WithVisibilityTimeout` above 12h (the SQS limit) is rejected.
- SQS standard queues: `Message.Key` is sent as MessageGroupId (fair queues);
  received messages have an empty `Key` and the group in the
  `sqs-message-group-id` attribute.

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

### 3.1 Changes of the 2026-11-22 deep review

Settlement, ordering and wire changes (contracts with services):

- **Key fence modes.** `pubsub.WithKeyFence(KeyFenceAuto | KeyFenceOn |
  KeyFenceOff)`, default `KeyFenceAuto`: a source reports through the new
  optional `pubsub.RedeliveryAdvisor` whether a fence helps (on: Kafka,
  memory, Pub/Sub ordered with a 5s TTL; off: SQS FIFO, Pub/Sub unordered).
  `WithKeyFenceTTL(0)` is now an error: use `WithKeyFence(KeyFenceOff)`.
- **No fence on the broker's last delivery.** A nack whose `DeliveryAttempt`
  reached the subscription's dead-letter limit (reported by the source) no
  longer fences the key. Before, the fence then nacked every later message
  of the key for 1 minute, burning their delivery attempts until the broker
  dead-lettered them unprocessed.
- **The fence awaits every nacked message, in order.** It now holds all the
  messages of the key nacked since the failure and lifts only after the
  broker redelivered all of them, in arrival order. Before, the redelivery of
  the first message lifted it, so a later message already handed over could
  run before earlier ones still on their way back; and a redelivery reaching
  the subscriber lifted the fence while older messages were still queued
  ahead of it.
- **Batch isolation keeps key order and stops on outages.** After a whole
  batch fails, a message whose key had an earlier message fail in isolation
  is nacked without running, and after 2 isolated failures in a row the rest
  are nacked without running (systemic failure).
- **In-flight default** is 2 × `WithConcurrency` (batch: concurrency × batch
  size) instead of 1000, so a replica leases only what it can work on. A
  batch size above `WithMaxInFlight` is an error.
- **`Subscriber.Close(ctx)`** cancels `Run` and waits for the drain (or ctx).
  `Run` after `Close` returns nil at once; a second `Run` still returns
  `ErrClosed`.
- **Errors.** A broker status that rejects the request itself
  (INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED, UNAUTHENTICATED,
  FAILED_PRECONDITION, OUT_OF_RANGE) keeps its code instead of becoming
  `DEPENDENCY_UNAVAILABLE` (503).
- **Telemetry.** Duration histograms use the semconv buckets (5ms..10s) —
  dashboards built on the old SDK default buckets must be rebuilt. New
  metrics `kitchen.pubsub.settled.messages` (`kitchen.pubsub.outcome`: ack,
  nack, dead_letter, ordering, shutdown), `kitchen.pubsub.retries`,
  `kitchen.pubsub.inflight.messages`. "message nacked" warnings are sampled
  (10/s per subscriber, `suppressed` count).
- **Consumer identity.** Consumer metrics, process spans and logs carry
  `messaging.consumer.group.name` (kafka) or
  `messaging.destination.subscription.name` (gcp, memory) through the new
  optional `pubsub.ConsumerIdentifier`; add these labels to dashboards (it
  raises metric cardinality by the number of consumers).
- **New helpers.** `pubsub.DeadLetterTo(pub, topic)` publishes dead letters to
  a topic (unkeyed copy with `dead-letter-cause`, `-source`, `-message-id`,
  `-delivery-attempt`, `-key` attributes). `memory.WithPrefetch()` models
  Pub/Sub/Kafka prefetch; the memory package no longer claims to be
  identical to Pub/Sub ordered delivery.
- **gcp.**
  - `WithExactlyOnce` is removed: Ack/Nack always use the `*WithResult`
    calls, never block, and a failed acknowledgement is logged in the
    background before `Receive` returns.
  - The Source reads the subscription at `Receive` start (needs
    `pubsub.subscriptions.get`, which `roles/pubsub.subscriber` lacks; grant
    `roles/pubsub.viewer` or use `WithSubscriptionConfig(ordered,
    maxDeliveryAttempts)`); on failure it logs a warning and assumes an
    ordered subscription without a dead-letter policy.
  - On an unordered subscription `Message.Key` is empty and the ordering key
    is in `Attributes["googclient_ordering_key"]` (`gcp.OrderingKeyAttribute`).
  - The failed keyed publish that pauses a key now wraps `ErrKeyPaused` as
    well as its cause; a Publish racing `Close` returns `ErrClosed` (was 503).
  - `NewPublisher` rejects `HedgingSettings`; `WithPublishSettings` fills zero
    fields from the defaults (a zero Timeout no longer means no deadline);
    topic IDs and full names share one handle.
- **kafka.**
  - Records a poll returned for a partition that was then revoked or lost
    are dropped (not delivered, not committed); before, the old owner could
    process and commit them while the new owner did too.
  - The empty end-of-session revoke of the cooperative balancer no longer
    waits up to `WithRevokeTimeout`; shutdown waits one RevokeTimeout, not
    two. Keep RevokeTimeout ≥ the subscriber's shutdown timeout.
  - Commits are serialised, so an older offset never overwrites a newer one.
  - `Publish` after `Close` returns `ErrClosed`; a failed keyed produce
    pauses the key (`ErrKeyPaused`) until `ResumeKey` (`pubsub.KeyResumer`).
    Callers retrying a failed keyed publish must call `ResumeKey` first.
- **sqs.**
  - Every message of a receive is visibility-extended from receipt, not only
    the one handed to the engine (no more duplicates behind backpressure);
    extension continues during the drain until everything is settled, stops
    at 12h from receipt, retries failed calls, drops invalid receipts and
    never undoes a Nack.
  - Bodies outside the SQS character set are base64-encoded (marker
    unchanged); consumers outside kitchen see base64 for binary payloads.
  - Publish splits requests at 1 MiB including attributes; an oversized
    message, and any sender-fault entry, fails permanently
    (`ErrPermanent` + INVALID_ARGUMENT), so the outbox dead-letters it
    instead of retrying forever. Broker faults are UNAVAILABLE.
  - Deletes run on `WithAckConcurrency(n)` workers (default 8); Ack returns
    once queued. `sqs.API` gained `GetQueueAttributes`, and the Source needs
    `sqs:GetQueueAttributes` to read the redrive policy (warning without).
  - `Run` returns an error for a missing queue, denied access or invalid
    credentials instead of retrying forever. FIFO receives set
    `ReceiveRequestAttemptId`.
- **outbox.**
  - Relays claim and publish a key's consecutive records in one call (a key
    no longer waits one poll per record). Records after a failed one are
    released and re-published after it; with brokers that do not pause the
    key within one call (SQS FIFO), consumers may see a later record before,
    and again after, the earlier one: deduplicate on `outbox-id`.
  - Wire contract: every relayed message carries the attribute `outbox-id`
    (the record ID, decimal) unless the producer set it.
  - `outbox/pgx` `Enqueue` takes `pg_advisory_xact_lock(KeyLockID(key))`
    per key (FNV-1a 64 of `kitchen/pubsub/outbox\0<table>\0<key>`), so
    same-key writers wait for each other until commit. One `Enqueue` per
    transaction, or keys in a consistent order, avoids deadlocks.
  - Transient publish failures (UNAVAILABLE, DEADLINE_EXCEEDED, context
    errors, paused keys) back off without counting toward `WithMaxAttempts`.
    Publish is bounded to the lease minus a settle margin and every settle
    call has its own timeout, so a hung broker still records `last_error`
    and backs off.
  - Schema: run `Schema()` again, or `CREATE INDEX CONCURRENTLY
    <t>_free_idx ...` (see the `Schema` doc); `<t>_due_idx` can be dropped.
    Retry times use the database clock.
  - `outbox.Store` gained `Release`, and `Claim` returns runs: custom stores
    must implement both.
  - Claim fencing: `Record.Claim` is a token that changes on every claim, and
    the settle methods take `outbox.Ref{ID, Claim}` (`Record.Ref()`). A settle
    for a record that another relay re-claimed after the lease lapsed is a
    no-op returning `*outbox.ClaimLostError` (`ErrClaimLost`), logged at Warn
    by the relay. pgx adds the column `claim_token bigint NOT NULL DEFAULT 0`:
    re-run `Schema()` (or `ALTER TABLE ... ADD COLUMN IF NOT EXISTS
    claim_token ...`) **before** deploying the new code.
  - `Listen` resets its reconnect backoff after a successful reconnect.
- **reply/redis.** Requires Redis 6.0+ (fractional BLPOP timeouts). A timed-out
  `Wait` returns `context.DeadlineExceeded` and no longer destroys a pooled
  connection; `Close` errors are `DEPENDENCY_UNAVAILABLE`. Size the pool for
  concurrent waiters (one blocked connection each), preferably a client
  dedicated to the broker.

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
