# pubsub

Publish messages to a broker and consume them with one engine: bounded
concurrency, per-key ordering, retries, dead letters and a graceful drain.
The same handler code runs on Google Cloud Pub/Sub, SQS, Kafka or the
in-memory broker. It is the canonical messaging package (D7 in
[docs/decisions.md](../docs/decisions.md)); do not call a broker SDK from
business code.

## Install

```sh
go get github.com/dungnh3/kitchen                 # pubsub, memory, outbox, reply
go get github.com/dungnh3/kitchen/pubsub/gcp      # Google Cloud Pub/Sub
go get github.com/dungnh3/kitchen/pubsub/sqs      # Amazon SQS
go get github.com/dungnh3/kitchen/pubsub/kafka    # Kafka (franz-go)
go get github.com/dungnh3/kitchen/pubsub/outbox/pgx
go get github.com/dungnh3/kitchen/pubsub/reply/redis
```

## Quick start

A typed JSON handler on the in-memory broker (from `ExampleNewSubscriber`):

```go
broker := memory.New()
sub, _ := broker.Subscribe("orders", "billing")

type Order struct {
	ID    string
	Total int
}
s, err := pubsub.NewSubscriber(sub, pubsub.JSON(func(_ context.Context, o Order, _ *pubsub.Message) error {
	fmt.Println("order", o.ID, o.Total)
	return nil // ack; return an error to retry, pubsub.Permanent(err) to dead-letter
}), pubsub.WithConcurrency(32))
if err != nil {
	panic(err)
}
go func() { stopped <- s.Run(ctx) }()

m, _ := pubsub.NewJSON(Order{ID: "o-1", Total: 42})
m.Key = "customer-7" // same key => handled in order
_, err = broker.Publish(context.Background(), "orders", m)
```

`Run` blocks until `ctx` is cancelled, then drains and returns nil. Swap
`memory` for an adapter below and the handler does not change.

The handler's result decides what happens to the message. That is the whole
contract:

| Handler returns | What happens |
|---|---|
| `nil` | acked |
| an error | retried in process with backoff, then nacked so the broker redelivers it |
| `pubsub.Permanent(err)` | no retry; the dead-letter hook runs, then the message is acked |
| a panic | recovered, logged with the stack, treated as an error |

Defaults: 10 handlers at once, 2 × concurrency messages in flight (a replica
leases only what it can work on), 3 in-process retries (1s doubling to 30s,
with jitter), a 1m timeout per attempt, a 30s drain, and the key fence in
`KeyFenceAuto` mode. Tracing and metrics are on and use the global OpenTelemetry
providers. Every broker delivers at least once, so **handlers must be
idempotent**.

## Configuration

| Option | Default | Change it when |
|---|---|---|
| `WithConcurrency(n)` | 10 | handlers are I/O bound (raise) or a downstream is fragile (lower). In batch mode: number of shards |
| `WithMaxInFlight(n)` | 2 × concurrency; batch: concurrency × batch size | handlers are very fast behind a slow broker, or a hot key fills the window (raise); it bounds memory and leases and applies backpressure to the broker |
| `WithRetry(n, base, max)` | 3, 1s, 30s | `n = 0` to leave all retries to the broker |
| `WithHandlerTimeout(d)` | 1m | keep below the broker's lease extension; 0 disables |
| `WithShutdownTimeout(d)` | 30s | match your pod's termination grace period |
| `WithBatch(size, flush)` | 100, 1s | batch subscribers only |
| `WithMaxDeliveryAttempts(n)` | 0 (off) | the broker has no dead-letter policy (Kafka) |
| `WithKeyFence(mode)` | `KeyFenceAuto` | `KeyFenceOn` / `KeyFenceOff` to force it (see Ordering) |
| `WithKeyFenceTTL(d)` | 1m | bound on a fence; a source may report a shorter one (Pub/Sub: 5s) |
| `WithDeadLetter(f)` | log and ack | you need to keep poison messages; `pubsub.DeadLetterTo(pub, topic)` publishes them to a topic |
| `WithLogger(l)` | `log.Default()` | pass the service logger |
| `WithTelemetry(opts...)` | global providers | tests, or a non-global provider |

## Integration with kitchen

- **Logging.** Pass the logger built by `log.Setup` in `main` with
  `WithLogger`; adapters take `WithSourceLogger` / `WithLogger`. Nacks are
  logged at Warn, sampled to 10 per second per subscriber (the next line
  carries a `suppressed` count; metrics count every nack), drops and
  dead-letter failures at Error, all with `messaging.*` attributes and trace
  correlation.
- **Errors.** Publish errors carry `kitchen/errs` codes, so an RPC that
  publishes returns the right status without mapping (from `Example_errors`):

  ```go
  broker := memory.New()
  _ = broker.Close(context.Background())

  _, err := broker.Publish(context.Background(), "orders", &pubsub.Message{Data: []byte("x")})
  fmt.Println(errors.Is(err, pubsub.ErrClosed), errs.Code(err))
  // Output: true Unavailable
  ```

  `ErrKeyPaused` is FailedPrecondition, `ErrClosed` is Unavailable, bad input
  (nil message, empty topic) is InvalidArgument. A broker status that rejects
  the request itself keeps its code (INVALID_ARGUMENT for an oversized
  message, NOT_FOUND, PERMISSION_DENIED, ...), and any other broker failure
  is `errs.ErrDependencyUnavailable` (503) with the broker error as its cause.
- **Telemetry.** Call `otelx/sdk.Setup` in `main` (see
  [otelx/USAGE.md](../otelx/USAGE.md)); publishers inject the trace context
  into message attributes and the subscriber continues it, so one trace spans
  producer and consumer.
- **Databases and Redis.** The outbox takes a pool from `pgx.Open`, the reply
  broker a client from `redisx.Open`.
- **Server.** Run subscribers next to `server.Run` under the same signal
  context; see [server/USAGE.md](../server/USAGE.md) for the full wiring.
  Code that stops components one by one calls `Close(ctx)` instead (from
  `ExampleSubscriber_Close`):

  ```go
  stopped := make(chan error, 1)
  go func() { stopped <- s.Run(context.Background()) }()

  // On SIGTERM, bounded by the pod's grace period:
  ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
  defer cancel()
  if err := s.Close(ctx); err != nil {
  	fmt.Println("drain did not finish:", err)
  }
  ```

## Advanced

### Ordering and the key fence

Messages with the same `Key` run one at a time in publish order; different
keys run in parallel (from `Example_ordering`):

```go
s, _ := pubsub.NewSubscriber(sub, func(_ context.Context, m *pubsub.Message) error {
	mu.Lock()
	defer mu.Unlock()
	if got = append(got, string(m.Data)); len(got) == 3 {
		close(done)
	}
	return nil
},
	pubsub.WithConcurrency(16),               // different keys in parallel
	pubsub.WithKeyFence(pubsub.KeyFenceAuto), // the default: fence a failed key when the broker needs it
)
```

When a keyed message fails for good and is nacked, a broker may already have
handed over the next messages of that key (Kafka polls ahead; Pub/Sub
ordered delivery sends later messages while earlier ones are unacked). The
messages of the key already queued in the subscriber are always nacked. The
key fence goes further: it also nacks every later message of the key that
arrives, and waits for the broker to redeliver all of them in their original
order (same IDs) before it lets the key run again, or until the TTL expires.

| Mode | Behaviour |
|---|---|
| `KeyFenceAuto` (default) | the source decides through `pubsub.RedeliveryAdvisor`: on for Kafka and the memory broker, on with a 5s TTL for Pub/Sub ordered subscriptions, off for SQS FIFO (its message groups already hold the key back) and Pub/Sub unordered subscriptions; on for a source that does not say |
| `KeyFenceOn` | always, with `WithKeyFenceTTL` |
| `KeyFenceOff` | never; order is what the broker guarantees |

In every mode, the nack that is the broker's last delivery of a message
(its `DeliveryAttempt` reached the subscription's dead-letter limit,
reported by the source) never fences: that message goes to the broker's
dead-letter topic and will not come back, so the rest of the key is
processed instead of being nacked into the dead-letter topic too.

Order then holds across failures:

- exact for Kafka partitions (redelivery is in process, same consumer);
- for SQS FIFO, the broker holds the group back while a message of it is in
  flight;
- for Pub/Sub it holds within one process. Across pods, ordering-key
  affinity is best effort, so make handlers check a version or sequence
  number when strict order matters.

The publisher side keeps order too: after a failed keyed publish, Pub/Sub and
SQS FIFO publishers pause the key (`ErrKeyPaused`) until you call
`ResumeKey(topic, key)`.

### Dead letters

Use it when a message can never succeed, or when the broker has no
dead-letter queue of its own (from `ExampleWithDeadLetter`):

```go
s, err := pubsub.NewSubscriber(sub, func(_ context.Context, m *pubsub.Message) error {
	if len(m.Data) == 0 {
		return pubsub.Permanent(errors.New("empty payload")) // retrying cannot fix it
	}
	return nil
},
	pubsub.WithRetry(3, 100*time.Millisecond, 5*time.Second),
	pubsub.WithMaxDeliveryAttempts(5),
	pubsub.WithDeadLetter(func(_ context.Context, m *pubsub.Message, cause error) error {
		fmt.Println("dead letter:", m.Topic, cause)
		return nil // acked; return an error to nack it instead
	}),
	pubsub.WithLogger(logger),
)
```

`WithMaxDeliveryAttempts(n)` sends a message to the hook once the broker has
delivered it `n` times. If the hook returns an error, the message is nacked,
never lost. On Pub/Sub and SQS, prefer the broker's own dead-letter policy and
leave this at 0.

`pubsub.DeadLetterTo(pub, topic)` is a ready hook that publishes a copy to a
dead-letter topic on any backend (the Kafka example below uses it). The copy
is unkeyed and carries the cause, source, original ID, delivery attempt and
key in the `dead-letter-*` attributes; if that publish fails the message is
nacked.

### Batches

Use a batch handler for bulk writes (from `ExampleNewBatchSubscriber`):

```go
s, _ := pubsub.NewBatchSubscriber(sub, func(_ context.Context, ms []*pubsub.Message) error {
	fmt.Println("batch of", len(ms))
	// Fail only message 1; the others are acked:
	// return &pubsub.BatchError{Failed: map[int]error{1: err}}
	return nil
}, pubsub.WithBatch(3, time.Second), pubsub.WithConcurrency(1))
```

Messages are sharded by key, so each key stays in order inside one shard;
unkeyed messages go to the least loaded shard. `pubsub.JSONBatch` decodes a
batch into a typed slice. A `*BatchError` fails only the listed messages;
any other error fails the whole batch. A batch that still fails as a whole
after its retries is split: each message runs once on its own to find the
poison message, except a message whose key had an earlier message fail (it
is nacked behind it, so order holds). If two isolated messages in a row fail,
the failure is taken as systemic (a dependency outage) and the rest are
nacked without running, so an outage does not multiply the load on the
failing dependency. Within one handler call, failing a message and
succeeding a later message of the same key is the handler's choice: fail the
later ones too when order matters.

### Long-running handlers

The defaults fail fast: 1m per attempt and 3 quick retries, then a nack and
an immediate redelivery. For jobs of minutes (reports, media generation):

- `WithHandlerTimeout(d)` above the longest job, and `WithRetry(0, 0, 0)` so
  a failure goes back to the broker instead of rerunning in process;
- keep the broker lease above it: gcp `WithMaxExtension`, sqs visibility
  extension (automatic, at most 12h), kafka `WithRevokeTimeout` and the
  group's rebalance timeout;
- set a Pub/Sub subscription retry policy (minimum backoff) so a failed job
  is not redelivered at once;
- keep `WithMaxInFlight` small (the default 2 × concurrency) so jobs are
  spread across replicas.

### Transactional outbox

Use it when a database write and a message must both happen or neither: the
message is written in the same transaction, and a relay publishes it later
(from the `outbox/pgx` example):

```go
pool, err := pgx.Open(ctx, pgx.Config{URL: "postgres://localhost/app"})
if err != nil {
	logger.ErrorContext(ctx, "startup failed", log.Err(err))
	return
}
defer pool.Close()
store, _ := outboxpgx.New(pool)

// Business write and message in one transaction.
err = jpgx.BeginFunc(ctx, pool, func(tx jpgx.Tx) error {
	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance - 10 WHERE id = 1"); err != nil {
		return err
	}
	return store.Enqueue(ctx, tx, "payments", &pubsub.Message{Key: "account-1", Data: []byte(`{"debit":10}`)})
})

var publisher pubsub.Publisher = memory.New() // e.g. gcp.NewPublisher(client)
relay, _ := outbox.NewRelay(store, publisher)
go func() { _ = store.Listen(ctx) }()
_ = relay.Run(ctx)
```

- Run several relays for availability. A relay claims a key's pending
  records as one run, starting at the oldest, and publishes them in order in
  one call; a key is held by one relay at a time, so per-key order survives
  any number of relays. Per-key order is the commit order of the enqueueing
  transactions: `Enqueue` serialises transactions that enqueue for the same
  key until they commit (one `Enqueue` per transaction, or keys in a
  consistent order, avoids deadlocks).
- Every relayed message carries `outbox-id` (the record ID). Delivery is at
  least once; deduplicate on it.
- `Listen` wakes the relay on `LISTEN/NOTIFY` instead of waiting for the
  next poll.
- The trace context of the enqueuing transaction travels with the message.
- `outbox.WithMaxAttempts(n)` marks a record dead after `n` counted
  failures, and so does an error wrapping `ErrPermanent`. Transient broker
  failures (UNAVAILABLE, DEADLINE_EXCEEDED, context errors, paused keys) back
  off without counting, so an outage does not kill records. Dead rows stay in the table
  (`dead_at`) for inspection.
- Apply `store.Schema()` through your migrations (`kitchen/migrate`).
  `CreateTable` is for development.

### Request/reply

Use it when a caller must wait for a worker's answer over a queue (from the
`reply/redis` example):

```go
broker, _ := replyredis.New(rdb)
pub := memory.New()

id := reply.NewID()
w, _ := broker.Expect(ctx, id) // register before sending
defer func() { _ = w.Close() }()
_, _ = pub.Publish(ctx, "render-jobs", &pubsub.Message{
	Data: []byte("render #42"), Attributes: map[string]string{reply.IDAttribute: id},
})
// The worker, after rendering: broker.Reply(ctx, m.Attributes[reply.IDAttribute], result)
go func() { _ = broker.Reply(ctx, id, []byte("render #42 done")) }()

result, err := w.Wait(ctx) // blocks until the reply, ctx's deadline or Close
```

Reply IDs are 128-bit random capabilities: whoever knows one can answer, so
never derive them from request data. Always bound `Wait` with a deadline.
`reply/redis` needs Redis 6.0+ and holds one connection per waiting caller:
size the pool for concurrent waiters, preferably with a client dedicated to
the broker.

## Backends and adapters

### memory

`memory.New()` is a complete in-process broker with redelivery, keys and
`WithNackDelay`. Use it in tests and examples, never across processes. By
default it hands over a key's next message only after the previous one is
settled; `memory.WithPrefetch()` hands later messages over early, as Pub/Sub
ordered delivery and Kafka do, so tests exercise the key fence.

### gcp (Google Cloud Pub/Sub)

From the `gcp` example:

```go
pub, _ := gcp.NewPublisher(client)
defer func() { _ = pub.Close(ctx) }()
if _, err := pub.Publish(ctx, "orders", &pubsub.Message{Key: "customer-7", Data: []byte(`{"id":1}`)}); err != nil {
	logger.WarnContext(ctx, "publish failed", log.Err(err)) // on ErrKeyPaused: handle, then pub.ResumeKey("orders", "customer-7")
}

// State the subscription's settings (message ordering on, dead-letter
// policy of 5 attempts): roles/pubsub.subscriber cannot read them, and
// the key fence needs the dead-letter limit.
src, _ := gcp.NewSource(client, "orders-billing",
	gcp.WithSubscriptionConfig(true, 5),
	gcp.WithMaxOutstanding(1000, 100<<20))
sub, _ := pubsub.NewSubscriber(src, func(_ context.Context, _ *pubsub.Message) error {
	return nil
}, pubsub.WithConcurrency(64))
_ = sub.Run(ctx) // blocks until ctx is cancelled, then drains
```

- Enable message ordering on the subscription for keys to matter. On an
  unordered subscription `Key` is empty and the ordering key is in
  `Attributes[gcp.OrderingKeyAttribute]`.
- Always pass `gcp.WithSubscriptionConfig(ordered, maxDeliveryAttempts)`
  with the subscription's real settings (`enable_message_ordering` and the
  dead-letter policy's `max_delivery_attempts`, 0 without a policy). The
  key fence uses them: it is off on unordered subscriptions and never fences
  on the delivery that sends a message to the dead-letter topic. Keep them
  in sync with the subscription's Terraform or gcloud config; wrong values
  break ordering or dead-lettering.
- Without the option the source reads the settings at `Receive` start. That
  needs `pubsub.subscriptions.get`, which `roles/pubsub.subscriber` does
  **not** include (grant `roles/pubsub.viewer` on the subscription). If the
  read fails it logs a warning and assumes an ordered subscription with no
  dead-letter policy.
- Acks never block; on exactly-once subscriptions the result is checked in
  the background and a failure is logged.
- Keep `WithMaxOutstanding` messages at or above `WithMaxInFlight`, and
  `WithMaxExtension` (default 60m) above handler timeout × attempts.
- The first failed keyed publish already wraps `ErrKeyPaused` (with its
  cause); hedging is not allowed with ordering, so `NewPublisher` rejects
  `HedgingSettings`.
- Keep the Google client's built-in tracing off; kitchen already traces, and
  it maps `googclient_traceparent` for you.

### sqs (Amazon SQS)

From the `sqs` example:

```go
pub, _ := sqs.NewPublisher(client)
if _, err := pub.Publish(ctx, queue, &pubsub.Message{
	Key: "customer-7", Data: []byte("hello"),
	Attributes: map[string]string{sqs.DedupIDAttribute: "order-1"},
}); err != nil {
	logger.WarnContext(ctx, "publish failed", log.Err(err))
}

src, _ := sqs.NewSource(client, queue)
sub, _ := pubsub.NewSubscriber(src, func(_ context.Context, _ *pubsub.Message) error { return nil })
_ = sub.Run(ctx)
```

- FIFO queues need a `Key` (MessageGroupId).
- On standard queues `Key` becomes the fair-queue group, and received
  messages carry it in the `sqs-message-group-id` attribute, not in `Key`.
- The source extends visibility from the moment a message is received
  (default 30s, at most 12h from receipt), also during the drain, and never
  after a Nack.
- Deletes are batched and sent by `WithAckConcurrency(n)` workers (default
  8); Ack returns once the delete is queued.
- Bodies outside the SQS character set (binary, protobuf) are base64-encoded
  with the `kitchen-body-encoding` attribute; requests are split at 1 MiB,
  and an oversized message or a sender-fault entry fails permanently.
- Configure a redrive policy on the queue for dead letters, and grant
  `sqs:GetQueueAttributes` so the source can read it (FIFO queues then never
  fence on the last receive; `KeyFenceAuto` leaves FIFO groups to SQS).
- `Run` returns an error for a missing queue, denied access or invalid
  credentials.

### kafka (franz-go)

From the `kafka` example:

```go
src, _ := kafka.NewSource("billing", []string{"orders"}, kafka.WithClientOptions(kgo.SeedBrokers("localhost:9092")))
sub, _ := pubsub.NewSubscriber(src, func(_ context.Context, _ *pubsub.Message) error {
	return nil
},
	// Kafka has no broker DLQ: after 5 deliveries, publish poison
	// messages to a dead-letter topic (nacked if that publish fails).
	pubsub.WithMaxDeliveryAttempts(5),
	pubsub.WithDeadLetter(pubsub.DeadLetterTo(pub, "orders.dlq")))
_ = sub.Run(ctx)
```

- Offsets are committed only up to the last settled record of each
  partition.
- A nacked record is redelivered in process after `WithRedeliveryBackoff`.
- A partition with too many pending records is paused
  (`WithMaxPendingPerPartition`, default 10000), so one stuck record cannot
  grow memory.
- Keep the franz-go defaults (idempotent producer) to preserve per-key order.
- A record whose produce fails pauses its key on that topic (`ErrKeyPaused`)
  until `pub.ResumeKey(topic, key)`; messages of the key in the same Publish
  call may already be written. `Publish` after `Close` fails with
  `ErrClosed`.
- Set `WithRevokeTimeout` at or above the subscriber's `WithShutdownTimeout`
  (both 30s) and below the group rebalance timeout (60s): acks of handlers
  still running after it are not committed and those records are
  redelivered.
- Records a poll returned for a partition that was revoked meanwhile are
  dropped, not delivered; the new owner processes them.
- Delivery attempts are counted in process and restart from 1 after a
  restart or rebalance, so `WithMaxDeliveryAttempts` bounds a poison record
  per owner, not forever.

## Testing

- Use `memory.New()` as both Publisher and Source; the examples above run
  as tests. Add `memory.WithPrefetch()` to test ordering under failures the
  way Pub/Sub and Kafka deliver.
- Use `WithLogger(log.Discard())` to silence logs, or
  `log.New(&buf, log.Options{})` to assert on them.
- Assert on errors with `errors.Is(err, pubsub.ErrKeyPaused)` and
  `errs.Code(err)`.
- Adapter tests use the broker emulators: `pstest` for Pub/Sub and `kfake`
  for Kafka. The outbox tests need `KITCHEN_TEST_PG_DSN`.

## Observability

- **Spans** (OTel messaging conventions): `send <topic>` (producer) and
  `process <topic>` (consumer), linked through the message attributes.
- **Metrics**: `messaging.client.operation.duration`,
  `messaging.client.sent.messages`, `messaging.client.consumed.messages`,
  `messaging.process.duration` (duration histograms use the semconv buckets,
  5ms to 10s), with `error.type` set to `timeout`, `canceled`, `permanent`,
  `key_paused`, `partial` or `_OTHER`. The subscriber adds
  `kitchen.pubsub.settled.messages` by `kitchen.pubsub.outcome` (`ack`,
  `nack`, `dead_letter`, `ordering`: nacked unprocessed to keep a key in
  order, `shutdown`), `kitchen.pubsub.retries` and the
  `kitchen.pubsub.inflight.messages` gauge. Alert on `dead_letter` and on a
  rising `nack` / `ordering` rate. Consumer metrics, process spans and log
  lines carry `messaging.consumer.group.name` (Kafka) or
  `messaging.destination.subscription.name` (Pub/Sub, memory), from a
  source implementing `pubsub.ConsumerIdentifier`.
- **Log attributes**: `messaging.system`, `messaging.destination.name`,
  `messaging.message.id`, `delivery_attempt`, `error`.
- **errs reasons**: `PUBSUB_KEY_PAUSED`, `PUBSUB_CLOSED`, `INVALID_REPLY_ID`,
  plus `DEPENDENCY_UNAVAILABLE` and `INVALID_ARGUMENT` on publish.

## Production checklist

- Make handlers idempotent; delivery is at least once.
- Set `WithShutdownTimeout` below the pod's termination grace period.
- Keep `WithHandlerTimeout` below the broker's lease or visibility limits.
- Configure a dead-letter policy on Pub/Sub and SQS, or `WithDeadLetter`
  on Kafka.
- Keep the key fence in `KeyFenceAuto` (or `KeyFenceOn`) when order matters.
- On Pub/Sub, pass `gcp.WithSubscriptionConfig(ordered, maxDeliveryAttempts)`
  matching the subscription (or grant `roles/pubsub.viewer`), so the fence
  knows whether the subscription is ordered and its dead-letter limit.
- For long jobs, follow "Long-running handlers" above.
- Run the outbox relay in every instance, and apply the outbox schema
  through migrations; re-apply `store.Schema()` before deploying a new
  kitchen version (it upgrades the table in place, e.g. the `claim_token`
  column that fences late settles to the relay holding the claim).
- Bound `reply.Waiter.Wait` with a deadline.

## Migrating from the old kitchen

The old `pubsub`, `syncmsg` and `syncbrokers` packages collapse into this
engine and `reply`; see
[docs/migration/messaging.md](../docs/migration/messaging.md).
