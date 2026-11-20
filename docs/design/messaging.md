# Design: messaging (`pubsub`, `lease`)

Status: approved by the owner on 2026-11-20 ("Đồng ý với tất cả Recommended").
Replaces the old packages `pubsub` (gcp, gcp/multi_async, gcp/multi_batch,
kafka_sarama, sqs), `syncmsg`, `syncbrokers`, `coordinators`, `queue` and
`scheduler` of the old library. Review of the old code:
[docs/review/03-messaging-pubsub-syncmsg.md](../review/03-messaging-pubsub-syncmsg.md).
Migration: [docs/migration/messaging.md](../migration/messaging.md).

## 1. Goals

- One small, stable `Publisher` interface that every backend, the transactional
  outbox and the SSE relay can target.
- One consumer engine (built from the best parts of the old `syncmsg`) shared by
  every backend: bounded concurrency, per-key FIFO, in-process retry with
  backoff, per-message batch results, dead-lettering, graceful drain,
  OpenTelemetry on by default.
- Backends: Google Cloud Pub/Sub **v2** (v1 support ends 2026-12-31), Kafka
  (franz-go), Amazon SQS (aws-sdk-go-v2), in-memory (tests).
- Transactional outbox on Postgres (pgx/v5).
- Request/reply over Redis (replaces `syncbrokers`).
- Leases with fencing tokens and a leader elector over Redis (replaces
  `coordinators`).
- Every bug listed in the review is fixed by construction (see the migration
  guide, "Behaviour changes").

Non-goals: a scheduler (dropped; use Cloud Scheduler → Pub/Sub, a cron library,
or `lease.Elector` around a `time.Ticker`), a router/middleware framework,
schema registries, exactly-once semantics.

## 2. Module layout

| Directory | Module | Contents | Heavy deps |
|---|---|---|---|
| `pubsub/` | core | `Message`, `Publisher`, `Source`/`Delivery`, `Subscriber` engine, errors, telemetry, `JSON[T]` | otel API |
| `pubsub/memory/` | core | in-memory broker (Publisher + Source) | – |
| `pubsub/outbox/` | core | `Store` interface + `Relay` | – |
| `pubsub/reply/` | core | `Broker`/`Waiter` interfaces, `NewID` | – |
| `lease/` | core | `Locker` interface, `Lease`, `Elector` | – |
| `pubsub/gcp/` | submodule | Pub/Sub v2 Publisher + Source | `cloud.google.com/go/pubsub/v2` v2.9.0 |
| `pubsub/kafka/` | submodule | franz-go Publisher + Source | `github.com/twmb/franz-go` v1.22.3 |
| `pubsub/sqs/` | submodule | SQS Publisher + Source | `aws-sdk-go-v2/service/sqs` v1.53.0 |
| `pubsub/outbox/pgx/` | submodule | Postgres `Store` + `Enqueue` | `github.com/jackc/pgx/v5` v5.13.0 |
| `pubsub/reply/redis/` | submodule | Redis `Broker` | `github.com/redis/go-redis/v9` v9.23.0 |
| `lease/redis/` | submodule | Redis `Locker` | `github.com/redis/go-redis/v9` v9.23.0 |

Submodules depend on the core module; local development uses an uncommitted
`go.work` (D1). Versions verified with `go list -m` on 2026-11-20.

## 3. Core API (`pubsub`)

```go
type Message struct {
    ID              string            // set by the broker on receive / returned by Publish
    Topic           string            // topic / queue the message was received from
    Key             string            // ordering key (Pub/Sub), record key (Kafka), MessageGroupId (SQS FIFO)
    Data            []byte
    Attributes      map[string]string
    PublishTime     time.Time
    DeliveryAttempt int               // 1-based; 0 = unknown
}

type Publisher interface {
    // Publish sends msgs to topic and waits for all outcomes. It returns the
    // broker IDs in input order. On partial failure it returns *PublishError.
    Publish(ctx context.Context, topic string, msgs ...*Message) ([]string, error)
    Close(ctx context.Context) error
}
type PublishError struct{ IDs []string; Failed map[int]error }
type KeyResumer interface{ ResumeKey(topic, key string) } // optional

type Delivery interface { Message() *Message; Ack(); Nack() }
type Source interface {
    // Receive calls fn for every message until ctx is done or a fatal error.
    // Contract: fn is called sequentially, in delivery order, for messages that
    // share a Key; it may be called concurrently for different keys. A nacked
    // keyed message is redelivered before later messages of that key.
    Receive(ctx context.Context, fn func(context.Context, Delivery)) error
    System() string // OTel messaging.system value
}

type Handler      func(ctx context.Context, m *Message) error
type BatchHandler func(ctx context.Context, ms []*Message) error
type BatchError   struct{ Failed map[int]error }

var ErrPermanent, ErrKeyPaused, ErrClosed error
func Permanent(err error) error

func NewSubscriber(src Source, h Handler, opts ...Option) (*Subscriber, error)
func NewBatchSubscriber(src Source, h BatchHandler, opts ...Option) (*Subscriber, error)
func (s *Subscriber) Run(ctx context.Context) error // returns after drain

func JSON[T any](h func(ctx context.Context, v T, m *Message) error) Handler
```

### 3.1 Settlement semantics

| Handler result | Action |
|---|---|
| `nil` | Ack |
| error | retry in process (`MaxRetries`, exponential backoff + jitter, each attempt bounded by `HandlerTimeout`), then Nack → broker redelivers / dead-letters by its own policy |
| `errors.Is(err, ErrPermanent)` | no retry → `DeadLetter` hook → Ack |
| panic | recovered, treated as an ordinary error |
| final failure and `DeliveryAttempt >= MaxDeliveryAttempts` (option, default off) | treated as permanent → `DeadLetter` hook → Ack |

`DeadLetter func(ctx, *Message, error) error`: nil → Ack, error → Nack (never
lose a message because the DLQ is down). Without a hook, a permanent failure is
logged at Error and acked. Messages expose no `Ack`/`Nack`: the handler result
is the only way to settle, so a message cannot be forgotten.

Batch: `nil` acks all; `*BatchError` retries only the failed indices (a
`Permanent` entry is dead-lettered at once) and acks the rest; any other error
fails the whole batch: retried, then split and each message run once alone to
isolate the poison message (handlers must be idempotent; documented).

### 3.2 Concurrency and ordering

- `Concurrency` (default 10) handler goroutines; `MaxInFlight` (default 1000)
  received-but-unsettled messages; when full, the source callback blocks
  (backpressure to the broker).
- Single mode: unkeyed messages run in parallel; keyed messages go to a per-key
  FIFO queue drained by one goroutine at a time, so same-key messages run
  sequentially and different keys in parallel. When a keyed message is finally
  nacked, the remaining queued messages of that key are nacked too, so the
  broker redelivers the key in order.
- Batch mode: `Concurrency` shards, shard = hash(key) (unkeyed: round robin);
  each shard buffers up to `MaxBatchSize` (default 100) or `FlushInterval`
  (default 1s) and processes its batches sequentially. Later same-key messages of
  a batch whose earlier message failed are nacked, not acked.

### 3.3 Lifecycle

`Run(ctx)` starts `Source.Receive`. When ctx is cancelled (or Receive fails),
the engine stops receiving, nacks queued-but-not-started messages, lets running
handlers finish (their ctx is not cancelled) for up to `ShutdownTimeout`
(default 30s), then cancels them, waits for every goroutine and returns (nil on
ctx cancellation, the fatal error otherwise). Backoff waits are interrupted by
shutdown (message nacked). goleak-verified.

### 3.4 Telemetry (on by default)

- Publisher: injects W3C trace context (global propagator by default) into a
  copy of the attributes; span `send {topic}` (kind PRODUCER); metrics
  `messaging.client.operation.duration`, `messaging.client.sent.messages`.
- Subscriber: extracts context per message; span `process {topic}` (kind
  CONSUMER) with the producer context as parent (single mode) and span links
  (single and batch); metrics `messaging.process.duration`,
  `messaging.client.consumed.messages`; `error.type` on failures.
- Attributes from semconv v1.43.0 (messaging conventions are still
  "Development"; pinned). Options `WithTracerProvider`, `WithMeterProvider`,
  `WithPropagator`; pass noop providers to disable.

## 4. Backends

- **gcp**: `gcp.NewPublisher(client, opts)` caches one `*pubsub.Publisher` per
  topic with `EnableMessageOrdering`; a failed keyed publish leaves the key
  paused and later publishes of it return `ErrKeyPaused` until
  `ResumeKey(topic, key)` (FIFO first). `gcp.NewSource(client, subscription,
  opts)` sets `ReceiveSettings` (MaxOutstandingMessages, MaxOutstandingBytes,
  MaxExtension, MinDurationPerAckExtension, NumGoroutines) and
  `ShutdownOptions`. Tests: `pstest` in-process fake.
- **kafka**: `kafka.NewPublisher(client)` produces synchronously in batches
  (`ProduceSync`), Key = record key. `kafka.NewSource(opts)` owns a consumer
  group client (cooperative-sticky), manual commits of the **contiguous**
  settled offsets per partition (at-least-once, no commit past unprocessed
  records), drains in-flight records of revoked partitions before committing,
  and implements Nack as in-process redelivery after backoff that keeps per-key
  order (later records of a key with a pending redelivery wait behind it).
  Kafka has no broker DLQ: use the `DeadLetter` hook (e.g. publish to a
  `.dlq` topic). Tests: franz-go `kfake`.
- **sqs**: topic = queue URL. Publisher uses `SendMessageBatch` (10 per call),
  Key → `MessageGroupId`, optional dedup id via attribute option. Source
  long-polls (`MaxNumberOfMessages=10`, `WaitTimeSeconds=20`), extends
  visibility for in-flight messages, batches deletes on Ack, Nack =
  `ChangeMessageVisibility(0)`, `DeliveryAttempt` = `ApproximateReceiveCount`,
  backoff on receive errors, empty bodies are delivered (not skipped). Tests:
  `httptest` fake of the SQS JSON protocol.
- **memory**: Publisher + per-subscription Source with ack/nack/redelivery and
  per-key ordering identical to Pub/Sub ordered delivery.

## 5. Transactional outbox

```go
// core: pubsub/outbox
type Record struct{ ID int64; Topic string; Msg *pubsub.Message; Attempts int }
type Store interface {
    Claim(ctx context.Context, limit int, lease time.Duration) ([]Record, error)
    Sent(ctx context.Context, ids []int64) error
    Failed(ctx context.Context, id int64, cause error, retryAt time.Time) error
}
type Waker interface{ Wake() <-chan struct{} } // optional on Store
func NewRelay(store Store, pub pubsub.Publisher, opts ...RelayOption) (*Relay, error)
func (r *Relay) Run(ctx context.Context) error

// submodule: pubsub/outbox/pgx
func New(pool *pgxpool.Pool, opts ...Option) (*Store, error)
func (s *Store) Enqueue(ctx context.Context, tx pgx.Tx, topic string, msgs ...*pubsub.Message) error
func (s *Store) CreateTable(ctx context.Context) error
```

- `Enqueue` writes inside the caller's transaction, injects the trace context
  and `pg_notify`s the relay. Table name is validated as an SQL identifier.
- `Claim` only takes the **head** (oldest unsent) row of each ordering key
  (`FOR UPDATE SKIP LOCKED`) plus any unkeyed rows, and leases them; several
  relays can run concurrently without breaking per-key order. A crashed relay's
  lease expires and another relay reclaims (at-least-once).
- Relay publishes per topic, deletes sent rows, reschedules failed rows with
  backoff, and calls `KeyResumer.ResumeKey` after a failure so the retried head
  goes first. LISTEN/NOTIFY wakes the relay; polling (default 1s) is the
  fallback.

## 6. Request/reply (`pubsub/reply`)

```go
type Broker interface {
    Expect(ctx context.Context, id string) (Waiter, error) // call before sending the request
    Reply(ctx context.Context, id string, payload []byte) error
}
type Waiter interface{ Wait(ctx context.Context) ([]byte, error); Close() error }
func NewID() string // 128-bit crypto/rand, URL-safe
```

Redis implementation: `Reply` = `LPUSH reply:{id}` + `PEXPIRE` in one
MULTI; `Wait` = `BLPOP` on that key in bounded slices so ctx cancellation is
honoured. A reply that arrives before `Wait` is kept until its TTL (no lost
replies), no fan-out, no pooling. IDs must be unguessable (`NewID`) because the
key is the only capability.

## 7. Leases (`lease`)

```go
type Lease struct{ Name, Owner string; Token uint64; TTL time.Duration }
type Locker interface {
    Acquire(ctx context.Context, name, owner string, ttl time.Duration) (Lease, error) // ErrNotHeld if taken
    Renew(ctx context.Context, l Lease) (Lease, error)                                 // ErrNotHeld if lost
    Release(ctx context.Context, l Lease) error
}
type Elector struct{ /* Locker, Name, Owner, TTL, RetryInterval, Logger */ }
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context, l Lease) error) error
```

Redis: `SET NX PX` + fencing token from `INCR {name}:fence` (monotonic,
never expires) in one Lua script; renew/release are owner-checked Lua scripts
(`PEXPIRE`, millisecond precision). `Elector` renews every TTL/3, cancels the
leader ctx before the lease can expire locally (TTL minus a safety margin since
the last successful renew), waits for `lead` to return before releasing, never
runs callbacks concurrently. Sharding = one lease per shard name. Downstream
writes should carry `Lease.Token` as a fencing token.

## 8. Decisions taken by default (to review)

The owner accepted every "(Recommended)" option on 2026-11-20 and asked that
later questions take the option best for system design and security. Each
decision below can be revisited.

| # | Decision | Alternatives considered | Why |
|---|---|---|---|
| M1 | Own engine based on syncmsg | wrap Watermill; use Watermill directly | batch handlers, per-key ordering, slog/OTel per D5, no heavy deps in core. Cost: we maintain the engine |
| M2 | `Handler` returns error; `ErrPermanent`; no public Ack/Nack | manual Ack/Nack too; no in-process retry | impossible to forget settlement; same semantics on every backend; Kafka needs in-process retry anyway |
| M3 | Batch: per-message results via `*BatchError` | all-or-nothing + split; `[]error` | avoids replaying succeeded side effects; length mismatch impossible |
| M4 | Ordering key not auto-resumed; `ErrKeyPaused` + `ResumeKey` | auto-resume (old gcp); configurable | keeps FIFO, which is the point of a key; old two publishers disagreed |
| M5 | Broker DLQ + optional `DeadLetter` hook | library-level DLQ always; broker only | uses Pub/Sub dead-letter policy / SQS redrive; Kafka gets a hook |
| M6 | Outbox in scope (`pubsub/outbox` + `pgx`) | leave to SSE/data-utils; later PR | dual writes are common; SSE relay can reuse it |
| M7 | OTel on by default | opt-in; propagate only | traces were broken across services |
| M8 | Parallel across keys, FIFO within a key | single worker; no ordering | throughput at 30K-connection scale with correct ordering |
| M9 | Keep Kafka (franz-go, pub + sub) | sarama; consumer only | owner wants Kafka; franz-go has `kfake` and correct group APIs |
| M10 | Keep SQS (pub + sub, FIFO) | consumer only; no FIFO | owner wants SQS; aws-sdk-go v1 is EOL |
| M11 | Request/reply on Redis per-ID list + BLPOP | per-instance pub/sub channel; on top of pubsub | no lost replies; no fan-out. Cost: one blocking connection per waiter |
| M12 | Lease on Redis with fencing tokens | Postgres; both; k8s Lease | light dep, fencing, testable with miniredis |
| M13 | Drop scheduler; merge syncmsg into pubsub | redesign scheduler; keep syncmsg separate | one engine; schedulers are a solved problem elsewhere |
| M14 | Layout `pubsub/*` + `lease/*` | top-level outbox/reply/lease; `messaging/*` | matches D6, short import paths |
| M15 | Outbox driver pgx/v5, `Enqueue(ctx, pgx.Tx, …)` | database/sql; minimal Execer interface | LISTEN/NOTIFY, performance |
| M16 | `Publish(ctx, topic, msgs...) ([]string, error)` only | + PublishAsync; single message | smallest stable interface; still batches |
| M17 | `[]byte` payload + `JSON[T]` helper | generic Subscriber[T]; bytes only | no generics leaking into backends; typed handlers when wanted |
| M18 | Outbox claims only the head row per key | single active relay; advisory locks | race-free per-key order with N relays (security/correctness) |
| M19 | Reply IDs from `crypto/rand` (`NewID`) and validated key prefix | caller-chosen IDs | the Redis key is the capability; guessable IDs allow reply spoofing |
| M20 | Outbox table name validated as identifier | free string | prevents SQL injection through configuration |
| M21 | Defaults: Concurrency 10, MaxInFlight 1000, MaxRetries 3, backoff 1s×2ⁿ cap 30s ±jitter, HandlerTimeout 60s, ShutdownTimeout 30s, MaxBatchSize 100, FlushInterval 1s | syncmsg's 3s flush / 2m timeout | lower latency; documented zero-value behaviour |
| M22 | Functional options everywhere in this area | Config structs | many optional knobs with defaults |
| M23 | Batch split after whole-batch failure runs each message once (no fresh retry budget) | full retry per split message (syncmsg) | bounded work on poison batches |

## 9. Open questions

- CI: submodules require the unpublished core module; `go mod tidy -diff`
  in CI fails until the core is tagged (affects every area's submodules).
- Should the SSE outbox (`sse` area) be replaced by `pubsub/outbox`? To agree
  with the SSE session.
