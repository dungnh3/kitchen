# Design: messaging (`pubsub`, `lease`)

Status: approved by the owner on 2026-11-20 ("Đồng ý với tất cả Recommended").
Replaces the old packages `pubsub` (gcp, gcp/multi_async, gcp/multi_batch,
kafka_sarama, sqs), `syncmsg`, `syncbrokers`, `coordinators`, `queue` and
`scheduler` of the old library.
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

- `Concurrency` (default 10) handler goroutines; `MaxInFlight` (default
  2 × Concurrency, batch: Concurrency × MaxBatchSize) received-but-unsettled
  messages; when full, the source callback blocks (backpressure to the
  broker), so a replica leases only what it can work on.
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
  MaxExtension, MinDurationPerAckExtension, NumGoroutines) and leaves
  `ShutdownOptions` at the v2 default (wait for processing, no timeout), so
  `Receive` returns only after every delivered message is settled. It reads
  the subscription at `Receive` start (ordering, dead-letter limit) for
  `RedeliveryAdvisor`. Tests: `pstest` in-process fake.
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
  an in-memory fake of the `sqs.API` interface (the subset of `*sqs.Client`
  used) with visibility and FIFO-group semantics.
- **memory**: Publisher + per-subscription Source with ack/nack/redelivery and
  per-key ordering identical to Pub/Sub ordered delivery.

## 5. Transactional outbox

```go
// core: pubsub/outbox
type Record struct{ ID, Claim int64; Topic string; Message *pubsub.Message; Attempts int }
type Ref struct{ ID, Claim int64 } // Record.Ref(): a settle applies only while the claim is held
type Store interface {
    Claim(ctx context.Context, limit int, lease time.Duration) ([]Record, error) // key runs
    Sent(ctx context.Context, refs []Ref) error
    Failed(ctx context.Context, ref Ref, cause error, retryAt time.Time) error
    Release(ctx context.Context, refs []Ref, cause error, retryAt time.Time) error // no attempt counted
    Dead(ctx context.Context, ref Ref, cause error) error // kept, never claimed again
}
// A settle for a record re-claimed by another relay is a no-op and returns
// *ClaimLostError (wraps ErrClaimLost); the relay logs it and goes on.
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
- `Enqueue` takes `pg_advisory_xact_lock(KeyLockID(key))` per distinct key
  (sorted), so same-key transactions commit in id order (review 2026-11-22,
  M7); a row committed out of id order could otherwise be claimed while a
  lower id of its key was in flight.
- `Claim` takes the **run** of each ordering key starting at its head (oldest
  unsent row, `FOR UPDATE SKIP LOCKED`: only the relay holding the head can
  take the rest) plus unkeyed rows, and leases them; several relays can run
  concurrently without breaking per-key order. Heads come from a bounded age
  window plus a key-by-key index scan that rotates over keys (`_key_idx`,
  `_free_idx`), so a claim is milliseconds even with a large backlog. A
  crashed relay's lease expires and another relay reclaims (at-least-once).
  Every relayed message carries `outbox-id` for consumer dedupe.
- Relay publishes a key's run in one call (Publish bounded to the lease minus
  a settle margin; each settle call has its own timeout), deletes sent rows,
  reschedules failed rows with backoff (transient broker failures do not
  count toward `WithMaxAttempts`), releases the records after a failed one of
  the same key without counting, and calls `KeyResumer.ResumeKey` after a
  failure so the retried head goes first. A record whose publish error wraps `pubsub.ErrPermanent`,
  or that failed `WithMaxAttempts(n)` times (default 0 = unlimited), is marked
  dead (pgx: `dead_at`, row kept for inspection/replay); dead rows are not
  claimed and no longer block their key (review 2026-11-21, G2-3). LISTEN/NOTIFY wakes the relay; polling (default 1s) is the
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
| M18 | Outbox claims only the head row per key (2026-11-22: the head's run, see §5) | single active relay; advisory locks | race-free per-key order with N relays (security/correctness) |
| M19 | Reply IDs from `crypto/rand` (`NewID`) and validated key prefix | caller-chosen IDs | the Redis key is the capability; guessable IDs allow reply spoofing |
| M20 | Outbox table name validated as identifier | free string | prevents SQL injection through configuration |
| M21 | Defaults: Concurrency 10, MaxInFlight 2×Concurrency (was 1000; changed 2026-11-22), MaxRetries 3, backoff 1s×2ⁿ cap 30s ±jitter, HandlerTimeout 60s, ShutdownTimeout 30s, MaxBatchSize 100, FlushInterval 1s | syncmsg's 3s flush / 2m timeout | lower latency; documented zero-value behaviour |
| M22 | Functional options everywhere in this area | Config structs | many optional knobs with defaults |
| M23 | Batch split after whole-batch failure runs each message once (no fresh retry budget) | full retry per split message (syncmsg) | bounded work on poison batches |

### 8.1 Review of 2026-11-21 (group 2)

| # | Finding | Decision |
|---|---|---|
| G2-1 | GCP: the engine's callback returns at once, so the client's per-key sequential callback no longer guards order; a message B received after A was nacked and its key queue drained is handled before A's redelivery (B → A → B). Kafka, SQS FIFO and memory are not affected. | Not fixed: a per-pod in-memory fence was rejected (owner). Documented in `pubsub/gcp`; handlers of ordered GCP subscriptions must tolerate it (version/sequence checks). |
| G2-2 | A handler aborted by the shutdown timeout on its last attempt was treated as failed and could be dead-lettered and acked. | Fixed: always nacked. |
| G2-3 | Outbox records failing permanently retried forever and blocked their key. | Fixed: `Store.Dead`, `WithMaxAttempts`, `ErrPermanent` → dead. |
| G2-5 | Kafka: per-partition OffsetCommit error codes (incl. topic-ID-only responses) were ignored, so a failed commit was never retried. | Fixed. |
| G2-6 | Kafka: a stuck partition head let `pending`/`blocked` grow without bound. | Fixed: `WithMaxPendingPerPartition` (10000) pauses fetching; resumed at half and on revoke/loss. |
| G2-7 | SQS FIFO: after a failed entry or request, later messages of the same group were still sent, so a retry broke FIFO. | Fixed: key pause + `KeyResumer`, like gcp (M4). |
| G2-8 | SQS fair queues (2025): standard queues ignored Key on publish and serialized by group on receive. | Fixed: Key → MessageGroupId; receive keeps Key empty, group in `sqs-message-group-id`. |
| G2-9 | gcp: acks on exactly-once subscriptions were fire-and-forget, so failed acks were silent. | Fixed (superseded 2026-11-22: always AckWithResult, checked in the background; option removed): `gcp.WithExactlyOnce()` uses Ack/NackWithResult and logs failures. |
| G2-10 | gcp: `MaxDurationPerAckExtension` not exposed (redelivery delay after a crash up to 10m). | Added `gcp.WithMaxAckExtension`. |
| G2-11 | gcp: messages traced by the Google client's built-in tracing (`googclient_traceparent`) broke the trace. | Fixed: Source maps `googclient_` trace fields when `traceparent` is absent; keep the built-in tracing off. |
| G2-12 (found by group 3) | Batch mode forgot a failed key as soon as a batch without it arrived, so a message of that key handed over before the nack (Kafka hands records over eagerly) ran before the redelivery. A narrower race (a Source hands a message over right around the nack) also hit the single path. | Fixed (owner): per-Run key fence. A keyed message that is finally nacked fences its key; every other message of the key is nacked (at receive, in the key queue and in batches) until the broker redelivers that message (same ID), or after `WithKeyFenceTTL` (default 1m; 0 disables). Exact for Kafka and SQS FIFO (one consumer per partition/group, same ID on redelivery); for Pub/Sub it also removes G2-1 within one process. |
| G2-4 | Panic log lacked ctx (no trace correlation); pgx `last_error` could be cut inside a UTF-8 rune (rejected by Postgres). | Fixed. |
| G2-13 (D7) | pubsub returned plain errors, so a failed Publish surfaced as a 500 and `ErrKeyPaused`/`ErrClosed` had no code; JWKS fetched with a bare `*http.Client`. | Fixed (owner rule: reuse kitchen packages). `ErrKeyPaused`, `ErrClosed`, `reply.ErrInvalidID` are errs sentinels; `Telemetry.Publish` (shared by every adapter) wraps broker failures as `errs.ErrDependencyUnavailable` unless they already carry an errs code or are context errors; validation errors carry InvalidArgument; outbox/pgx uses `pg.MapError`; JWKS GETs go through `httpx` (jwx's cache still schedules refreshes). OTLP exporters keep their own HTTP/gRPC clients: exporting through `httpx` would trace the exporter's own requests. |

### 8.3 Deep review of 2026-11-22

Full report and repro code: the review notes of the pubsub deep-review
session. Owner decisions:

| # | Finding | Decision |
|---|---|---|
| R-C1 | Key fence + broker dead-letter policy dead-lettered later messages of a key unprocessed (emulator repro). | Fixed: `WithKeyFence` modes with `KeyFenceAuto` driven by `RedeliveryAdvisor`; never fence on the broker's last delivery. |
| R-C2 | kafka: the old owner delivered and committed records of a revoked partition. | Fixed: partition state only from assignment; stale records dropped. |
| R-C3 | sqs: messages held behind backpressure were not visibility-extended. | Fixed. |
| R-H1 | kafka: every cooperative rebalance waited RevokeTimeout (empty revoke = all). | Fixed. |
| R-H2/H3 | Batch isolation ran later messages of a failed key, and fanned out one call per message during outages. | Fixed: skip failed keys, stop after 2 isolated failures in a row. |
| R-H4 | sqs: binary bodies rejected; batches not split at 1 MiB. | Fixed. |
| R-H5/H6, M7–M10 | outbox: hung broker disabled backoff/attempts; one record per poll per key; commit order ≠ id order; O(N) claim; no dedupe id; outages counted as attempts. | Fixed (see migration §3.1). |
| R-H7 | Duration histograms used SDK default buckets. | Fixed: semconv buckets. |
| R-H8 | MaxInFlight 1000 vs Concurrency 10 hoarded leases. | Default derived from concurrency. |
| R-M1 | Fence sweep O(n) per block under the global lock (545µs at 30k fences). | Amortised (≈225ns). |
| R-M (property test) | The fence remembered only the first nacked message and was lifted at receive; later messages could overtake. | Fixed: the fence awaits every nacked message in arrival order and advances in queue order. |
| R-L (outbox F9/F11) | A late settle after the lease lapsed could delete, release or dead-mark a row another relay had re-claimed; `Listen` backoff never reset. | Fixed: claim token fencing (`Ref`, `ErrClaimLost`, pgx `claim_token`); backoff reset on reconnect. |
| R-L (consumer identity) | Metrics could not be split by consumer group/subscription. | Fixed: optional `ConsumerIdentifier` (kafka group, gcp/memory subscription). |
| R-L (declined) | Panic treated as poison (option); outbox option to turn off NOTIFY per Enqueue. | Declined by the owner (2026-11-22): panics stay transient per the settlement contract; NOTIFY stays on. |
| R-L (recorded) | reply/redis holds one connection per waiter; semconv v1.45 renames `messaging.system` → `messaging.system.name` and `gcp_pubsub` → `gcp.pubsub`. | Documented (pool sizing in USAGE); semconv rename waits for an otel-go semconv package ≥ v1.45. |
| R-M2–M4, M11–M16 | sqs serial deletes, shutdown heartbeat, gcp blocking acks, reply BLPOP rounding, metrics/log flood, kafka poison, long jobs, batch shards. | Fixed or documented (USAGE "Long-running handlers", `DeadLetterTo`). |

## 9. Open questions

- CI: the submodules import the core module, which is not published yet, so
  their `go.mod` files do not require it at all (a fake pseudo-version breaks
  module resolution). They build only through a git-ignored `go.work` that
  `use`s the root and the submodules; `GOWORK=off go build` and CI's
  per-module `go mod tidy -diff` fail for them until the core is tagged and a
  `require github.com/dungnh3/kitchen vX.Y.Z` line is added.
- Postgres outbox tests run only when `KITCHEN_TEST_PG_DSN` is set; CI should
  provide a Postgres service.
- Should the SSE outbox (`sse` area) be replaced by `pubsub/outbox`? To agree
  with the SSE session.

### 8.2 Reuse review of 2026-11-21 (owner goal: reuse kitchen packages, one standard; mature library before custom code)

| Topic | Decision |
|---|---|
| Watermill (v1.5.1, last release 2025-09) instead of the engine | Keep the engine (owner): Watermill has no parallel-across-keys/FIFO-within-key processing, no per-message batch results, its official Kafka adapter is Sarama (franz-go only community), OTel is a separate older package. Core pubsub stays OTel-API-only. |
| Five copies of the exponential backoff (subscriber, outbox, outbox/pgx, kafka, sqs) | Replaced by `timex.Backoff` (equal jitter); the reconnect loops of sqs and outbox/pgx gained jitter. |
| Span error recording re-implemented in `pubsub/telemetry.go` | Uses `otelx.RecordError`. Note: pubsub now links package otelx (which also holds the otelgrpc/otelhttp helpers). |
| Client construction | Examples and docs of `reply/redis`, `lease/redis`, `outbox/pgx` use `redisx.Open` / `pg.Open`. |

