# Review: pubsub / syncmsg / syncbrokers / coordinators / queue / scheduler (read-only)

None of these modules have any `_test.go` files.

**Who imports what.** I grepped the sibling repos under `../`.
- **pubsub** is imported by about 35 repos. All of them use `pubsub/gcp`, `gcp/multi_async`, `gcp/multi_batch` or the root interfaces.
- **`kafka_sarama` and `sqs` have no importers.**
- `AsyncPublisher` / `PublishAsync` is not used anywhere. The `PublishAsync(` hits in ms-creative-studio are its own code.
- **No local importers at all** for `syncmsg`, `syncbrokers`, `coordinators`, `scheduler` and `queue`. ms-creative-studio carries its own copy of syncmsg in `pkg/sync_msg`, and ms-orders has its own `pkg/scheduler`. Your "1 repo" counts must come from repos that aren't checked out here.

## 1. pubsub

**API.** The root `pubsub/pubsub.go:8-85` defines:
- `SubscriberMessage`: ID, Value, Key, Topic, Timestamp, `Commit(any)`, Metadata, ReceiveCount
- `Subscriber`: `Run` / `Close`
- `Publisher`: `Publish`, `PublishWithAttributes`, `PublishWithOptions`
- optional `AsyncPublisher` / `PublishResult`

Backends: `gcp.Publisher`, `gcp/multi_async`, `gcp/multi_batch`, `kafka_sarama` (consumer only), `sqs` (subscriber only).

**Abstraction quality**
- The common interface is only partly real. Nack is not in the interface; it exists only on the concrete GCP type (`gcp/message.go:39`). Callers have to type-assert or rely on the ack deadline.
- Kafka doesn't implement `pubsub.Subscriber` handlers at all. It has its own `Handler(ctx, *ConsumerMessage) error` (`kafka_sarama/consumer_group.go:11`).
- Handlers have no `ctx` and return no error (`pubsub.go:8,10`). Ack/nack is left entirely to user code, and context and trace can't be propagated.
- `Metadata() any` returns `map[string]string` on GCP and `nil` on SQS and Kafka.
- There is no GCP publisher counterpart for Kafka or SQS.

**Bugs: Run never returns, plus channel misuse**
- **sqs `Run`** (`sqs/subscriber.go:159-209`): on ctx cancel the receiver does `errChan <- err`, which is unbuffered, before its deferred `close(msgChan)`. Meanwhile `Run` is blocked in `wg.Wait()`, and the workers are waiting for `msgChan` to close. The result is a deadlock and graceful shutdown never finishes.
- **kafka `ConsumerGroup.Run`** (`kafka_sarama/consumer_group.go:30-73`) and **`BatchConsumerGroup.Run`** (`batch_consumer_group.go:40-83`) have the same deadlock.
- **multi_async** (`gcp/multi_async/subscriber.go:62-108`) has the same deadlock. It also has two senders on `errChan`: the second blocks forever, or panics after `defer close(errChan)`. The `Receive` callback can send on `msgChan` after it has been closed, which panics.
- `Run` uses a value receiver, so `s.ctx = ctx` at line 51 is lost.
- **multi_async** returns from the GCP callback right after handing the message to a worker. Ordering is lost even with ordering keys.

**Bugs: multi_batch** (`gcp/multi_batch/subscriber.go`)
- The handler has no error return. Retries happen only on panic (lines 335-370) and re-run the whole batch, including messages already acked.
- On ctx cancel, the worker returns (line 358) while the handler goroutine at line 339 keeps running, so that goroutine is orphaned.
- `finalFlush` → `enqueueBatch` selects on an already-done ctx (lines 429, 487), so the "final flush" often drops the batch. The messages are only redelivered after the ack deadline.
- Possible send-on-closed-channel: the flusher closes `msgChan` at line 298 while `state` can still be `Running` if the parent ctx was cancelled, and an in-flight Receive callback can then flush at line 464.
- `Config.ErrorChannelSize` and `MaxRetries` are reused for unrelated purposes.

**Bugs: kafka**
- `Commit` has an inverted type check (`consumer_message.go:36-38`), so the metadata is always `""`.
- N workers mark offsets out of order (`consumer_group.go:68`). Marking a later offset commits past earlier unprocessed ones, which gives at-most-once on crash and no per-partition ordering.
- Handler errors are only logged (lines 63-65): no nack or retry.
- The batch consumer's default `workerNo` is 0 (`batch_consumer.go:26-34`), so nothing is ever processed.
- The batch consumer has one ticker shared by every `ConsumeClaim` goroutine (`batch_consumer_group.go:111-114`), which races on Stop/Reset.
- It sends on an unbuffered channel while holding the mutex (line 126).
- It buffers messages across rebalances, so commits go through a stale session.
- `Close` is a no-op (`consumer.go:104`).

**Bugs: sqs**
- `WithMaxNumMsg` is never used: `MaxNumberOfMessages` isn't set at line 167, so it defaults to 1 message per poll.
- Any single receive error kills the subscriber (line 174), with no backoff.
- Empty-body messages are skipped and never deleted (line 201).
- There is no visibility-timeout extension and no batch delete.
- `Close` is a no-op.

**Publisher** (`gcp/publisher.go`)
- It is reasonable: cached topic handles, `Close` flushes, `EnableMessageOrdering` is set before first use.
- It auto-resumes the ordering key after a failure (line 268-274). That trades FIFO for availability, and it contradicts the syncmsg publisher (`syncmsg/gcp/publisher.go:82-86`), which deliberately does not resume.
- **No trace injection** into attributes. `log` is the only extra dependency.

**Dependencies**
| Dependency | Used | Latest | Notes |
|---|---|---|---|
| `cloud.google.com/go/pubsub` | v1.50.1 | v1.52.0 (Nov 4 2026) | pkg.go.dev marks the v1 module **deprecated** ([v1 versions](https://pkg.go.dev/cloud.google.com/go/pubsub?tab=versions)) |
| `cloud.google.com/go/pubsub/v2` | v2.0.0, already pulled in as indirect | v2.9.0 (Nov 10 2026) | [v2 versions](https://pkg.go.dev/cloud.google.com/go/pubsub/v2?tab=versions) |
| `aws/aws-sdk-go` | v1.55.3 | none | End-of-support 2025-07-31, repo archived ([AWS blog](https://aws.amazon.com/blogs/developer/announcing-end-of-support-for-aws-sdk-for-go-v1-on-july-31-2025)) |
| `aws-sdk-go-v2/service/sqs` | not used | v1.53.0 (Nov 10 2026) | [versions](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/sqs?tab=versions) |
| `IBM/sarama` | v1.43.2 | v1.62.0 (Nov 20 2026) | [versions](https://pkg.go.dev/github.com/IBM/sarama?tab=versions) |
| franz-go | not used | v1.21.0 per README | Date unverified ([repo](https://github.com/twmb/franz-go/)) |

**Recommendation: REDESIGN.** Make it GCP-first on pubsub/v2. Drop `kafka_sarama` and `sqs` unless the owner confirms they're needed; if Kafka comes back, use franz-go.

Target API sketch:
```go
type Message struct{ ID, Key string; Data []byte; Attrs map[string]string; PublishTime time.Time; DeliveryAttempt int }
type Handler func(ctx context.Context, m *Message) error            // nil=ack, err=nack, ErrPermanent=ack+DLQ
type BatchHandler func(ctx context.Context, ms []*Message) BatchResult // per-message ack/nack
type Publisher interface { Publish(ctx, topic string, m *Message) (id string, err error); PublishAsync(...) Result; Close(ctx) error }
type Subscriber interface { Run(ctx) error /* returns after drain */ }
```
Add OTel inject/extract middleware following messaging semconv. Note the messaging semconv is still in "Development" status ([OTel](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans)), so pin a version. Add an in-memory backend for tests.

## 2. syncmsg (+gcp)

**Purpose.** A generic batching consumer `Subscriber[T]` with `Source` / `RawMessage` / `Decoder[T]` / `Handler[T]`, retry with jittered exponential backoff, poison-message isolation by splitting the batch, `ErrPermanentDecodeFailure`, metrics/trace hooks and a bounded drain (`syncmsg/source.go`, `subscriber.go`, `worker.go`). There is also an OTel-injecting ordered GCP publisher (`gcp/publisher.go:72-73`).

This is the **best-engineered package in scope**: lifecycle under `stopOnce`, a `msgChanClosed` guard (`buffer.go:58`), a drain ctx that is independent of the receive ctx.

**Issues**
- The GCP adapter breaks the `Source` contract that "fn MUST be called synchronously" (`source.go:18`). In the v1 client, callbacks run concurrently up to `MaxOutstandingMessages` even with `NumGoroutines=1` (`gcp/source.go:96-98`), so the comment is wrong.
- `PreserveOrdering` therefore doesn't guarantee order unless the subscription itself has ordering enabled.
- Splitting a batch after its retries are exhausted (`worker.go:94-121`) replays messages that may already have side effects, and each split message gets the full retry/backoff again. Handlers must be idempotent; this isn't documented strongly.
- The trace extractor uses only the first message's attributes (`worker.go:143-145`). It should create one span per message, or span links.
- The doc says HandlerTimeout "Default: 5m" (`config.go:53`) but the code sets 2m (`config.go:86`). The Handler doc says "Called once per batch after all retry attempts" (line 93), which is wrong.
- `GCPSource.Close` is a no-op.
- It duplicates `pubsub/gcp.Publisher`.

**Dependencies:** pubsub v1.50.1 (deprecated module), otel v1.39.0 (latest not verified).

**Recommendation: MERGE.** Make syncmsg's core, meaning the Source seam, the batch engine, the retry and decode-error semantics, and the trace hooks, the basis of the new `pubsub` subscriber. Replace `multi_batch` and `multi_async` with it. Keep a single publisher.

## 3. syncbrokers

**Purpose.** A request/reply "wait for async result" on Redis pub/sub. `Wait(ctx, id, timeout)` blocks until a message arrives on `reply:<id>` (`syncbrokers/broker.go:326`). There is no reply/publish side in the library. The package name is `syncbroker` but the directory is `syncbrokers`.

**Critical bugs**
- **`sync.Pool` reuse while a waiter still holds the entry.** `notify` and `timeoutWaiter` send to `entry.ch`, then `returnWaiterEntry` immediately drains the channel (lines 207-208, 191-194, 225-228). If the waiting goroutine isn't already parked on the receive, the result is stolen, and `Wait` hangs until ctx ends. With no deadline it hangs forever while holding a semaphore slot. The recycled entry can also be handed to another ID, so results can cross between requests.
- `cleanupWorker` and `Stop` recycle entries without notifying waiters (lines 305-314, 364-377).
- `Stop` double-closes on a second call and uses `time.Sleep(100ms)`.
- The listener gives up permanently after 5 reconnects over the process's whole lifetime: `retries` is never reset (line 238).
- `ReplyKeyPattern` is configurable but the prefix `"reply:"` is hard-coded (line 254).
- It's inherently racy: Redis pub/sub is fire-and-forget, so a reply published before `Wait` registers is lost.
- `PSubscribe reply:*` fans every reply out to every instance.

**Dependencies:** go-redis v9.14.0, wrongly marked `// indirect` in go.mod. Latest stable is v9.23.0 (Oct 5 2026) ([versions](https://pkg.go.dev/github.com/redis/go-redis/v9?tab=versions)).

**Recommendation: DROP, or REDESIGN if it's actually used.** If request/reply is needed:
- register before publishing, as in `Call(ctx, req) (Result, error)`;
- use a per-instance reply channel or a Redis Stream / `BLPOP` on a per-ID key with a TTL, which avoids the lost-reply race;
- no pooling.

## 4. coordinators

**Purpose.** Redis lease-based shard ownership: each pod owns at most one shard, using `SETNX` with TTL plus a Lua check-and-`EXPIRE` renewal, with `onShardAcquired` / `onShardLost` callbacks (`coordinators/coordinator.go`). It uses the newer `kitchen/l` logger and `pkg/errors`.

**Issues**
- No fencing token. The callbacks run as unordered goroutines (lines 229, 287), so a "lost" callback can run after a later "acquired" one, and two pods can briefly both consume a shard.
- A single transient Redis error drops the shard and grabs another (line 218-236) while the old key stays live, so this pod still holds the old lease.
- `EXPIRE` takes whole seconds (line 350): sub-second TTLs truncate, and a TTL under 1s becomes 0. It should use `PEXPIRE`.
- `Stop` neither releases the lease nor calls `onShardLost`.
- `Start` blocks the caller with `time.Sleep` for 0-4s (line 115).
- `rand.NewSource` result is discarded (line 55).
- One shard per pod means shards go unowned when there are fewer pods than shards.
- `SetCallbacks` isn't synchronized.

**Recommendation: DROP** if no repo uses it. Otherwise REDESIGN as a generic `Lease` / `Elector` with fencing tokens and owner-checked release. Alternatives: k8s `coordination.k8s.io` Lease via client-go `leaderelection`, or Pub/Sub ordering keys or Kafka consumer groups, which make manual sharding unnecessary.

## 5. scheduler

**Purpose.** Runs several `Processor`s, each a fixed-delay loop (`scheduler/scheduler.go`).

**Issues**
- One processor's panic stops all of them (line 62-65).
- With zero processors, `Run` blocks forever (line 80).
- `process` never returns an error, so only panics surface.
- No distributed lock: it runs on every replica. No per-execution timeout, no jitter. `Close` is a no-op. go 1.22.5.

**Recommendation: REDESIGN or DROP.** ms-orders already has its own version. If kept: `Run(ctx)` with an errgroup, per-job timeout, cron or interval, optional leader-lease from the coordinators redesign. Otherwise point users at gocron v2 or Cloud Scheduler → Pub/Sub. Version not verified.

## 6. queue

It's an empty module, just a go.mod (`queue/go.mod`). **DROP.**

## 7. How the packages overlap

- **pubsub vs syncmsg:** both are batch consumers on GCP. syncmsg is a cleaner rewrite of multi_batch, and there are two GCP publishers with opposite ordering-key resume policies. → Merge into one.
- **syncbrokers** is synchronous RPC over async messaging, a different concern.
- **coordinators** is partition assignment, a different concern.
- None of them duplicate each other apart from pubsub and syncmsg.

## 8. Trends

- **Watermill** v1.5.3 (Aug 25 2026, [versions](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill?tab=versions)) already provides a Publisher/Subscriber abstraction, GCP/Kafka/SQL backends, a router with retry/poison/DLQ middleware and an **SQL-based outbox (forwarder)**. Wrapping it is a credible alternative to maintaining your own engine, at the cost of per-message ack/nack instead of batch.
- **gocloud.dev/pubsub** and **CloudEvents SDK**: worth considering for a portable envelope, e.g. a `ce-` attribute binding on Pub/Sub. Versions not verified.
- **Transactional outbox**: nothing like it exists here. Several services publish after DB writes, so an outbox (Postgres table plus relay) would be a high-value addition to the new repo.
- **DLQ**: rely on the Pub/Sub dead-letter policy (`maxDeliveryAttempts`), which syncmsg documents (`config.go:38-42`). Surface `DeliveryAttempt` in the core API.

## 9. Open questions for the owner

1. Is Kafka or SQS used anywhere in production, including repos not checked out here? Locally, nothing imports `kafka_sarama` or `sqs`.
2. Who uses `syncbrokers`, `coordinators`, `scheduler` and `kitchen/syncmsg`? Your "1 repo" counts aren't visible locally. If it's a repo not checked out here, what are the request/reply latency and shard-count requirements?
3. Is ordering-key FIFO a correctness requirement anywhere? It decides whether the publisher auto-resumes paused keys or leaves it to the caller.
4. Do GCP subscriptions in production have dead-letter topics or `maxDeliveryAttempts` configured?
5. Is it acceptable to break the handler signature (adding `ctx` and an `error` return) for about 35 consuming repos, or is a compatibility shim needed?
6. Do services need publish-after-commit guarantees, i.e. an outbox?
7. Should OTel trace propagation be on by default in the new library? Today only syncmsg injects it.
