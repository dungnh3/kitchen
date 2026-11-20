# kitchen/sse review (read-only)

Module `github.com/vulcanlabsvn/kitchen/sse`, `go 1.24.0`, one go.mod. 17.0K lines of Go including tests; `manager.go` 1277, `redis.go` 776, `outbox/relay.go` 884. It also carries about 6.1K lines of markdown docs (USAGE.md 3252, HOW_TO_USAGE.md 2171, technical report 725). No other kitchen module imports it.

## 1. Package map and data flow

| Pkg | Role |
|---|---|
| `sse` (root) | Wire `Event`, `Connection`/`Upgrade` (connection.go), Config, Hooks, event types, visibility, middleware, `NewEventBus` factory (config.go:186) |
| `envelope`, `envelopefilter` | Typed JSON envelope + builder; validator / strict token-only filter |
| `eventbus` | `EventBus` interface (eventbus.go:86): Publish, **PublishWithMirror**, Subscribe, Publish/SubscribeInteraction (HITL `:interact`), Done, **Expire**, Close |
| `eventbus/redis` | Redis Streams impl: XADD MAXLEN~ + EXPIRE in MULTI, Lua dual-write (publish_mirror.lua), XREAD BLOCK loop with gap detection, replay-done sentinel, transient retry, adaptive COUNT |
| `eventbus/producer` | Stateless server-to-server writer (Direct / Mirror-atomic / Split-best-effort), errors go to hooks rather than being returned |
| `eventbus/outbox` (+`pubsubgcp`, `promhooks`) | Writer + Relay: XREADGROUP consumer group, per-ordering-key lanes, XAUTOCLAIM, DLQ via Lua, Pub/Sub publisher |
| `stream` | Manager (Pipe/Publisher/Admit/Shutdown), per-pod `streamHub` fanout, publisher-side aggregation, filters/enrichers/aggregator/ResumeStore, token coalescing buffer |
| `metrics` | Prometheus `sse.Hooks` |

**Flow.** Runtime `mgr.Publisher(ctx,id).SendTyped` (or `producer`) does XADD on `sse:{id}`, optionally mirroring to the outbox stream, which the Relay sends on to Pub/Sub. On the gateway side, `Admit` returns a ticket, then `Upgrade`, then `Pipe`:
- **No Last-Event-ID:** the client attaches to the pod-local hub. There is one XREAD per stream per pod, starting from `"0"` (redis.go:344). A 128-entry ring is replayed to late joiners (hub.go:25, 262).
- **With Last-Event-ID:** the client gets a dedicated `bus.Subscribe` (manager.go:348). Gap check uses XINFO bounds; a gap emits `__sse_replay_gap__`. A replay-done marker becomes `stream.resumed`.

Each connection runs a reader goroutine and a writer goroutine with a bounded channel (default 64). Slow-client policy is Disconnect (default), Drop or Block. A clean end sends `stream.done`.

Heartbeat is a separate goroutine per connection: `: ping` with jittered start (connection.go:123,144). Flushing uses `http.ResponseController` with a per-write deadline. Aggregation is either per connection (consumer side) or per publisher.

## 2. Design review

**Scalability (30K CCU)**
1. **The hub only pays off when several viewers watch the same stream.** For agent chat (usually one viewer per run), Redis read connections stay O(connections): one blocking XREAD holds one pool connection (redis.go:430). The PoolSize guard (redis.go:93) and default 8192 (config.go:178) acknowledge this.
2. **Reconnect storms bypass the hub for the whole life of the stream.** A reconnect is not just a replay phase. During a rolling deploy, roughly 30K clients come back with Last-Event-ID, so each gets a dedicated XREAD (manager.go:348), and each of those first makes synchronous XINFO round-trips.
3. **Fix for both:** a per-pod multiplexed reader, i.e. one `XREAD BLOCK COUNT n STREAMS k1..kN id1..idN` loop per shard of streams. Do replay with XRANGE, then hand the client over to the hub.
4. **Single-node client only** (`goredis.NewClient`, redis.go:101); there is no Cluster or Sentinel support. The mirror Lua writes `sse:<id>` and a global outbox key in one script, which fails with CROSSSLOT on Cluster and makes a single hot outbox key. The Relay accepts `UniversalClient` and has `hashTag` (relay.go:851), so the two halves are inconsistent.
5. **Global `m.mu`** is taken on every Pipe/Admit/Publisher/hub-empty. Its critical sections are short (no I/O under the lock), so contention is tolerable, but it is still one mutex across 30K connections. Shard it by streamID hash.

**Correctness**
6. **Late-join history is inconsistent.** The first subscriber of a new hub replays the whole stream from `"0"` (up to MaxLen 10K). With BufferSize 64 and PolicyDisconnect, that burst can disconnect the first client immediately (hub.go:145 fanout is non-blocking). A second same-pod joiner silently gets only the last 128 entries.
   - The doc comment says `""` means `">"` (eventbus.go:54); the code uses `"0"`.
7. **Silent data loss paths.** PolicyBlock is downgraded to Drop at the hub (hub.go:134). PolicyDrop drops tokens mid-text (manager.go:450,682). `MaxAge` stale entries are skipped without telling the client (redis.go:489). For LLM token streams, dropping must be a disconnect followed by resume, never a silent gap.
8. **Consumer-side aggregation runs per connection.** `OnAggregated` (manager.go:1029) fires once per tab, and on a resumed or ring-truncated connection it aggregates a partial message. The doc.go example persists this with `db.SaveMessage`, which can produce duplicate or partial persistence. `ResumeStore.Save` is likewise called per connection per token (manager.go:957).
9. **Shutdown** resets `streamConnCount` (manager.go:1152,1229), then Pipe defers decrement it, leaving negative, never-deleted entries. Harmless after close, but sloppy.
   - `Shutdown` and `ShutdownWithOptions` are near-duplicates.
   - Both return nil when the drain times out (the timeout error is swallowed).
10. **Heartbeat** write errors are ignored (connection.go:144), so a dead socket is detected only by the next data write. It also sends pings during active token flow (wasted bytes and syscalls at scale).
    - `Connection: keep-alive` (connection.go:98) is invalid on HTTP/2.
11. **Client-supplied Last-Event-ID** goes straight into XREAD without format validation. The streamID trust boundary is pushed onto callers (doc.go "Security").
12. **`pipeBase` and `pipeExtended`** duplicate the reader loop and slow-client logic (manager.go ~420–470 vs ~660–700), so bug fixes must be applied twice.

**Outbox.** The relay is solid: XREADGROUP, ack-after-publish, DLQ+XACK in Lua, per-key lanes with Pub/Sub `ResumePublish`, XAUTOCLAIM, trim detection, lag hooks.
- It is not a transactional outbox: the source of truth is Redis, not the DB transaction. ModeSplit is explicitly best-effort.
- Redis needs `noeviction` plus MINID retention to avoid loss (outbox/doc.go). The "outbox" name will mislead; call it a "Redis-stream relay".

**Overlap with `pubsub/`.** `outbox/pubsubgcp` re-implements a GCP publisher (ordering, classify) that the `pubsub/` module (gcp, kafka_sarama, sqs; Publisher/AsyncPublisher) already abstracts. The Relay should target `pubsub.AsyncPublisher`. Both modules pin pubsub **v1.50.1**.

**API complexity.**
- Two constructors (`NewManager` vs `NewManagerWithOptions`) choose different code paths.
- There are four extension "groups", and three aggregation modes (consumer, publisher, none).
- `EventBus` leaks Redis concepts (Mirror, Expire, Interaction), so a NATS or memory implementation is awkward.
- 6K lines of docs is a smell.

**Tests.** 34 files. Relay tests are strong (35 tests; one Redis-7 test is gated by `OUTBOX_TEST_REDIS_ADDR`, relay_test.go:1044). There are review-ID regression comments (KS-R4-01, EB-R3-03).
- Gaps: **no benchmarks**, no load or soak test for 30K, and no goroutine-leak checks (goleak).
- Mostly miniredis (5 files), which does not model Cluster or real blocking semantics. `time.Sleep` appears in 10 test files.

## 3. Dependency freshness

Checked against proxy.golang.org `/@latest` on 2026-11-20.

| Dep | Pinned | Latest |
|---|---|---|
| go-redis/v9 | v9.19.0 | **v9.23.0** (2026-10-05) |
| miniredis/v2 | v2.38.0 | v2.40.0 |
| oklog/ulid/v2 | v2.1.1 | v2.1.2 |
| client_golang | v1.23.2 | **v1.25.0** |
| cloud.google.com/go/pubsub | v1.50.1 | v1.52.0; **v1 deprecated, support ends 2026-12-31** ([pkg.go.dev](https://pkg.go.dev/cloud.google.com/go/pubsub)). v2 latest v2.9.0, but only v2.0.0 is in the indirect deps |
| grpc | v1.74.2 | v1.85.0 (only `codes`/`status` used, pubsubgcp.go:23) |
| Go | 1.24 | go1.27.4 ([go.dev/dl](https://go.dev/dl/?mode=json)) |

**Action: migrate to pubsub v2 now; this is urgent.**

## 4. Modern trends and recommendation

- **Stdlib:** `ResponseController` with Flush and SetWriteDeadline is already modern. Possible additions:
  - Go 1.23 iterators: `iter.Seq2[Message, error]` instead of the `(msgC, errC)` channel pair, which removes the subtle drain-ordering code.
  - `sync.OnceFunc`, `context.AfterFunc` for detach.
  - `testing/synctest` (Go 1.25) to replace sleeps.
- **Specs:**
  - [WHATWG SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html): `id` / `retry` / comment heartbeat are handled.
  - [AI SDK UI message stream](https://ai-sdk.dev/docs/ai-sdk-ui/stream-protocol): needs the `x-vercel-ai-ui-message-stream: v1` header, typed `text-delta`/tool parts, and a final `data: [DONE]`. Resume there is done app-side with Redis ([resume docs](https://app.unpkg.com/ai@6.0.146/files/docs/04-ai-sdk-ui/03-chatbot-resume-streams.mdx)). An envelope adapter to this format would help browser clients.
  - [MCP 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28/changelog) **removed** Last-Event-ID resumability and sessions from Streamable HTTP, so do not couple the bus to MCP resume semantics.
- **Alternatives:**
  - Redis 7 sharded Pub/Sub (`SSUBSCRIBE`) for live fanout plus Streams for replay: one connection per shard, not per stream.
  - NATS JetStream: per-subject replay, consumer per client, built-in multiplexing.
  - Centrifugo: off-the-shelf history and recovery, which would replace most of `stream/`.

**Verdict**
- **KEEP:** wire protocol / `Connection`, envelope, outbox Relay core (re-target to `pubsub.AsyncPublisher`), gap and replay-done semantics.
- **UPDATE:** deps (pubsub v2), Cluster support with hash tags `sse:{id}`, heartbeat inside the writer loop, validation of Last-Event-ID, a single Pipe path.
- **REDESIGN:** the subscription layer.

**Target sketch:**
```
Bus interface: Append(ctx,id,msg) / Range(ctx,id,from,n) / Watch(ctx, ids...) iter
Pod reader: N shards × one XREAD BLOCK STREAMS k1..kN (or SSUBSCRIBE)
  → hub per stream (unbounded-free ring, size=MaxLen policy)
Client: if LastEventID → XRANGE catch-up → join hub at cursor (no dedicated reader)
Slow client: disconnect+resume only (no silent drop for token types)
Aggregation: publisher-side only (one writer, exactly-once-ish persist)
```
Target Redis connections per pod: O(shards) rather than O(streams).

## 5. Open questions for the owner

1. What is the real viewers-per-stream ratio and the active-streams-per-pod count at 30K? That decides whether the hub alone is enough.
2. Is Redis standalone, Sentinel or Cluster (or Memorystore) in prod? Is the maxmemory-policy `noeviction`?
3. Is anything downstream using `PolicyDrop`/`PolicyBlock`, consumer aggregation, or `ResumeStore` in production?
4. Should late joiners without Last-Event-ID get full history, the last N events, or live only?
5. What is the Pub/Sub outbox for (analytics/projection)? Are at-least-once delivery and possible reordering after takeover acceptable to its consumers?
6. Must the browser protocol conform to AI SDK UI stream or AG-UI, or is a custom envelope fine?
7. Is the 30K target backed by a load test? None exists in this repo.
8. Should the new repo keep the HITL `:interact` sub-stream inside the SSE lib or move it out?

Sources: [go-redis release notes](https://github.com/redis/go-redis/blob/master/RELEASE-NOTES.md), [pubsub v1 deprecation](https://pkg.go.dev/cloud.google.com/go/pubsub), [pubsub v2](https://pkg.go.dev/cloud.google.com/go/pubsub/v2), [MCP changelog 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28/changelog), [AI SDK stream protocol](https://ai-sdk.dev/docs/ai-sdk-ui/stream-protocol), [client_golang](https://pkg.go.dev/github.com/prometheus/client_golang). Version numbers come from `https://proxy.golang.org/<module>/@latest`.