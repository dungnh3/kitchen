# Design: `sse` (submodule `github.com/dungnh3/kitchen/sse`)

Status: approved by the owner on 2026-11-20. Every "(Recommended)" option was
accepted; see "Decisions" and "Decisions taken by default (to review)".
Inputs: `docs/review/04-sse.md`, a survey of the 7 downstream services
(ms-agent-gateway, -orchestrator, -execution-engine, -smith-nexus, -seleya,
-creative-studio, -writing-suite), and the versions/specs below, all checked on
2026-11-20.

## 1. Goals and load profile

- Stream agent output (LLM tokens + lifecycle events) from backend writers to
  browsers through Redis Streams, **30K concurrent connections**, about
  **1 viewer per stream** and **3–10K streams per gateway pod**.
- No silent data loss: a viewer either receives every event in order or gets
  an explicit terminal signal (`stream.replay_gap`), never a hole.
- Redis connections per pod are O(readers), not O(connections).
- Keep what was sound in the old module (wire format, envelope, the relay's
  delivery rules, gap and resume semantics). Fix every defect listed in the
  review; the migration guide lists each one as a behaviour change.

## 2. Package layout

```
sse/                 module github.com/dungnh3/kitchen/sse (go 1.26.0)
├── sse              wire Event/AppendEvent, Message, Encoder, Server (Admit/Serve/Shutdown),
│                    Transform, lifecycle event types; stdlib + OTel API only
├── envelope         Envelope JSON, Builder, ULID ids, Encoder (token unwrap),
│                    ContextValidator transform
├── aisdk            Encoder for the Vercel AI SDK UI message stream (v1 header)
├── redisbus         Keyspace (bucketed hash tags), Producer (Append/End + outbox leg),
│                    Hub (multiplexed per-pod reader, implements sse.Source)
├── relay            Redis-stream relay: outbox shards → Publisher (Pub/Sub, ...)
└── internal/redisx  shared stream-ID parsing and slot partitioning
```

Dependencies: `github.com/redis/go-redis/v9` v9.23.0, `go.opentelemetry.io/otel`
(+`/metric`) v1.47.0, tests: `github.com/alicebob/miniredis/v2` v2.40.0,
`go.uber.org/goleak` v1.3.0. **No GCP SDK and no Prometheus client.** The relay
targets a local `Publisher` interface (D5) that the messaging area's
`pubsub/gcp` (pubsub v2) publisher satisfies via a small adapter.

## 3. Data model and keyspace

```
stream key  = Prefix + "{b" + fnv32a(streamID) % Buckets + "}:" + streamID   // sse:{b17}:run_01HX…
outbox key  = Outbox + ":{b<k>}"                                          // outbox:orch:{b17}
dlq key     = outbox key + ":dlq"
wake key    = Prefix + "{b<k>}:__wake:" + hubID + ":" + readerID           // hub-internal
entry       = XADD fields: type=<event type>, data=<payload>  (end sentinel: type=__sse_end__)
outbox entry= type, data, stream=<streamID>
```

- `Buckets` (default **64**) is part of the key, so all writers and readers must
  share it. Streams in one bucket share a hash slot, so one multi-key `XREAD`
  can serve many streams on Redis Cluster. On standalone/Sentinel any keys mix.
- The stream ID (`sse.Message.ID`) is the Redis entry ID (`<ms>-<seq>`). It is
  the SSE `id:` field and the only accepted `Last-Event-ID` format (validated).

## 4. Write path: `redisbus.Producer`

```go
p, err := redisbus.NewProducer(rdb redis.UniversalClient, redisbus.ProducerConfig{
    Keyspace: redisbus.Keyspace{Prefix: "sse:", Buckets: 64},
    MaxLen: 10_000, ActiveTTL: 24*time.Hour, DoneTTL: 2*time.Minute,
    Outbox: &redisbus.OutboxConfig{Stream: "outbox:orch", MinIDRetention: 6*time.Hour},  // Mirror
    // or OutboxClient: other, for Split mode
})
id, err := p.Append(ctx, runID, sse.Message{Type: "stream.token", Data: b})
err = p.End(ctx, runID)                // idempotent
```

- `Append` is one `EVALSHA`: check the last entry (`XREVRANGE + - COUNT 1`), and
  if it is the end sentinel return `ErrStreamEnded`; otherwise `XADD MAXLEN ~`,
  `EXPIRE ActiveTTL`, and in Mirror mode `XADD` to the bucket's outbox shard
  (same hash tag, so it is atomic on Cluster too). One round trip.
- **Split mode** (`OutboxClient` set): main append, then a best-effort `XADD` on
  the other client. A failed second leg returns the ID **and** `ErrOutboxDiverged`.
- `End` appends the sentinel unless already ended, then sets `EXPIRE DoneTTL`.
- Errors are always returned. There is no per-stream state in the process, so it
  is safe in retried/replayed durable workflow steps.

## 5. Read path: `redisbus.Hub` (implements `sse.Source`)

```go
hub, _ := redisbus.NewHub(rdb, redisbus.HubConfig{Keyspace: ks})
go hub.Run(ctx)            // returns after all readers stop
for msg, err := range hub.Subscribe(ctx, runID, lastEventID) { … }
```

```
                ┌──────────── pod ─────────────┐
 Redis ◄─XREAD BLOCK COUNT n STREAMS k1..kN ids─ reader[g][i]   (≤ MaxKeysPerRead keys each)
                │        │ append to ring, broadcast            │
                │   stream state (ring, lastID, ended)          │
                │        ▲ cursor-based pull (no channels/drops)│
                │   subscriber: XRANGE catch-up → ring → wait   │
                └───────────────────────────────────────────────┘
```

- **Readers.** Streams are grouped by slot compatibility: on a `*ClusterClient`
  a group is one bucket; otherwise there is one group. Each group holds readers
  of at most `MaxKeysPerRead` (512) streams; readers are created and retired on
  demand. Each reader loops `XREAD BLOCK Block(2s) COUNT Count(256)` over its keys
  plus its own **wake key**. Adding a stream to a reader `XADD`s to the wake key
  (same slot), so the blocking read restarts immediately and the first live
  token is not delayed.
- **Registration.** The first subscriber of a stream reads the current last ID
  (`XREVRANGE COUNT 1`) and registers the stream with that cursor. The reader
  reads only newer entries; history is read by subscribers themselves.
- **Per-stream state.** Bounded ring (`RingSize`, 512) of recent entries covering
  `(base, last]`, plus `ended`, plus a broadcast channel replaced on append.
- **Subscriber (cursor pull).** With cursor `C`: entries in the ring after `C` →
  deliver; `C` older than the ring → `XRANGE` from Redis in pages; `C == last` →
  wait for the broadcast. Delivery is paced by the consumer, so a slow client
  never blocks the reader, never drops data and never holds unbounded memory.
- **Resume / gap.** For a non-zero `C`, the first page is `XRANGE C + COUNT n`
  (inclusive). If its first entry is not `C`, the cursor was trimmed or expired:
  yield `ErrReplayGap`. `C` beyond the stream's last ID is `ErrInvalidCursor`.
  After a resumed catch-up reaches live, a synthetic `stream.resumed` message
  (no ID) is yielded once.
- **Late joiner without `Last-Event-ID`:** full replay from the start of the
  retained stream (bounded by `MaxLen`), identical for every viewer.
- **End:** the sentinel ends the sequence cleanly (nil error); the stream leaves
  its reader. The state is dropped when the last subscriber leaves.

## 6. HTTP: `sse.Server`

```go
srv, _ := sse.NewServer(hub, sse.ServerConfig{MaxConnections: 10_000, MaxConnectionsPerStream: 20})
mux.HandleFunc("GET /runs/{id}/stream", func(w http.ResponseWriter, r *http.Request) {
    t, err := srv.Admit(r.PathValue("id"))           // 503 / 429 before headers
    if err != nil { … }
    err = srv.Serve(w, r, r.PathValue("id"), sse.ServeOptions{
        Ticket: t,
        Encoder: envelope.NewEncoder,                // or aisdk.NewEncoder
        Transforms: []sse.TransformFactory{envelope.ContextValidator(rule)},
        Visible: myVisibility,
    })
})
defer srv.Shutdown(ctx)
```

- `Serve` validates `Last-Event-ID` before committing headers
  (`ErrInvalidCursor` → caller returns 400), sets `Content-Type`,
  `Cache-Control: no-cache`, `X-Accel-Buffering: no` (no `Connection` header:
  invalid on HTTP/2), flushes, writes `retry:`.
- One pump goroutine pulls from the source into a bounded 64-slot hand-off (it blocks, never drops); the handler
  goroutine is the only writer. It **coalesces** everything ready into one write
  and one `Flush` (optional `FlushInterval`), keeps the event IDs intact, and
  sends `: ping` only after `Heartbeat` (15s) of **idle** time. Every write has a
  `WriteTimeout` deadline; any write error, heartbeat included, ends `Serve`.
- `Transform func(ctx, *Message) (Action, error)` built per connection by a
  `TransformFactory`: `Pass`, `Skip`, or `Stop` (write the possibly rewritten
  message, then end). It replaces the old filters, enrichers and envelope filter.
- `Visible func(type) bool`: default shows only the library's `stream.*` types;
  services declare their own types (safe default, unknown = hidden).
- `Encoder` (one per connection) maps a `Message` to wire events and writes the
  terminal events: `stream.done` on clean end (once, even if the writer already
  sent one), `stream.replay_gap` on gap, `stream.error` on idle timeout, and
  **nothing** on shutdown, so EventSource reconnects to another pod and resumes.
- `Admit` uses an atomic global counter plus 64-way sharded per-stream counters;
  tickets release idempotently. `Shutdown(ctx)` refuses new admits, ends active
  `Serve` calls and returns `ctx.Err()` if the drain times out.
- `MaxIdle` (optional) ends a connection whose stream produced nothing for that
  long (replaces the gateway's first-chunk timeout).

## 7. Encoders

- `sse.RawEncoder`: `id:` = entry ID, `event:` = type, `data:` = payload.
- `envelope.Encoder` (current browser wire): like raw, but `stream.token`
  envelopes are unwrapped to plain text.
- `aisdk.Encoder`: header `x-vercel-ai-ui-message-stream: v1`; `stream.started`
  → `start`, `stream.token` → `text-start`/`text-delta`, `stream.done` →
  `text-end` + `finish`, `stream.error` → `error`, any other type →
  `data-<type>` part; ends with `data: [DONE]`. IDs are kept for resume.

## 8. Relay: `relay.Relay`

```go
type Publisher interface { Publish(ctx context.Context, m relay.Message) error }
type Resumer interface { ResumePublish(orderingKey string) }   // optional (Pub/Sub ordering)
r, _ := relay.New(rdb, relay.Config{Stream: "outbox:orch", Buckets: 64, Group: "relay-orch", Consumer: pod}, pub)
err := r.Run(ctx)
```

- One loop per slot-compatible partition of outbox shards (Cluster: one per
  shard; standalone: one loop over all shards with multi-key `XREADGROUP`).
- At-least-once: `XACK` only after a successful publish or a dead-letter
  (DLQ `XADD` + `XACK` in one Lua script). Consumers dedupe on
  `(stream, event_id)`.
- Per ordering key (default: the source stream ID) order is preserved within a
  replica: after a failure the key is held (its later entries stay pending), the
  publisher's `ResumePublish` is called, and the pending list is retried in ID
  order after a jittered backoff. Other keys continue.
- Pending entries are re-read (`XREADGROUP … 0`) every loop, which also keeps
  their idle time low; `XAUTOCLAIM` takes over entries idle ≥ `MinIdle` from dead
  consumers. Poison errors (`Classify`) dead-letter after `PoisonAttempts`;
  entries without `data` and entries older than `MaxAge` whose key is failing are
  dead-lettered at once. `Replay` moves DLQ entries back.
- Retention: Redis must run with `maxmemory-policy noeviction`; outbox shards
  are trimmed by `MINID` (time) with `MaxLen` as a cap.

## 9. Telemetry (OTel API, meter `github.com/dungnh3/kitchen/sse`)

`sse.connections` (up-down), `sse.admission.rejected`, `sse.events.sent`,
`sse.write.errors`, `sse.hub.streams` (up-down), `sse.hub.readers` (up-down),
`sse.hub.catchup.reads`, `sse.hub.read.errors`, `sse.producer.appends`,
`sse.relay.published`, `sse.relay.dead_lettered`, `sse.relay.publish.errors`.
No per-token spans (too hot); `*slog.Logger` for errors.

## 10. Testing

- Unit tests with miniredis, `-race`, `goleak.VerifyTestMain` in every package,
  `testing/synctest` for heartbeat/idle/backoff timing, table tests.
- Benchmarks: `AppendEvent`, encoder, hub fan-out/delivery, `Serve` write path.
- `redisbus/loadtest_test.go` (build tag `loadtest`, env `SSE_LOADTEST_REDIS`):
  N streams × M `httptest` viewers against a real Redis, reports p50/p99 latency
  and Redis client count. Running 30K on real machines is the team's job.

## 11. Decisions

| # | Question | Decision |
|---|---|---|
| 1 | Viewers/stream, streams/pod | ~1 viewer/stream, 3–10K streams/pod: design for 1:1 |
| 2 | Redis topology | Accept `redis.UniversalClient` (standalone, Sentinel, Cluster) |
| 3 | Eviction/retention | `noeviction` + `MAXLEN ~` + `ActiveTTL`/`DoneTTL` |
| 4 | Reader | Multiplexed per-pod readers + per-stream ring |
| 5 | Key layout | Bucketed hash tag `sse:{b<k>}:<id>` |
| 6 | Late joiner | Full replay, consistent for every viewer |
| 7 | Slow client | Cursor-based pull, never drop; disconnect only on write timeout |
| 8 | Aggregation | Removed from the library |
| 9 | Pub/Sub outbox | Keep relay in `sse/relay`, target a local `Publisher` interface |
| 10 | Browser protocol | Envelope wire by default + pluggable `Encoder` + `aisdk` encoder |
| 11 | HITL `:interact` | Removed |
| 12 | Load test | Benchmarks + Go soak test (build tag) |
| 13 | Mirror on Cluster | Outbox sharded by the same buckets |
| 14 | Gateway pipeline | One `Transform` chain + `Visible` func |
| 15 | Coalescing / admission | Write coalescing (no token merging) + admission kept |
| 16 | Event catalog | Only `stream.*` lifecycle types in the library |

## 12. Decisions taken by default (to review)

The owner said: accept every recommended option, and for new questions pick
the option that is best for the system and for security. These are those picks.

| # | Decision | Alternatives | Pros / cons of the pick |
|---|---|---|---|
| 17 | `Buckets` default 64 | 256, 1024 | + ≤64 blocking conns/pod and ≤64 relay loops on Cluster; − one slot carries 1/64 of traffic |
| 18 | One stateless `Producer` that returns errors | keep stateful `Manager.Publisher` too | + one API, retry/replay safe; − no in-process idempotent `Done` helper (End is idempotent in Redis) |
| 19 | `Append` via Lua, `ErrStreamEnded` after End | document only; "soft" End | + late writes are reported, not silently invisible; − Lua instead of MULTI (same cost) |
| 20 | Public `Hub.Subscribe` iterator shared by HTTP and gRPC | HTTP only | + gRPC `SubscribeRun` stops opening a XREAD per call; removes the channel pair |
| 21 | Config style: plain `Config` structs with documented zero-value defaults | functional options | + loads straight from YAML like today's configs; − no compile-time required fields (validated in `New*`) |
| 22 | Default `Visible`: only library `stream.*` types; unknown types hidden | show everything | + safe default, same as old `DefaultEventVisibility`; − services must declare their types |
| 23 | Shutdown sends no terminal event, only closes | old `TerminalEvent: stream.cancelled` | + browsers reconnect and resume on another pod during a rolling deploy; − clients that waited for `cancelled` see a reconnect instead |
| 24 | `Last-Event-ID` must be `<ms>-<seq>` (or `0`), ≤ 41 bytes, else 400 | pass through | + closes the injection path into XREAD (review §11); − malformed clients get 400 |
| 25 | Telemetry via OTel metrics, drop `metrics/` (Prometheus) and `promhooks` | keep Prometheus hooks | + D5; services export via the OTel Prometheus exporter; − metric names change, dashboards must be updated |
| 26 | Built-in ULID generator (crypto/rand) instead of `oklog/ulid` | keep dependency | + one less dep; − own code to maintain (30 lines, tested) |
| 27 | Wake key per reader to interrupt `XREAD BLOCK` | short block only; `CLIENT UNBLOCK` | + new streams get live data immediately, Cluster-safe; − one tiny write per registration |
| 28 | Entry fields stay `type`/`data` (+`stream` on outbox) | rename | + readable in redis-cli, outbox entries look like before; − none |
| 29 | `Serve` writes `stream.done` once, skipping it if the writer already sent one | always write | + fixes the duplicate `stream.done` seen with nexus; − encoders keep a little state |
| 30 | Relay has no lag sampling hook in v0 | port `OnLag` | + smaller relay; − lag must be read from Redis (`XINFO GROUPS`) by ops for now |

## 13. Removed (unused in production, see survey)

HITL interaction stream, consumer/publisher aggregation, `ResumeStore`,
`MaxEventAge`, `PolicyDrop`/`PolicyBlock`/`PolicyDisconnect`, token content
merging (`BufferConfig`), `NewManager` vs `NewManagerWithOptions`, the
agent-platform event catalog, `metrics/` and `promhooks`, `pubsubgcp`.
