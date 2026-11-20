# Migration: `github.com/vulcanlabsvn/kitchen/sse` → `github.com/dungnh3/kitchen/sse`

This is a clean break (decision D3). Design: `docs/design/sse.md`.

```
go get github.com/dungnh3/kitchen/sse@latest     # tag sse/vX.Y.Z
```

## 1. Roll-out order (wire and keyspace changed)

The Redis key layout changed (`sse:<id>` → `sse:{b<k>}:<id>`), and so did the
end-of-stream sentinel (`__sse_done__` → `__sse_end__`). Old and new writers
and readers cannot share a stream. Migrate per stream family, all at once:

1. Deploy the gateway (reader) with both the old and the new stack, routing by
   a feature flag per run. New runs use the new stack; in-flight runs finish on
   the old one (they live at most `DoneTTL`/`ActiveTTL`).
2. Switch the writers (orchestrator, execution-engine, smith-nexus, seleya,
   creative-studio v2, writing-suite) to `redisbus.Producer` with the **same**
   `Keyspace{Prefix, Buckets}` as the gateway.
3. Switch the outbox relays at the same time as their writers. Drain the old
   outbox stream first: old entries are not read by the new relay.
4. Remove the old stack after `ActiveTTL` (24h by default).

## 2. Package and symbol map

| Old | New |
|---|---|
| `sse.Event`, `Event.WriteTo` | `sse.Event`, `sse.AppendEvent` / `Event.WriteTo` |
| `sse.Upgrade`, `sse.Connection`, `ConnectionConfig` | internal to `sse.Server.Serve`; settings in `sse.ServerConfig` (`Heartbeat`, `WriteTimeout`, `RetryHint`) |
| `sse.Config`, `sse.NewEventBus(cfg)` | `redisbus.NewProducer(rdb, ProducerConfig)` + `redisbus.NewHub(rdb, HubConfig)`; you create the `redis.UniversalClient` |
| `eventbus.EventBus` | gone. Write: `redisbus.Producer`; read: `sse.Source` (`redisbus.Hub`) |
| `eventbus.Message` | `sse.Message{ID, Type, Data}` (`PublishedAt` removed) |
| `EventBus.Publish` / `PublishWithMirror` | `Producer.Append` (mirror = `ProducerConfig.Outbox`) |
| `EventBus.Done` + `Expire` | `Producer.End` (sets `DoneTTL`, idempotent) |
| `EventBus.Subscribe(ctx, id, opts)` (channel pair) | `Hub.Subscribe(ctx, id, cursor)` → `iter.Seq2[sse.Message, error]` |
| `PublishInteraction`, `SubscribeInteraction`, `Publisher.WaitForInput` | removed (HITL goes through the workflow engine / gRPC `RespondHitl`) |
| `stream.NewManager`, `NewManagerWithOptions` | `sse.NewServer(source, ServerConfig)` (gateway) / `redisbus.NewProducer` (writers) |
| `Manager.Admit(id)` → `*AdmissionTicket` | `Server.Admit(ctx, id)` → `*sse.Ticket` |
| `Manager.Pipe(ctx, conn, id, PipeOptions{LastEventID, Ticket})` | `Server.Serve(w, r, id, ServeOptions{Ticket, LastEventID, Encoder, Transforms, Visible, MaxIdle})` |
| `Manager.Shutdown`, `ShutdownWithOptions` | `Server.Shutdown(ctx)` |
| `Manager.Publisher(ctx, id)` + `Send` / `SendTyped` / `SendEvent` / `SendEnveloped` | `Producer.Append(ctx, id, msg)`; build `msg` with `envelope.Builder...Message()` |
| `Publisher.Done` / `Error` / `ErrorWithMessage` | append your terminal event (`stream.done` / `stream.error`), then `Producer.End` |
| `eventbus/producer` (`ModeDirect`/`ModeMirror`/`ModeSplit`, Hooks) | `redisbus.Producer`: no Outbox = direct; `Outbox` = mirror; `Outbox` + `OutboxClient` = split |
| `eventbus/outbox.Writer` | `ProducerConfig.Outbox` + `OutboxClient` |
| `eventbus/outbox.New(...)` Relay | `relay.New(rdb, relay.Config{Stream, Buckets, Group, Consumer}, pub)` |
| `outbox.Publisher` (`Publish` → `Result`, `Resume`) | `relay.Publisher` (`Publish(ctx, Message) error`) + optional `relay.Resumer` (`ResumePublish`) |
| `outbox.EnvelopeRoute(source)` | `relay.EnvelopeRoute(source)` (same attributes and ordering key) |
| `outbox.Replay` | `Relay.Replay(ctx, limit)` |
| `outbox/pubsubgcp` | removed: adapt the messaging area's `pubsub/gcp` (Pub/Sub **v2**) publisher to `relay.Publisher` |
| `outbox/promhooks`, `metrics.Hooks`, `sse.Hooks` | OTel metrics (see §5) |
| `envelope.Envelope`, `NewBuilder`, `BuildText`, `BuildJSON`, `Build` | same names in `sse/envelope`; plus `Envelope.Message()` and `envelope.Parse` |
| `envelopefilter.NewContextValidatorFactory(resolve)` | `envelope.ContextValidator(envelope.ValidatorConfig{Resolve})` |
| `ValidationRule{Allowed*IDs, StrictUnmarshal, StrictTokenOnly, OnMismatch}` | `envelope.Rule{AppIDs, TenantIDs, UserIDs, SessionIDs, Raw}`; logging and metric are always on |
| `stream.Filter`, `Enricher`, `WithFilters`, `WithFilterFactories`, `WithEnrichers`, `WithEnvelopeFilter` | `sse.TransformFactory` / `sse.Transform` (`ActionPass`/`Skip`/`Stop`) |
| `WithEventVisibility`, `sse.DefaultEventVisibility` | `ServeOptions.Visible`, `sse.VisibleTypes(...)`, `sse.DefaultVisible` |
| `WithBufferConfig` (token merging) | removed; writes are coalesced automatically, `ServeOptions.FlushInterval` optional |
| `WithAggregator`, `OnAggregated`, `WithPublisherAggregation`, `ResumeStore` | removed (aggregate and persist in the writer or in ms-agent-conversation) |
| `StreamConfig.SlowClientPolicy` (Drop/Block/Disconnect), `BufferSize` | removed: slow clients are paced by cursor, never dropped |
| `StreamConfig.MaxEventAge`, `InteractTimeout` | removed |
| first-chunk timeout (gateway) | `ServeOptions.MaxIdle` |
| `sse.Chain` middleware | use `net/http` middleware from the transport area |
| event catalog (`run.*`, `step.*`, `plan.*`, `approval.*`, `hitl.*`, `tool_call.*`, `quota.*`, `safety.*`, `limit.*`) | removed from kitchen (D5): copy the constants you use into the agent-platform repo. `stream.*` stays as `sse.Type*` |

### Mechanical renames

```
gofmt -r 'sse.EventStreamToken -> sse.TypeToken' -w .
gofmt -r 'sse.EventStreamStarted -> sse.TypeStarted' -w .
gofmt -r 'sse.EventStreamDone -> sse.TypeDone' -w .
gofmt -r 'sse.EventStreamError -> sse.TypeError' -w .
gofmt -r 'sse.EventStreamCancelled -> sse.TypeCancelled' -w .
gofmt -r 'sse.EventStreamResumed -> sse.TypeResumed' -w .
gofmt -r 'sse.EventStreamReplayGap -> sse.TypeReplayGap' -w .
sed -i 's#github.com/vulcanlabsvn/kitchen/sse/envelope#github.com/dungnh3/kitchen/sse/envelope#' $(grep -rl vulcanlabsvn/kitchen/sse/envelope --include=*.go .)
```

The rest (Manager → Server/Producer) is a small manual change per service.

### Writer, before and after

```go
// before
pub := mgr.Publisher(ctx, runID)
defer pub.Done(ctx)
env, _ := envelope.NewBuilder(runID, app, user).BuildText(sse.EventStreamToken, tok)
_ = pub.SendEnveloped(ctx, env)

// after
env, _ := envelope.NewBuilder(runID, app, user).BuildText(sse.TypeToken, tok)
msg, _ := env.Message()
if _, err := prod.Append(ctx, runID, msg); err != nil { ... }   // errors are returned
...
_, _ = prod.End(ctx, runID)
```

## 3. Behaviour changes and fixed defects

Each item below was a defect or a risky behaviour in the old module
(review `docs/review/04-sse.md`); none is carried over.

1. **Reader scalability.** Old: one blocking `XREAD` (one pool connection) per
   connection when resuming, and for the whole life of every reconnect
   (review §2.1–2.3); `PoolSize ≥ 100` guard. New: per-process multiplexed
   readers, at most `MaxKeysPerRead` (512) streams per connection; resumes
   catch up with `XRANGE` and then share the live reader. Size the pool for
   readers + catch-up reads (hundreds, not thousands). The `PoolSize` guard
   is gone.
2. **Redis Cluster/Sentinel.** Old: single-node client only, and the mirror Lua
   failed with `CROSSSLOT` on Cluster (§2.4). New: any
   `redis.UniversalClient`; keys carry a bucket hash tag; mirror outbox shards
   share the stream's slot.
3. **Global mutex.** Old: one `Manager.mu` for every Pipe/Admit (§2.5). New:
   sharded registries and atomic admission counters.
4. **Late joiners.** Old: the first viewer of a pod replayed from `"0"` and could
   be disconnected by that burst; a second viewer silently got only the last
   128 entries; the doc said `""` means `">"` but the code used `"0"` (§2.6).
   New: every viewer without `Last-Event-ID` gets the full retained stream,
   paced by the client, never a burst into a buffer.
5. **Silent data loss.** Old: `PolicyBlock` was silently downgraded to drop at
   the hub, `PolicyDrop` dropped tokens mid-text, `MaxEventAge` skipped entries
   without telling the client (§2.7). New: nothing is dropped; a viewer behind
   the in-memory ring reads from Redis; only a trimmed resume cursor ends the
   stream, with `stream.replay_gap`. A trim **during** catch-up is detected too.
6. **Consumer aggregation** fired per tab and on partial messages, so
   persistence could duplicate or truncate (§2.8). Removed.
7. **Shutdown.** Old: counters went negative, two near-duplicate methods, the
   drain timeout error was swallowed (§2.9). New: one `Shutdown(ctx)` that
   returns `ctx.Err()` (wrapped) on timeout. On shutdown no terminal event is
   sent (old gateway: `stream.cancelled`), so browsers reconnect to another pod
   and resume instead of treating the run as cancelled.
8. **Heartbeat.** Old: a goroutine per connection, write errors ignored, pings
   during active streaming (§2.10). New: in the writer loop, only after
   `Heartbeat` (default 15s, was 30s in the gateway config) of idle time; a
   failed ping ends the connection.
9. **`Connection: keep-alive`** header removed (invalid on HTTP/2).
10. **`Last-Event-ID` validation.** Old: passed straight into `XREAD` (§2.11).
    New: must be `<ms>-<seq>` (or `0`), ≤ 41 bytes; otherwise `Serve` returns
    `sse.ErrInvalidCursor` before writing headers — answer **400**. A cursor
    newer than the stream is also rejected.
11. **Duplicate `Pipe` paths** (§2.12): one `Serve` code path.
12. **Writes after End.** Old: events appended after the `Done` sentinel (e.g.
    late execution-engine events) were invisible to readers. New:
    `Append` returns `redisbus.ErrStreamEnded`.
13. **Duplicate `stream.done`.** Old: writers sent `stream.done` and `Pipe` sent
    another. New: the encoder adds `stream.done` only if the stream did not
    carry one.
14. **Errors are returned.** Old `eventbus/producer` swallowed failures into hooks.
    New `Producer.Append` returns them (`ErrOutboxDiverged` in split mode still
    returns the entry ID). Ignore errors explicitly if a write is best effort.
15. **WaitGroup/goroutine leaks** are tested with goleak; tests use
    `testing/synctest` instead of sleeps for timers.
16. **Wire field validation.** `Event` fields with CR/LF/NUL return
    `sse.ErrInvalidEvent` instead of corrupting the stream; a bare CR in data
    starts a new `data:` line, as the spec's parser expects.
17. **Envelope validator.** New check: the envelope's `event_type` must equal
    the stream entry type (`type_mismatch`). Default `Raw` policy is
    `RawRejectTokens` (the old `StrictUnmarshal+StrictTokenOnly`); the old
    default (validator off) has no equivalent: always install it at the gateway.
18. **`envelope.Builder.WithEventID`** applies to the next build only (old: every
    later build reused the same ID). `Build` rejects invalid JSON data.
19. **Visibility default.** Unknown types are hidden unless listed in `Visible`
    (same as the old `DefaultEventVisibility`), but the old catalog is no longer
    built in: pass `sse.VisibleTypes("run.", "step.", ...)` with your types.
20. **Payload limit.** `Append` rejects payloads over `MaxPayloadBytes` (1 MiB).
21. **TTL refresh** is part of the append script (every append, same round trip)
    instead of a throttled separate call.
22. **Relay.** `Publisher.Publish` is synchronous per message (per-key
    pipelining removed); keys still publish in parallel up to `MaxInFlight`.
    Lag hooks (`OnLag`, `OnTrimmed`) are not ported: read `XINFO GROUPS` lag
    from ops tooling for now. DLQ entries carry `reason`, `error`, `source_id`.

## 4. Config keys

| Old YAML | New |
|---|---|
| `event_bus.redis.addr/password/db/pool_size` | build the `redis.UniversalClient` yourself |
| `prefix` | `keyspace.prefix` (default `sse:`), plus `keyspace.buckets` (default 64, must match everywhere) |
| `max_len` | `ProducerConfig.MaxLen` (10000) |
| `active_ttl`, `ttl_refresh_interval` | `ProducerConfig.ActiveTTL` (24h); no refresh interval |
| `done_ttl` | `ProducerConfig.DoneTTL` (2m) |
| `heartbeat_interval`, `write_timeout`, `retry_hint` | `ServerConfig.Heartbeat` (15s), `WriteTimeout` (5s), `RetryHint` (3s) |
| `max_connections`, `max_connections_per_stream` | `ServerConfig.MaxConnections`, `MaxConnectionsPerStream` |
| `buffer_size`, `slow_client_policy`, `first_connect_lossless`, `interact_timeout`, `max_event_age` | removed |
| `first_chunk_timeout` | `ServeOptions.MaxIdle` |
| outbox `outbox_stream`, `outbox_maxlen`, `min_id_retention` | `ProducerConfig.Outbox{Stream, MaxLen, MinIDRetention}` |
| relay `group`, `consumer`, `max_in_flight`, `relay_max_age` | `relay.Config{Group, Consumer, MaxInFlight, MaxAge}`, plus `Buckets` |

## 5. Metrics

Prometheus collectors are replaced by OTel instruments (scope
`github.com/dungnh3/kitchen/sse`); export them with the OTel Prometheus
exporter. Dashboards must be updated:

`sse.connections`, `sse.admission.rejected`, `sse.events.sent`,
`sse.write.errors`, `sse.hub.streams`, `sse.hub.readers`,
`sse.hub.catchup.reads`, `sse.hub.read.errors`, `sse.producer.appends{result}`,
`sse.envelope.rejected{reason}`, `sse.relay.published`,
`sse.relay.dead_lettered{reason}`, `sse.relay.publish.errors`.

## 6. Per-service checklist

- **ms-agent-gateway:** `Server` + `Hub`; `envelope.NewEncoder`;
  `envelope.ContextValidator` (Resolve from JWT + ticket); `Visible:
  sse.VisibleTypes("step.", "presentation.", "seleya.", "creative.", …)`;
  moderation second pass moves to the writer or ms-agent-conversation; gRPC
  `SubscribeRun` ranges over `Hub.Subscribe` instead of `bus.Subscribe`;
  map `ErrInvalidCursor` → 400, `ErrTooManyConnections` → 503,
  `ErrTooManyForStream` → 429.
- **ms-agent-orchestrator, ms-execution-engine:** `Producer` with `Outbox`
  (mirror) or `Outbox`+`OutboxClient` (split); `relay.New` with
  `relay.EnvelopeRoute(source)` and a Pub/Sub v2 publisher adapter. Handle
  `ErrStreamEnded` for late engine events.
- **ms-smith-nexus, ms-seleya, ms-creative-studio (v2):** `Producer` +
  `envelope.Builder`; `End` instead of `pub.Done`.
- **ms-writing-suite:** wrap its raw `presentation.*` payloads in envelopes,
  or keep them raw (passes the default `RawRejectTokens` validator because they
  are not tokens).
- **ms-creative-studio (v1, own SSE server):** `Server` + `Hub` with
  `Keyspace{Prefix: "creative:sse:"}`; its XREVRANGE cursor capture becomes the
  `Append` return value.
- **ms-agent-conversation** (Pub/Sub consumer): no change; attributes are the same.
