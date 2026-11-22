# sse

`sse` streams ordered event logs (LLM tokens, run lifecycle events) to
browsers over Server-Sent Events. Writers append to Redis Streams; any number
of gateway pods serve the same stream, resume from `Last-Event-ID`, and never
skip a slow viewer ahead. Use it for one-way, high-fan-out streams. Use
`pubsub` for service-to-service messaging, and gRPC streams when both sides
are your services.

Redis access goes through `redisx`, logging through `log`, and errors through
`errs`, as [D7](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22)
requires. Design: [docs/design/sse.md](../docs/design/sse.md).
API reference: [pkg.go.dev](https://pkg.go.dev/github.com/dungnh3/kitchen/sse).

## Install

`sse` is its own module (with `redisbus`, `envelope`, `aisdk` and `relay`),
so the core module does not pull in its dependencies:

```
go get github.com/dungnh3/kitchen/sse@latest
```

## Quick start

A writer appends with a `redisbus.Producer`. A gateway serves with a
`redisbus.Hub` (the `sse.Source`) and an `sse.Server`. From `ExampleServer`:

```go
rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
if err != nil {
	fmt.Println(err)
	return
}
defer func() { _ = rdb.Close() }()

// Writer side: append, then End.
prod, _ := redisbus.NewProducer(rdb, redisbus.ProducerConfig{})
_, _ = prod.Append(ctx, "run_1", sse.Message{Type: sse.TypeToken, Data: []byte("Hello")})
_, _ = prod.End(ctx, "run_1")

// Reader side: one Hub and one Server per process.
hub, _ := redisbus.NewHub(rdb, redisbus.HubConfig{Logger: logger})
defer hub.Close()
srv, _ := sse.NewServer(hub, sse.ServerConfig{
	MaxConnections:          10_000,
	MaxConnectionsPerStream: 20,
	Logger:                  logger,
})
defer func() { _ = srv.Shutdown(ctx) }()

mux := http.NewServeMux()
mux.HandleFunc("GET /runs/{id}/stream", func(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ticket, err := srv.Admit(r.Context(), id)
	if err != nil {
		errs.WriteHTTP(w, r, err) // 503 or 429, before anything is written
		return
	}
	err = srv.Serve(w, r, id, sse.ServeOptions{Ticket: ticket})
	switch {
	case err == nil, errors.Is(err, sse.ErrServerClosed), errors.Is(err, sse.ErrReplayGap):
	case errors.Is(err, sse.ErrInvalidCursor), errors.Is(err, sse.ErrStreamingUnsupported):
		errs.WriteHTTP(w, r, err) // the response is not committed yet
	default:
		logger.WarnContext(r.Context(), "stream ended", log.Err(err))
	}
})
```

The browser receives `event: stream.token` / `data: Hello`, then
`event: stream.done`. The safe defaults:

- **Every retained event, in order.** A viewer without `Last-Event-ID` gets
  the whole retained stream, then live events. Nothing is dropped.
- **Unknown types are hidden.** Only the library's `stream.*` types are
  visible until you list your own in `ServeOptions.Visible`.
- **Bounded memory.** Each connection has a 64-slot hand-off. A slow viewer
  reads older entries from Redis at its own pace.
- **Write deadline.** A write that takes over 5s ends the connection; the
  browser reconnects and resumes.
- **Bounded streams.** Each stream keeps about 10,000 entries and expires 24h
  after its last append, or 2m after `End`.
- **Admission before headers.** `Admit` fails while the response can still
  carry an HTTP error.

`Admit` → ticket → `Serve` is the pattern to use. `Serve` without a ticket
admits by itself, but then admission errors arrive with the other `Serve`
errors.

## Configuration

`sse.ServerConfig` (one per process):

| Field | Default | When to change |
|---|---|---|
| `MaxConnections` | 0 (no cap) | Always set it in production: memory and file descriptors per pod. |
| `MaxConnectionsPerStream` | 0 (no cap) | Set it to stop one run from taking the pod (tab storms, scrapers). |
| `Heartbeat` | 15s | Lower it when a proxy closes idle connections sooner. |
| `WriteTimeout` | 5s | Raise it for very slow mobile clients. |
| `RetryHint` | 3s | The `retry:` sent to EventSource for its reconnect delay. |
| `Logger` | `log.Default()` | Pass the service's `*log.Logger`. |
| `MeterProvider` | global | Tests, or a non-global provider. |

`sse.ServeOptions` (one per request):

| Field | Default | Meaning |
|---|---|---|
| `Ticket` | nil (Serve admits) | The slot from `Admit`. |
| `LastEventID` | the `Last-Event-ID` header | A cursor from elsewhere, such as a query parameter. |
| `Encoder` | `sse.NewRawEncoder` | `envelope.NewEncoder`, `aisdk.NewEncoder`, or your own. |
| `Transforms` | none | Per-connection checks and rewrites. |
| `Visible` | `sse.DefaultVisible` | `sse.VisibleTypes("run.", "step.", ...)`. |
| `MaxIdle` | 0 (never) | Ends a silent stream with `stream.error {"type":"idle_timeout"}`. |
| `FlushInterval` | 0 (flush when idle) | Batches small token writes. |

`redisbus.ProducerConfig`: `Keyspace` (`Prefix` "sse:", `Buckets` 64),
`MaxLen` 10000, `ActiveTTL` 24h, `DoneTTL` 2m, `MaxPayloadBytes` 1 MiB,
`Outbox`, `OutboxClient`. `redisbus.HubConfig`: `Keyspace`, `Block` 2s,
`Count` 256, `MaxKeysPerRead` 512, `RingSize` 512, `CatchupBatch` 256,
`RetryBackoff` 100ms to `MaxRetryBackoff` 5s, `Logger`. All config structs
have `json` and `yaml` tags.

**Writers, readers and relays must use the same `Keyspace`.** Changing
`Buckets` changes every key, so in-flight streams are lost.

## Integration with kitchen

- **Logger.** `ServerConfig`, `redisbus.HubConfig`, `relay.Config` and
  `envelope.ValidatorConfig` take a `*log.Logger`; nil means `log.Default()`.
  Call `log.Setup` once in `main` (D5).
- **Redis.** Open the client with `redisx.Open`; it is traced, measured and
  pinged. Standalone, Sentinel and Cluster all work, because keys carry a
  bucket hash tag (`sse:{b17}:run_1`).
- **errs.** Every sentinel is an `errs` error. Write `Admit` errors, and
  `Serve` errors returned before the response is committed, with
  `errs.WriteHTTP`. Return them from a gRPC handler unchanged.
- **otelx.** Metrics use the global `MeterProvider`, which `otelx/sdk`
  installs in `main`. `sse` emits no per-token spans; Redis spans come from
  `redisx`.
- **server.** Register the handler in your service's `RegisterHTTP`; see
  [server/USAGE.md](../server/USAGE.md) for the service wiring and
  `sse/kitchen_test.go` for a working mount. When the kitchen server starts
  draining (`server.Draining`), `Admit` returns `ErrServerClosed` and open
  streams end with no terminal event. Browsers then resume on another pod
  instead of being cut at the shutdown timeout.
- **context.** A `TransformFactory` gets the request context, so it can read
  the caller's identity (`xcontext.UserID`, JWT claims) once per connection.

A gateway runs `go hub.Run(ctx)`, calls `srv.Shutdown(ctx)` before exit, and
passes the envelope encoder, its visible types and the context validator to
`Serve`. From `Example_gateway` in `sse/redisbus`:

```go
err = srv.Serve(w, r, runID, sse.ServeOptions{
	Ticket:     ticket,
	Encoder:    envelope.NewEncoder,
	Visible:    visible,
	Transforms: []sse.TransformFactory{validator},
})
```

## Advanced

### Resume and the replay gap

EventSource sends the last `id:` it saw as `Last-Event-ID` when it
reconnects. `Serve` then streams only what follows. From
`ExampleServer_resume`:

```go
// Serve reads the Last-Event-ID header; ServeOptions.LastEventID
// overrides it (for a cursor carried in a query parameter).
err := srv.Serve(w, r, "run_1", sse.ServeOptions{LastEventID: r.URL.Query().Get("cursor")})
if errors.Is(err, sse.ErrInvalidCursor) {
	errs.WriteHTTP(w, r, err) // 400 INVALID_LAST_EVENT_ID
}
```

- A malformed cursor, or one newer than the stream's last entry, returns
  `ErrInvalidCursor` before the headers (`Source.CheckCursor`): answer 400.
  A Redis error from that check is also returned before the headers.
- When the cursor was trimmed (`MaxLen`) or the stream expired, the client
  gets `stream.replay_gap` with `{"reason":"cursor_trimmed"}`, and `Serve`
  returns `ErrReplayGap`. The client must reload the state (for example from
  the database) instead of resuming. The library never resumes silently from
  a later point.
- When a resumed catch-up reaches the live tail, a synthetic `stream.resumed`
  event (no ID) is sent once.

### Transforms and tenant isolation

A `TransformFactory` builds one `Transform` per connection. It returns
`ActionPass`, `ActionSkip` (drop for this connection) or `ActionStop` (write
the message, then end). An error ends the connection. From
`ExampleTransformFactory`:

```go
var moderate sse.TransformFactory = func(context.Context) sse.Transform {
	return func(_ context.Context, m *sse.Message) (sse.Action, error) {
		switch m.Type {
		case "safety.blocked":
			return sse.ActionStop, nil // written, then the connection ends
		case "internal.audit":
			return sse.ActionSkip, nil // this connection only
		}
		return sse.ActionPass, nil
	}
}
```

Always install `envelope.ContextValidator` at a gateway. It skips envelopes
whose app, tenant, user or session is not in the connection's `Rule`, and
envelopes whose `event_type` differs from the stream entry type. So events
cannot cross users even when a writer targets the wrong stream. From
`ExampleContextValidator` in `sse/envelope`:

```go
validator := envelope.ContextValidator(envelope.ValidatorConfig{
	Logger: log.Discard(), // pass the service logger; one Warn per rejection
	Resolve: func(ctx context.Context) envelope.Rule {
		return envelope.Rule{
			UserIDs:   []string{xcontext.UserID(ctx)},
			TenantIDs: []string{"acme"},
		}
	},
})
```

An empty list in `Rule` skips that check. `Rule.Raw` decides what happens to
payloads that are not envelopes. The default, `RawRejectTokens`, drops raw
`stream.token` messages and passes raw lifecycle messages. `RawPass`
turns the check off for raw payloads: use it only for streams whose writers
never carry user content.

### Outbox: mirror and split mode

Set `ProducerConfig.Outbox` to also write every append to an outbox shard,
which `relay` publishes to Pub/Sub. Without `OutboxClient` (mirror mode) both
writes are one atomic Lua call. With `OutboxClient` (split mode) the outbox
is on another Redis and is written after the stream entry. From
`ExampleProducer_outbox`:

```go
p, _ := redisbus.NewProducer(streams, redisbus.ProducerConfig{
	Outbox:       &redisbus.OutboxConfig{Stream: "outbox:orch"},
	OutboxClient: outbox, // omit for mirror mode
})
id, err := p.Append(ctx, "run_1", sse.Message{Type: "run.started", Data: []byte(`{}`)})
switch {
case errors.Is(err, redisbus.ErrOutboxDiverged):
	// Viewers get the event (id is valid); the projection misses it.
	log.Default().WarnContext(ctx, "outbox diverged", log.String("entry", id), log.Err(err))
case err != nil:
	// Nothing was written; retry or fail the run.
}
```

`Append` after `End` returns `redisbus.ErrStreamEnded`, so late events are
not lost silently. `End` is idempotent.

## Backends and adapters

### sse/redisbus

The `Producer` keeps no per-stream state, so any process may write any
stream. The `Hub` serves up to `MaxKeysPerRead` streams per `XREAD BLOCK`
connection and keeps a ring of `RingSize` recent entries per stream. Size the
`redisx` pool for readers plus catch-up reads: hundreds, not thousands. Any
consumer can range over a stream with `hub.Subscribe(ctx, id, cursor)` (see
`ExampleHub_Subscribe`).

### sse/envelope

The JSON envelope (`SchemaVersion` "v1") that agent-platform writers put in
`Message.Data`. Build it with `envelope.NewBuilder(session, app, user)` and
turn it into a message with `Envelope.Message()`. `envelope.NewEncoder` keeps
today's browser wire and unwraps `stream.token` envelopes to plain text.

### sse/aisdk

`aisdk.NewEncoder` speaks the Vercel AI SDK UI message stream protocol, so
the browser can use `useChat`. It sets `x-vercel-ai-ui-message-stream: v1`,
keeps entry IDs for resume, and ends with `data: [DONE]`. Map your own event
types to AI SDK parts with `Options.Map`; unmapped types become
`data-<type>` parts. From `ExampleNewEncoderWith`:

```go
factory := aisdk.NewEncoderWith(aisdk.Options{Map: func(t string, data json.RawMessage) ([]aisdk.Part, bool) {
	if t != "agent.tool_call" {
		return nil, false // default: data-<type>
	}
	var c struct {
		ID, Name string
		Args     json.RawMessage
	}
	_ = json.Unmarshal(data, &c)
	return []aisdk.Part{{"type": "tool-input-available", "toolCallId": c.ID, "toolName": c.Name, "input": c.Args}}, true
}})
opts := sse.ServeOptions{Encoder: factory}
```

### sse/relay

`relay` drains the outbox shards with Redis consumer groups and publishes
each entry to `Config.Topic` on any kitchen `pubsub.Publisher`. From
`ExampleNew`:

```go
var pub pubsub.Publisher = memory.New() // pubsub/gcp with ordering in production
hostname, _ := os.Hostname()
r, err := relay.New(rdb, relay.Config{
	Stream:   "outbox:orch", // = redisbus.OutboxConfig.Stream
	Topic:    "agent-events",
	Group:    "relay",
	Consumer: hostname, // unique per replica
	Route:    relay.EnvelopeRoute("orchestrator"),
	Classify: func(err error) relay.Class {
		if errors.Is(err, pubsub.ErrPermanent) {
			return relay.Poison // dead-lettered after PoisonAttempts
		}
		return relay.Transient // retried with backoff
	},
	Logger: logger,
}, pub)
```

- **At least once.** An entry is acknowledged only after it is published or
  dead-lettered. Consumers dedupe, for example on `(run_id, event_id)`.
- **Ordered per key.** After a failure the key is held. If the publisher
  implements `pubsub.KeyResumer`, `ResumeKey` is called, and the key retries
  after a `timex.Backoff` delay (500ms to 30s). Other keys keep flowing.
- **Dead letters.** Poison entries go to `<shard>:dlq` after
  `PoisonAttempts` (5), unroutable entries at once, and failing entries older
  than `MaxAge` (5h) with reason `expired`. `Relay.Replay(ctx, limit)` moves
  them back.
- `Buckets` must equal the producers' `Keyspace.Buckets`. The classifier
  above is `relay.DefaultClassify`; replace it only to add more poison cases.

## Testing

Use miniredis with `redisx.Open`, as the examples do. Production code paths
run unchanged. For a server test, mount the handler on `httptest.NewServer`
and read the body; `ExampleServer` shows how. Use `log.Discard()` as the
logger. Assert on errors with `errors.Is` against the sentinels, and on HTTP
status by writing them with `errs.WriteHTTP` into an `httptest.ResponseRecorder`
(see `TestAdmissionErrorsAreErrs`). Check goroutine leaks with
`goleak.VerifyTestMain`; `Hub.Close` and `Server.Shutdown` stop everything.

## Observability

Metrics (scope `github.com/dungnh3/kitchen/sse`): `sse.connections`,
`sse.admission.rejected`, `sse.events.sent`, `sse.write.errors`,
`sse.hub.streams`, `sse.hub.readers`, `sse.hub.catchup.reads`,
`sse.hub.read.errors`, `sse.producer.appends{result}`,
`sse.envelope.rejected{reason}`, `sse.relay.published`,
`sse.relay.dead_lettered{reason}`, `sse.relay.publish.errors`.

Logs, all at Warn: Hub read failures, unencodable events the Server drops,
envelope rejections (IDs only, never the payload), and relay read, publish,
ack and dead-letter failures.

| Error | Code / HTTP | Reason |
|---|---|---|
| `sse.ErrTooManyConnections` | Unavailable / 503, Retry-After 3s | `SSE_SERVER_FULL` |
| `sse.ErrTooManyForStream` | ResourceExhausted / 429 | `CONCURRENCY_LIMITED` |
| `sse.ErrServerClosed` | Unavailable / 503 | `SERVICE_DRAINING` |
| `sse.ErrInvalidCursor` | InvalidArgument / 400 | `INVALID_LAST_EVENT_ID` |
| `sse.ErrReplayGap` | OutOfRange | `SSE_REPLAY_GAP` |
| `sse.ErrIdle` | DeadlineExceeded | `SSE_IDLE_TIMEOUT` |
| `sse.ErrStreamingUnsupported` | Internal | `SSE_STREAMING_UNSUPPORTED` |
| `sse.ErrInvalidEvent` | InvalidArgument | `SSE_INVALID_EVENT` |
| `redisbus.ErrStreamEnded` | FailedPrecondition | `SSE_STREAM_ENDED` |
| `redisbus.ErrInvalidMessage` | InvalidArgument | `SSE_INVALID_MESSAGE` |
| `redisbus.ErrOutboxDiverged` | Internal | `SSE_OUTBOX_DIVERGED` |
| `envelope.ErrMissingField` | InvalidArgument | `SSE_ENVELOPE_MISSING_FIELD` |
| `envelope.ErrUnsupportedSchema` | InvalidArgument | `SSE_ENVELOPE_UNSUPPORTED_SCHEMA` |

`redisbus.ErrInvalidStreamID`, `redisbus.ErrHubClosed` and
`relay.ErrUnroutable` are plain errors.

## Production checklist

- Run Redis with `maxmemory-policy noeviction`. Streams are bounded by
  `MaxLen` and TTLs, never by eviction.
- Use the same `Keyspace` (prefix and buckets) in writers, gateways and
  relays.
- Set `MaxConnections` and `MaxConnectionsPerStream`.
- Install `envelope.ContextValidator` and an explicit `Visible` list.
- Call `End` after the writer's terminal event, and handle `ErrStreamEnded`.
- Mount the server on kitchen `server`, so drains move viewers to other pods.
- Disable proxy buffering. `Serve` sets `X-Accel-Buffering: no`; other
  proxies need their own setting.
- Keep the relay's `MaxAge` below the outbox `MinIDRetention` (6h).

## Migrating from the old kitchen

The key layout, the end sentinel and the API changed; migrate a stream family
at a time. See [docs/migration/sse.md](../docs/migration/sse.md).
