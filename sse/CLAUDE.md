# sse: package rules

Read `doc.go` and `docs/design/sse.md` before changing this package. It serves
the platform's high-fan-out streaming, so delivery guarantees and memory per
connection matter more than features.

## Delivery guarantees (do not weaken)

- A viewer receives every retained event of its stream, in order:
  - first from the start or from its `Last-Event-ID`;
  - then live events.
- A slow viewer is never skipped ahead. It reads the older events from Redis
  at its own pace.
- If the viewer's resume position was trimmed, it receives
  `stream.replay_gap`. Never resume silently from a later point.
- A write timeout ends the connection. The browser reconnects and resumes.
- Every way a stream ends after the headers either writes a terminal event
  (`stream.done`, `stream.replay_gap`, `stream.error`, `stream.cancelled`) or
  is meant to be resumed (shutdown, write error). Never end with an error
  that the client cannot act on: EventSource reconnects forever. Errors a
  client can fix (bad stream ID, bad cursor) are returned before the headers.
- Relay: per-key order holds across retries; a key with a backoff record
  publishes only from a full pass over the pending list.

## Wire contracts

Browsers and the agent platform depend on these:

- the SSE event framing;
- `envelope` with `SchemaVersion` "v1";
- the `aisdk` encoder, which follows the Vercel AI SDK UI stream protocol.

A breaking change needs a new version and a note in `docs/migration/sse.md`.

## Implementation rules

- Redis access goes through `redisx` clients. The `redisbus` producer appends
  with one atomic Lua call, including the optional outbox leg; keep it atomic.
- Per-connection state must stay bounded, with no unbounded buffers.
- Changes to the hub or reader need race tests and goleak checks.
- miniredis does not model everything: it ignores COUNT on pending-list
  reads and has no `XINFO STREAM` entries-added. Tests that depend on them
  use the Redis at `KITCHEN_TEST_REDIS_ADDR` (a local `redis-server` works).
- Run `TestResumeProperty` after any hub change; the loadtest
  (`-tags loadtest`, `SSE_LOADTEST_REDIS`) after reader changes.
