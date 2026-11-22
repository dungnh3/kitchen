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
