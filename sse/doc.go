// Package sse streams ordered event logs (LLM tokens and lifecycle events) to
// browsers over Server-Sent Events.
//
// The module has four parts:
//
//   - sse (this package): the wire format (Event, AppendEvent), the Source
//     abstraction, per-connection Encoders, Transforms, and Server, which
//     admits, streams, heartbeats and drains HTTP connections.
//   - sse/redisbus: Redis Streams storage. Producer appends (one atomic Lua
//     call, optional outbox leg); Hub is the per-process multiplexed reader
//     that implements Source.
//   - sse/envelope and sse/aisdk: the agent-platform JSON envelope and two
//     browser encoders (today's envelope wire, Vercel AI SDK UI stream).
//   - sse/relay: drains the outbox shards into a Pub/Sub-style Publisher.
//
// Delivery guarantees: a viewer receives every retained event of its stream in
// order, from the start or after its Last-Event-ID, and then live events. A
// slow viewer is never sent a gap: it reads older events from Redis at its own
// pace. If its resume position was trimmed it receives "stream.replay_gap"
// and must reload state. Writes that time out end the connection; the browser
// reconnects and resumes.
//
// See docs/design/sse.md in the repository for the design and its decisions.
package sse
