package sse

import (
	"context"
	"net/http"
)

// EndReason says why Serve is ending a connection.
type EndReason int

const (
	// EndDone means the stream ended cleanly (the writer called End).
	EndDone EndReason = iota
	// EndGap means the resume cursor is no longer retained.
	EndGap
	// EndIdle means no message arrived within ServeOptions.MaxIdle.
	EndIdle
	// EndShutdown means the server is shutting down; the client should reconnect.
	EndShutdown
	// EndStopped means a Transform returned ActionStop. The encoders write a
	// terminal event so the browser does not reconnect and resume past the
	// message that stopped the stream.
	EndStopped
)

func (r EndReason) String() string {
	switch r {
	case EndDone:
		return "done"
	case EndGap:
		return "replay_gap"
	case EndIdle:
		return "idle_timeout"
	case EndShutdown:
		return "shutdown"
	case EndStopped:
		return "stopped"
	}
	return "unknown"
}

// Encoder maps stream messages to wire events for one connection. Encoders
// may keep per-connection state; a new one is made per Serve call.
type Encoder interface {
	// Header adds protocol headers before the response is committed.
	Header(h http.Header)
	// Encode appends zero or more events for m to dst.
	Encode(dst []Event, m Message) []Event
	// End appends the terminal events for reason to dst.
	End(dst []Event, reason EndReason) []Event
}

// EncoderFactory makes a fresh Encoder per connection.
type EncoderFactory func() Encoder

// RawEncoder writes each message as id/event/data with no rewriting, and the
// library's lifecycle events at the end:
//
//   - EndDone: "stream.done" unless the stream already carried one;
//   - EndGap: "stream.replay_gap" with {"reason":"cursor_trimmed"};
//   - EndIdle: "stream.error" with {"type":"idle_timeout"};
//   - EndStopped: "stream.cancelled" with {"reason":"stopped"}, so the
//     browser closes instead of resuming after the stopping message;
//   - EndShutdown: nothing (the browser reconnects and resumes).
type RawEncoder struct{ sawDone bool }

// NewRawEncoder is an EncoderFactory for RawEncoder.
func NewRawEncoder() Encoder { return &RawEncoder{} }

// Header implements Encoder.
func (*RawEncoder) Header(http.Header) {}

// Encode implements Encoder.
func (e *RawEncoder) Encode(dst []Event, m Message) []Event {
	if m.Type == TypeDone {
		e.sawDone = true
	}
	return append(dst, Event{ID: m.ID, Type: m.Type, Data: nonNil(m.Data)})
}

// End implements Encoder.
func (e *RawEncoder) End(dst []Event, reason EndReason) []Event {
	return AppendEndEvents(dst, reason, e.sawDone)
}

// AppendEndEvents appends the RawEncoder terminal events; other encoders that
// keep the lifecycle wire (envelope.Encoder) reuse it.
func AppendEndEvents(dst []Event, reason EndReason, sawDone bool) []Event {
	switch reason {
	case EndDone:
		if !sawDone {
			dst = append(dst, Event{Type: TypeDone, Data: []byte{}})
		}
	case EndGap:
		dst = append(dst, Event{Type: TypeReplayGap, Data: []byte(`{"reason":"cursor_trimmed"}`)})
	case EndIdle:
		dst = append(dst, Event{Type: TypeError, Data: []byte(`{"type":"idle_timeout"}`)})
	case EndStopped:
		dst = append(dst, Event{Type: TypeCancelled, Data: []byte(`{"reason":"stopped"}`)})
	case EndShutdown:
	}
	return dst
}

func nonNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// Action is a Transform's verdict for one message.
type Action int

const (
	// ActionPass forwards the (possibly modified) message.
	ActionPass Action = iota
	// ActionSkip drops the message for this connection only.
	ActionSkip
	// ActionStop writes the (possibly rewritten) message, then ends the
	// connection with EndStopped (raw and envelope encoders write
	// "stream.cancelled"; aisdk writes finish). Use it for e.g.
	// "safety.blocked". Clients must close on that terminal event: a client
	// that reconnects with Last-Event-ID resumes after the stopping message.
	ActionStop
)

// Transform inspects or rewrites a message before it is encoded. It runs on
// the connection's writer goroutine and must not block for long. Returning an
// error aborts the connection with that error.
type Transform func(ctx context.Context, m *Message) (Action, error)

// TransformFactory builds a per-connection Transform; ctx is the request
// context (for auth claims). Stateful transforms (look-back windows) keep their
// state in the returned closure.
type TransformFactory func(ctx context.Context) Transform
