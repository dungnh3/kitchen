// Package aisdk encodes sse streams as the Vercel AI SDK UI message stream
// protocol (header "x-vercel-ai-ui-message-stream: v1"), so browsers can use
// the AI SDK's useChat directly.
//
// Mapping from agent-platform envelopes (sse/envelope):
//
//	stream.started   → start {messageId}
//	stream.token     → text-start {id} (once), then text-delta {id, delta}
//	stream.done      → text-end {id} (if text started), finish
//	stream.error     → error {errorText}
//	stream.cancelled → abort {reason}
//	any other type   → data-<type> {id, data}   (e.g. "data-run.started")
//	end of stream    → finish (if not sent), then "data: [DONE]"
//
// Each event keeps the stream position as its SSE id, so the gateway can still
// resume. Non-envelope payloads are passed as data parts (JSON) or text.
//
// Services map their own event types (reasoning, tool calls, sources, steps)
// to AI SDK parts with Options.Map instead of the data-<type> fallback:
//
//	aisdk.NewEncoderWith(aisdk.Options{Map: func(t string, data json.RawMessage) ([]aisdk.Part, bool) {
//		if t != "agent.tool_call" {
//			return nil, false
//		}
//		var c struct{ ID, Name string; Args json.RawMessage }
//		_ = json.Unmarshal(data, &c)
//		return []aisdk.Part{{"type": "tool-input-available", "toolCallId": c.ID, "toolName": c.Name, "input": c.Args}}, true
//	}})
package aisdk

import (
	"encoding/json"
	"net/http"

	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/envelope"
)

// HeaderName and HeaderValue identify the protocol version.
const (
	HeaderName  = "x-vercel-ai-ui-message-stream"
	HeaderValue = "v1"
)

// Part is one UI message stream part: a JSON object whose "type" names it
// ("text-delta", "reasoning-delta", "tool-input-available", "start-step",
// "data-<name>", ...). See https://ai-sdk.dev/docs/ai-sdk-ui/stream-protocol.
type Part map[string]any

// Mapper maps one service event (its type and envelope payload, or raw data
// for non-envelope messages) to AI SDK parts. handled=false falls back to
// the default mapping (data-<type>); handled=true with no parts hides it.
type Mapper func(eventType string, data json.RawMessage) (parts []Part, handled bool)

// Options configures NewEncoderWith.
type Options struct {
	// Map handles event types other than the library's stream.* types.
	Map Mapper
}

// NewEncoderWith returns an sse.EncoderFactory using opts.
func NewEncoderWith(opts Options) sse.EncoderFactory {
	return func() sse.Encoder { return &Encoder{mapper: opts.Map} }
}

// Encoder implements sse.Encoder; one per connection.
type Encoder struct {
	mapper    Mapper
	messageID string
	textID    string
	textOpen  bool
	started   bool
	finished  bool
}

// NewEncoder is an sse.EncoderFactory.
func NewEncoder() sse.Encoder { return &Encoder{} }

// Header implements sse.Encoder.
func (*Encoder) Header(h http.Header) { h.Set(HeaderName, HeaderValue) }

func (e *Encoder) emit(dst []sse.Event, id string, p Part) []sse.Event {
	b, err := json.Marshal(p)
	if err != nil {
		return dst
	}
	return append(dst, sse.Event{ID: id, Data: b})
}

// Encode implements sse.Encoder.
func (e *Encoder) Encode(dst []sse.Event, m sse.Message) []sse.Event {
	env, err := envelope.Parse(m.Data)
	isEnv := err == nil
	var payload json.RawMessage
	eventID := m.ID
	if isEnv {
		payload = env.Data
		eventID = env.EventID
	}
	switch m.Type {
	case sse.TypeResumed, sse.TypeReplayGap:
		return dst // resume markers have no AI SDK equivalent
	case sse.TypeStarted:
		return e.start(dst, m.ID, eventID)
	case sse.TypeToken:
		dst = e.start(dst, "", eventID)
		text := string(m.Data)
		if isEnv {
			text, _ = envelope.TokenText(m.Data)
		}
		if !e.textOpen {
			e.textOpen = true
			e.textID = eventID
			dst = e.emit(dst, "", Part{"type": "text-start", "id": e.textID})
		}
		return e.emit(dst, m.ID, Part{"type": "text-delta", "id": e.textID, "delta": text})
	case sse.TypeDone:
		dst = e.closeText(dst)
		return e.finish(dst, m.ID)
	case sse.TypeError:
		return e.emit(dst, m.ID, Part{"type": "error", "errorText": errorText(payload, isEnv, m.Data)})
	case sse.TypeCancelled:
		return e.emit(dst, m.ID, Part{"type": "abort", "reason": "cancelled"})
	}
	if e.mapper != nil {
		raw := payload
		if !isEnv {
			raw = m.Data
		}
		if parts, handled := e.mapper(m.Type, raw); handled {
			if len(parts) == 0 {
				return dst
			}
			dst = e.start(dst, "", eventID)
			for i, p := range parts {
				if t, ok := p["type"].(string); !ok || t == "" {
					continue // a part without a type would break the client
				}
				id := ""
				if i == len(parts)-1 {
					id = m.ID // resume position on the last part only
				}
				dst = e.emit(dst, id, p)
			}
			return dst
		}
	}
	var data any = payload
	if !isEnv {
		if json.Valid(m.Data) {
			data = json.RawMessage(m.Data)
		} else {
			data = string(m.Data)
		}
	} else if len(payload) == 0 {
		data = nil
	}
	return e.emit(dst, m.ID, Part{"type": "data-" + m.Type, "id": eventID, "data": data})
}

func errorText(payload json.RawMessage, isEnv bool, raw []byte) string {
	src := raw
	if isEnv {
		src = payload
	}
	var obj struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(src, &obj) == nil && obj.Message != "" {
		return obj.Message
	}
	var s string
	if json.Unmarshal(src, &s) == nil && s != "" {
		return s
	}
	return "stream error"
}

func (e *Encoder) start(dst []sse.Event, id, messageID string) []sse.Event {
	if e.started {
		return dst
	}
	e.started = true
	e.messageID = messageID
	p := Part{"type": "start"}
	if messageID != "" {
		p["messageId"] = messageID
	}
	return e.emit(dst, id, p)
}

func (e *Encoder) closeText(dst []sse.Event) []sse.Event {
	if !e.textOpen {
		return dst
	}
	e.textOpen = false
	return e.emit(dst, "", Part{"type": "text-end", "id": e.textID})
}

func (e *Encoder) finish(dst []sse.Event, id string) []sse.Event {
	if e.finished {
		return dst
	}
	e.finished = true
	return e.emit(dst, id, Part{"type": "finish"})
}

// End implements sse.Encoder.
func (e *Encoder) End(dst []sse.Event, reason sse.EndReason) []sse.Event {
	switch reason {
	case sse.EndShutdown:
		return dst // let the client resume elsewhere
	case sse.EndGap:
		dst = e.emit(dst, "", Part{"type": "error", "errorText": "stream history is no longer available"})
	case sse.EndIdle:
		dst = e.emit(dst, "", Part{"type": "error", "errorText": "stream idle timeout"})
	case sse.EndDone, sse.EndStopped:
	}
	dst = e.closeText(dst)
	dst = e.finish(dst, "")
	return append(dst, sse.Event{Data: []byte("[DONE]")})
}
