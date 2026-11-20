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

// Encoder implements sse.Encoder; one per connection.
type Encoder struct {
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

type part map[string]any

func (e *Encoder) emit(dst []sse.Event, id string, p part) []sse.Event {
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
			dst = e.emit(dst, "", part{"type": "text-start", "id": e.textID})
		}
		return e.emit(dst, m.ID, part{"type": "text-delta", "id": e.textID, "delta": text})
	case sse.TypeDone:
		dst = e.closeText(dst)
		return e.finish(dst, m.ID)
	case sse.TypeError:
		return e.emit(dst, m.ID, part{"type": "error", "errorText": errorText(payload, isEnv, m.Data)})
	case sse.TypeCancelled:
		return e.emit(dst, m.ID, part{"type": "abort", "reason": "cancelled"})
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
	return e.emit(dst, m.ID, part{"type": "data-" + m.Type, "id": eventID, "data": data})
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
	p := part{"type": "start"}
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
	return e.emit(dst, "", part{"type": "text-end", "id": e.textID})
}

func (e *Encoder) finish(dst []sse.Event, id string) []sse.Event {
	if e.finished {
		return dst
	}
	e.finished = true
	return e.emit(dst, id, part{"type": "finish"})
}

// End implements sse.Encoder.
func (e *Encoder) End(dst []sse.Event, reason sse.EndReason) []sse.Event {
	switch reason {
	case sse.EndShutdown:
		return dst // let the client resume elsewhere
	case sse.EndGap:
		dst = e.emit(dst, "", part{"type": "error", "errorText": "stream history is no longer available"})
	case sse.EndIdle:
		dst = e.emit(dst, "", part{"type": "error", "errorText": "stream idle timeout"})
	case sse.EndDone, sse.EndStopped:
	}
	dst = e.closeText(dst)
	dst = e.finish(dst, "")
	return append(dst, sse.Event{Data: []byte("[DONE]")})
}
