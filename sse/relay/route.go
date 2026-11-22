package relay

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dungnh3/kitchen/pubsub"
)

// Attribute keys set by EnvelopeRoute (unchanged from the old outbox).
const (
	AttrSource      = "source"
	AttrTenantID    = "tenant_id"
	AttrWorkspaceID = "workspace_id"
	AttrRunID       = "run_id"
	AttrStepID      = "step_id"
	AttrTodoID      = "todo_id"
	AttrEventType   = "event_type"
	AttrEventID     = "event_id"
	AttrOutboxID    = "outbox_id"
	AttrProducedAt  = "produced_at"
	AttrTraceParent = "traceparent"
	AttrTraceState  = "tracestate"
)

// ErrUnroutable marks an entry that cannot be routed (dead-lettered).
var ErrUnroutable = errors.New("relay: unroutable outbox entry")

// EnvelopeRoute routes sse/envelope payloads: ordering key
// "<tenant>:<workspace>:<run>", and the attributes above (run_id = the
// envelope's session_id). Routing fields inside data are best effort.
func EnvelopeRoute(source string) Route {
	return func(e Entry) (*pubsub.Message, error) {
		var env struct {
			EventID   string          `json:"event_id"`
			SessionID string          `json:"session_id"`
			TenantID  string          `json:"tenant_id"`
			EventType string          `json:"event_type"`
			Timestamp json.RawMessage `json:"timestamp"`
			Data      json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(e.Data, &env); err != nil {
			return nil, fmt.Errorf("%w: envelope: %w", ErrUnroutable, err)
		}
		if env.SessionID == "" {
			return nil, fmt.Errorf("%w: envelope without session_id", ErrUnroutable)
		}
		var data struct {
			WorkspaceID string `json:"workspace_id"`
			TodoID      string `json:"todo_id"`
			StepID      string `json:"step_id"`
			TraceParent string `json:"traceparent"`
			TraceState  string `json:"tracestate"`
		}
		if d := bytes.TrimSpace(env.Data); len(d) > 0 && d[0] == '{' {
			_ = json.Unmarshal(d, &data)
		}
		stepID := data.StepID
		if stepID == "" {
			stepID = data.TodoID
		}
		eventType := e.Type
		if eventType == "" {
			eventType = env.EventType
		}
		var producedAt string
		_ = json.Unmarshal(env.Timestamp, &producedAt)
		attrs := map[string]string{
			AttrSource: source, AttrTenantID: env.TenantID, AttrWorkspaceID: data.WorkspaceID,
			AttrRunID: env.SessionID, AttrStepID: stepID, AttrTodoID: data.TodoID,
			AttrEventType: eventType, AttrEventID: env.EventID, AttrOutboxID: e.ID, AttrProducedAt: producedAt,
		}
		if data.TraceParent != "" {
			attrs[AttrTraceParent] = data.TraceParent
			if data.TraceState != "" {
				attrs[AttrTraceState] = data.TraceState
			}
		}
		return &pubsub.Message{
			Key:        env.TenantID + ":" + data.WorkspaceID + ":" + env.SessionID,
			Data:       e.Data,
			Attributes: attrs,
		}, nil
	}
}
