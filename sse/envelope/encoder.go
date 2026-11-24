package envelope

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/sse"
)

// Encoder is the default browser wire of the agent platform: like
// sse.RawEncoder (id = stream position, event = type, data = full envelope
// JSON), except that "stream.token" envelopes are unwrapped to their text so
// browsers append tokens without parsing JSON.
type Encoder struct{ sawDone bool }

// NewEncoder is an sse.EncoderFactory for Encoder.
func NewEncoder() sse.Encoder { return &Encoder{} }

// Header implements sse.Encoder.
func (*Encoder) Header(http.Header) {}

// Encode implements sse.Encoder.
func (e *Encoder) Encode(dst []sse.Event, m sse.Message) []sse.Event {
	data := m.Data
	switch m.Type {
	case sse.TypeToken:
		if text, ok := TokenText(m.Data); ok {
			data = []byte(text)
		}
	case sse.TypeDone:
		e.sawDone = true
	}
	if data == nil {
		data = []byte{}
	}
	return append(dst, sse.Event{ID: m.ID, Type: m.Type, Data: data})
}

// End implements sse.Encoder.
func (e *Encoder) End(dst []sse.Event, reason sse.EndReason) []sse.Event {
	return sse.AppendEndEvents(dst, reason, e.sawDone)
}

// TokenText returns the payload of an envelope as text: a JSON string payload
// is unquoted, any other JSON payload is returned verbatim. ok is false when
// data is not a JSON object with a non-empty event_id. It does not run
// Validate.
func TokenText(data []byte) (string, bool) {
	var probe struct {
		EventID string          `json:"event_id"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &probe); err != nil || probe.EventID == "" {
		return "", false
	}
	return unwrap(probe.Data), true
}

func unwrap(data json.RawMessage) string {
	if len(data) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		return s
	}
	return string(data)
}

// RawPolicy says what ContextValidator does with messages that are not
// envelopes. The zero value is the secure default.
type RawPolicy int

const (
	// RawRejectTokens drops non-envelope "stream.token" messages (user
	// content must be attributable) and passes other non-envelope messages
	// (lifecycle signals carry no user content; dropping them strands clients).
	RawRejectTokens RawPolicy = iota
	// RawRejectAll drops every non-envelope message.
	RawRejectAll
	// RawPass passes every non-envelope message.
	RawPass
)

// Rule is an allowlist per envelope field; an empty list skips that check.
// A Rule with every list empty fails closed: it rejects every envelope
// (reason "no_rule"), so a Resolve that found no claims lets no envelope
// through. Non-envelope messages still follow Raw. Set AllowAll to pass envelopes without checks.
type Rule struct {
	AppIDs, TenantIDs, UserIDs, SessionIDs []string
	// AllowAll passes every envelope when all lists are empty. It has no
	// effect when any list is set.
	AllowAll bool
	Raw      RawPolicy
}

func (r Rule) empty() bool {
	return len(r.AppIDs) == 0 && len(r.TenantIDs) == 0 && len(r.UserIDs) == 0 && len(r.SessionIDs) == 0
}

// ValidatorConfig configures ContextValidator.
type ValidatorConfig struct {
	// Resolve returns the rule for a connection from its request context
	// (JWT claims, admission ticket). Required.
	Resolve func(ctx context.Context) Rule
	// Logger receives one Warn per rejected message (IDs only, never the
	// payload); nil = log.Default().
	Logger *log.Logger
	// MeterProvider for the sse.envelope.rejected counter; nil = global.
	MeterProvider metric.MeterProvider
}

// ContextValidator returns a transform that skips messages whose envelope
// does not match the connection's Rule, so envelopes do not cross the users,
// tenants, apps or sessions the Rule restricts, even if a writer targets the
// wrong stream. Non-envelope messages are handled by Rule.Raw.
func ContextValidator(cfg ValidatorConfig) sse.TransformFactory {
	logger := log.OrDefault(cfg.Logger)
	mp := cfg.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	rejected, _ := mp.Meter(sse.ScopeName).Int64Counter("sse.envelope.rejected",
		metric.WithDescription("Messages skipped by the envelope context validator."))
	return func(ctx context.Context) sse.Transform {
		rule := cfg.Resolve(ctx)
		reject := func(ctx context.Context, reason string, e *Envelope) (sse.Action, error) {
			rejected.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
			attrs := []any{"reason", reason}
			if e != nil {
				attrs = append(attrs, "app_id", e.AppID, "tenant_id", e.TenantID,
					"user_id", e.UserID, "session_id", e.SessionID, "event_id", e.EventID)
			}
			logger.WarnContext(ctx, "sse/envelope: message rejected", attrs...)
			return sse.ActionSkip, nil
		}
		return func(ctx context.Context, m *sse.Message) (sse.Action, error) {
			e, err := Parse(m.Data)
			if err != nil {
				switch {
				case rule.Raw == RawPass, rule.Raw == RawRejectTokens && m.Type != sse.TypeToken:
					return sse.ActionPass, nil
				}
				return reject(ctx, "not_envelope", nil)
			}
			switch {
			case rule.empty() && !rule.AllowAll:
				return reject(ctx, "no_rule", &e)
			case !allowed(rule.AppIDs, e.AppID):
				return reject(ctx, "app_mismatch", &e)
			case !allowed(rule.TenantIDs, e.TenantID):
				return reject(ctx, "tenant_mismatch", &e)
			case !allowed(rule.UserIDs, e.UserID):
				return reject(ctx, "user_mismatch", &e)
			case !allowed(rule.SessionIDs, e.SessionID):
				return reject(ctx, "session_mismatch", &e)
			case e.EventType != m.Type:
				return reject(ctx, "type_mismatch", &e)
			}
			return sse.ActionPass, nil
		}
	}
}

func allowed(list []string, v string) bool { return len(list) == 0 || slices.Contains(list, v) }
