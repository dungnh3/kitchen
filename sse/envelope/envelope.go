// Package envelope is the JSON envelope that agent-platform writers put in
// sse.Message.Data: routing and audit metadata around an event payload. It
// also provides the browser Encoder that keeps today's wire format and a
// Transform that checks envelopes against the caller's identity.
package envelope

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/sse"
)

// SchemaVersion is the only supported envelope schema.
const SchemaVersion = "v1"

var (
	// ErrMissingField means a required field is empty.
	ErrMissingField = errs.New(codes.InvalidArgument, "SSE_ENVELOPE_MISSING_FIELD", errs.Message("envelope: missing required field"))
	// ErrUnsupportedSchema means schema_version is set and is not "v1".
	ErrUnsupportedSchema = errs.New(codes.InvalidArgument, "SSE_ENVELOPE_UNSUPPORTED_SCHEMA", errs.Message("envelope: unsupported schema version"))
)

// Envelope wraps an event payload. JSON keys are unchanged from the old
// kitchen/sse envelope.
type Envelope struct {
	SchemaVersion string          `json:"schema_version,omitempty"`
	EventID       string          `json:"event_id"`            // ULID, required
	SessionID     string          `json:"session_id"`          // stream ID, required
	TenantID      string          `json:"tenant_id,omitempty"` // optional
	AppID         string          `json:"app_id"`              // required
	UserID        string          `json:"user_id"`             // required
	Source        string          `json:"source,omitempty"`    // debugging hint
	EventType     string          `json:"event_type"`          // required, = sse.Message.Type
	Timestamp     time.Time       `json:"timestamp"`           // UTC creation time
	Data          json.RawMessage `json:"data,omitempty"`      // event payload
}

// Validate checks required fields and the schema version.
func (e *Envelope) Validate() error {
	for _, f := range [...]struct{ name, v string }{
		{"event_id", e.EventID},
		{"session_id", e.SessionID},
		{"app_id", e.AppID},
		{"user_id", e.UserID},
		{"event_type", e.EventType},
	} {
		if f.v == "" {
			return fmt.Errorf("%w %q", ErrMissingField, f.name)
		}
	}
	if e.SchemaVersion != "" && e.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w %q", ErrUnsupportedSchema, e.SchemaVersion)
	}
	return nil
}

// Marshal fills SchemaVersion and Timestamp when empty, validates and encodes.
func (e Envelope) Marshal() ([]byte, error) {
	if e.SchemaVersion == "" {
		e.SchemaVersion = SchemaVersion
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("envelope: marshal: %w", err)
	}
	return b, nil
}

// Message marshals e into an sse.Message of type e.EventType.
func (e Envelope) Message() (sse.Message, error) {
	b, err := e.Marshal()
	if err != nil {
		return sse.Message{}, err
	}
	return sse.Message{Type: e.EventType, Data: b}, nil
}

// Parse decodes and validates an envelope.
func Parse(data []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return Envelope{}, fmt.Errorf("envelope: unmarshal: %w", err)
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

// Builder fills the per-stream fields of envelopes.
type Builder struct {
	sessionID, appID, userID, tenantID, source, eventID string
}

// DefaultTenant is the TenantID a Builder uses until WithTenantID is called.
const DefaultTenant = "default"

// NewBuilder returns a Builder for one stream and caller. TenantID starts as
// DefaultTenant.
func NewBuilder(sessionID, appID, userID string) *Builder {
	return &Builder{sessionID: sessionID, appID: appID, userID: userID, tenantID: DefaultTenant}
}

// WithTenantID sets the tenant.
func (b *Builder) WithTenantID(id string) *Builder { b.tenantID = id; return b }

// WithSource sets the source hint.
func (b *Builder) WithSource(s string) *Builder { b.source = s; return b }

// WithEventID fixes the next event ID (for idempotent replays); it is used
// once and then cleared.
func (b *Builder) WithEventID(id string) *Builder { b.eventID = id; return b }

// Build returns an envelope with a raw JSON payload.
func (b *Builder) Build(eventType string, data json.RawMessage) (Envelope, error) {
	id := b.eventID
	b.eventID = ""
	if id == "" {
		id = NewULID(time.Now())
	}
	if data != nil && !json.Valid(data) {
		return Envelope{}, errors.New("envelope: data is not valid JSON")
	}
	e := Envelope{
		SchemaVersion: SchemaVersion, EventID: id, SessionID: b.sessionID, TenantID: b.tenantID,
		AppID: b.appID, UserID: b.userID, Source: b.source, EventType: eventType,
		Timestamp: time.Now().UTC(), Data: data,
	}
	return e, e.Validate()
}

// BuildText returns an envelope whose payload is the JSON string text.
func (b *Builder) BuildText(eventType, text string) (Envelope, error) {
	raw, _ := json.Marshal(text) // strings always marshal
	return b.Build(eventType, raw)
}

// BuildJSON returns an envelope whose payload is v encoded as JSON.
func (b *Builder) BuildJSON(eventType string, v any) (Envelope, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return Envelope{}, fmt.Errorf("envelope: marshal data: %w", err)
	}
	return b.Build(eventType, raw)
}

// ulidEntropy is crypto-random and monotonic within a millisecond, so IDs
// minted in the same millisecond still sort in creation order.
var ulidEntropy = &ulid.LockedMonotonicReader{MonotonicReader: ulid.Monotonic(rand.Reader, 0)}

// NewULID returns a 26-character ULID (oklog/ulid): 48-bit millisecond
// time and 80 bits of crypto/rand entropy, monotonic within a millisecond,
// lexicographically sortable by time.
func NewULID(t time.Time) string {
	id, err := ulid.New(ulid.Timestamp(t), ulidEntropy)
	if err != nil { // monotonic overflow (2^80 IDs in one ms) or a clock before 1970
		id, err = ulid.New(ulid.Timestamp(t), rand.Reader)
		if err != nil {
			id = ulid.Make()
		}
	}
	return id.String()
}
