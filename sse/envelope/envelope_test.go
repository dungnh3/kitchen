package envelope_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/envelope"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestBuilderAndParse(t *testing.T) {
	b := envelope.NewBuilder("run1", "app", "u1").WithSource("pod-1")
	e, err := b.BuildText(sse.TypeToken, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if e.TenantID != envelope.DefaultTenant || e.SchemaVersion != "v1" || len(e.EventID) != 26 {
		t.Fatalf("envelope = %+v", e)
	}
	m, err := e.Message()
	if err != nil || m.Type != sse.TypeToken {
		t.Fatalf("Message = %+v, %v", m, err)
	}
	got, err := envelope.Parse(m.Data)
	if err != nil || got.EventID != e.EventID || string(got.Data) != `"hello"` {
		t.Fatalf("Parse = %+v, %v", got, err)
	}

	fixed, err := b.WithEventID("evt-1").BuildJSON("run.started", map[string]int{"n": 1})
	if err != nil || fixed.EventID != "evt-1" {
		t.Fatalf("WithEventID: %+v, %v", fixed, err)
	}
	next, _ := b.BuildJSON("run.started", nil)
	if next.EventID == "evt-1" {
		t.Fatal("WithEventID must apply to one build only")
	}
	if _, err := b.Build("x", json.RawMessage("{bad")); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	if _, err := envelope.NewBuilder("", "a", "u").BuildText("x", ""); !errors.Is(err, envelope.ErrMissingField) {
		t.Fatalf("missing session: %v", err)
	}
	bad := envelope.Envelope{SchemaVersion: "v9", EventID: "1", SessionID: "s", AppID: "a", UserID: "u", EventType: "t"}
	if _, err := bad.Marshal(); !errors.Is(err, envelope.ErrUnsupportedSchema) {
		t.Fatalf("schema: %v", err)
	}
}

func TestULID(t *testing.T) {
	ts := time.UnixMilli(1732089600000)
	a, b := envelope.NewULID(ts), envelope.NewULID(ts.Add(time.Millisecond))
	if len(a) != 26 || a[:10] == "" || a >= b {
		t.Fatalf("ulids not time-sortable: %s %s", a, b)
	}
	if a[:10] != "01JD49BP00" { // Crockford base32 of 1732089600000 ms
		t.Fatalf("time prefix = %s", a[:10])
	}
	if strings.ContainsAny(a, "ILOU") {
		t.Fatalf("invalid Crockford character in %s", a)
	}
}

func TestEncoderUnwrapsTokens(t *testing.T) {
	e, _ := envelope.NewBuilder("r", "a", "u").BuildText(sse.TypeToken, "Hi\nthere")
	m, _ := e.Message()
	m.ID = "1-0"
	j, _ := envelope.NewBuilder("r", "a", "u").BuildJSON("run.started", map[string]string{"k": "v"})
	jm, _ := j.Message()
	enc := envelope.NewEncoder()
	evs := enc.Encode(nil, m)
	evs = enc.Encode(evs, jm)
	evs = enc.Encode(evs, sse.Message{ID: "3-0", Type: sse.TypeToken, Data: []byte("raw")})
	evs = enc.End(evs, sse.EndDone)
	if string(evs[0].Data) != "Hi\nthere" || evs[0].ID != "1-0" {
		t.Fatalf("token = %+v", evs[0])
	}
	if !strings.Contains(string(evs[1].Data), `"event_type":"run.started"`) {
		t.Fatalf("non-token must keep the envelope: %s", evs[1].Data)
	}
	if string(evs[2].Data) != "raw" || evs[3].Type != sse.TypeDone {
		t.Fatalf("events = %+v", evs)
	}
}

func TestContextValidator(t *testing.T) {
	type ctxKey struct{}
	f := envelope.ContextValidator(envelope.ValidatorConfig{
		Resolve: func(ctx context.Context) envelope.Rule { return ctx.Value(ctxKey{}).(envelope.Rule) },
		Logger:  slog.New(slog.DiscardHandler),
	})
	tok := func(user string) sse.Message {
		e, _ := envelope.NewBuilder("r1", "app", user).BuildText(sse.TypeToken, "x")
		m, _ := e.Message()
		return m
	}
	raw := func(typ string) sse.Message { return sse.Message{Type: typ, Data: []byte("plain")} }
	wrongType := tok("u1")
	wrongType.Type = "run.started"

	tests := []struct {
		rule envelope.Rule
		m    sse.Message
		want sse.Action
	}{
		{envelope.Rule{UserIDs: []string{"u1"}}, tok("u1"), sse.ActionPass},
		{envelope.Rule{UserIDs: []string{"u1"}}, tok("u2"), sse.ActionSkip},
		{envelope.Rule{SessionIDs: []string{"other"}}, tok("u1"), sse.ActionSkip},
		{envelope.Rule{TenantIDs: []string{"t9"}}, tok("u1"), sse.ActionSkip},
		{envelope.Rule{AppIDs: []string{"app"}}, tok("u1"), sse.ActionPass},
		{envelope.Rule{}, wrongType, sse.ActionSkip},
		{envelope.Rule{}, raw(sse.TypeToken), sse.ActionSkip}, // secure default
		{envelope.Rule{}, raw("presentation.slide"), sse.ActionPass},
		{envelope.Rule{Raw: envelope.RawRejectAll}, raw("presentation.slide"), sse.ActionSkip},
		{envelope.Rule{Raw: envelope.RawPass}, raw(sse.TypeToken), sse.ActionPass},
	}
	for i, tt := range tests {
		ctx := context.WithValue(context.Background(), ctxKey{}, tt.rule)
		m := tt.m
		got, err := f(ctx)(ctx, &m)
		if err != nil || got != tt.want {
			t.Errorf("case %d: action = %v, %v; want %v", i, got, err, tt.want)
		}
	}
}

func ExampleBuilder() {
	b := envelope.NewBuilder("run_1", "vulcan-chat", "user_1")
	e, _ := b.WithEventID("01JD3BHZG0AAAAAAAAAAAAAAAA").BuildText(sse.TypeToken, "Hello")
	fmt.Println(e.EventType, e.SessionID, string(e.Data))
	// Output: stream.token run_1 "Hello"
}
