package aisdk_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/aisdk"
	"github.com/dungnh3/kitchen/sse/envelope"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func msg(t *testing.T, id, typ string, build func(*envelope.Builder) (envelope.Envelope, error)) sse.Message {
	t.Helper()
	e, err := build(envelope.NewBuilder("r", "a", "u").WithEventID("E" + id))
	if err != nil {
		t.Fatal(err)
	}
	m, err := e.Message()
	if err != nil {
		t.Fatal(err)
	}
	m.ID, m.Type = id, typ
	return m
}

func wire(evs []sse.Event) string {
	var b []byte
	for _, e := range evs {
		b, _ = sse.AppendEvent(b, e)
	}
	return string(b)
}

func TestEncoderFlow(t *testing.T) {
	enc := aisdk.NewEncoder()
	h := http.Header{}
	enc.Header(h)
	if h.Get(aisdk.HeaderName) != "v1" {
		t.Fatal("missing protocol header")
	}
	var evs []sse.Event
	evs = enc.Encode(evs, msg(t, "1-0", sse.TypeStarted, func(b *envelope.Builder) (envelope.Envelope, error) {
		return b.BuildJSON(sse.TypeStarted, map[string]string{"model": "m"})
	}))
	for i, tok := range []string{"Hel", "lo"} {
		evs = enc.Encode(evs, msg(t, fmt.Sprintf("%d-0", i+2), sse.TypeToken, func(b *envelope.Builder) (envelope.Envelope, error) {
			return b.BuildText(sse.TypeToken, tok)
		}))
	}
	evs = enc.Encode(evs, msg(t, "4-0", "run.step", func(b *envelope.Builder) (envelope.Envelope, error) {
		return b.BuildJSON("run.step", map[string]int{"n": 1})
	}))
	evs = enc.Encode(evs, msg(t, "5-0", sse.TypeDone, func(b *envelope.Builder) (envelope.Envelope, error) {
		return b.BuildJSON(sse.TypeDone, map[string]string{"finish_reason": "stop"})
	}))
	evs = enc.End(evs, sse.EndDone)
	got := wire(evs)
	want := strings.Join([]string{
		`id: 1-0` + "\n" + `data: {"messageId":"E1-0","type":"start"}`,
		`data: {"id":"E2-0","type":"text-start"}`,
		`id: 2-0` + "\n" + `data: {"delta":"Hel","id":"E2-0","type":"text-delta"}`,
		`id: 3-0` + "\n" + `data: {"delta":"lo","id":"E2-0","type":"text-delta"}`,
		`id: 4-0` + "\n" + `data: {"data":{"n":1},"id":"E4-0","type":"data-run.step"}`,
		`data: {"id":"E2-0","type":"text-end"}`,
		`id: 5-0` + "\n" + `data: {"type":"finish"}`,
		`data: [DONE]`,
	}, "\n\n") + "\n\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestEncoderEndReasons(t *testing.T) {
	for _, tt := range []struct {
		reason sse.EndReason
		want   string
	}{
		{sse.EndGap, `"errorText":"stream history is no longer available"`},
		{sse.EndIdle, `"errorText":"stream idle timeout"`},
	} {
		got := wire(aisdk.NewEncoder().End(nil, tt.reason))
		if !strings.Contains(got, tt.want) || !strings.HasSuffix(got, "data: [DONE]\n\n") {
			t.Errorf("%v: %s", tt.reason, got)
		}
	}
	if evs := aisdk.NewEncoder().End(nil, sse.EndShutdown); len(evs) != 0 {
		t.Errorf("shutdown must emit nothing: %v", evs)
	}
	enc := aisdk.NewEncoder()
	got := wire(enc.Encode(nil, sse.Message{ID: "1-0", Type: sse.TypeError, Data: []byte(`{"message":"quota"}`)}))
	if !strings.Contains(got, `"errorText":"quota"`) {
		t.Errorf("error part: %s", got)
	}
	got = wire(enc.Encode(nil, sse.Message{ID: "2-0", Type: "x.raw", Data: []byte("not json")}))
	if !strings.Contains(got, `"data":"not json"`) {
		t.Errorf("raw data part: %s", got)
	}
}

func TestMapperHook(t *testing.T) {
	newEnc := aisdk.NewEncoderWith(aisdk.Options{Map: func(typ string, data json.RawMessage) ([]aisdk.Part, bool) {
		switch typ {
		case "agent.tool_call":
			var c struct {
				ID   string          `json:"id"`
				Name string          `json:"name"`
				Args json.RawMessage `json:"args"`
			}
			_ = json.Unmarshal(data, &c)
			return []aisdk.Part{
				{"type": "start-step"},
				{"type": "tool-input-available", "toolCallId": c.ID, "toolName": c.Name, "input": c.Args},
			}, true
		case "agent.internal":
			return nil, true // hidden
		case "agent.bad":
			return []aisdk.Part{{"toolCallId": "x"}}, true // no type: dropped
		}
		return nil, false
	}})
	enc := newEnc()
	var evs []sse.Event
	evs = enc.Encode(evs, msg(t, "1-0", "agent.tool_call", func(b *envelope.Builder) (envelope.Envelope, error) {
		return b.BuildJSON("agent.tool_call", map[string]any{"id": "c1", "name": "search", "args": map[string]string{"q": "go"}})
	}))
	evs = enc.Encode(evs, msg(t, "2-0", "agent.internal", func(b *envelope.Builder) (envelope.Envelope, error) {
		return b.BuildJSON("agent.internal", map[string]int{"n": 1})
	}))
	evs = enc.Encode(evs, msg(t, "3-0", "agent.bad", func(b *envelope.Builder) (envelope.Envelope, error) {
		return b.BuildJSON("agent.bad", map[string]int{"n": 1})
	}))
	evs = enc.Encode(evs, msg(t, "4-0", "run.step", func(b *envelope.Builder) (envelope.Envelope, error) {
		return b.BuildJSON("run.step", map[string]int{"n": 1})
	}))
	got := wire(evs)
	want := strings.Join([]string{
		`data: {"messageId":"E1-0","type":"start"}`,
		`data: {"type":"start-step"}`,
		`id: 1-0` + "\n" + `data: {"input":{"q":"go"},"toolCallId":"c1","toolName":"search","type":"tool-input-available"}`,
		`id: 4-0` + "\n" + `data: {"data":{"n":1},"id":"E4-0","type":"data-run.step"}`,
	}, "\n\n") + "\n\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}
