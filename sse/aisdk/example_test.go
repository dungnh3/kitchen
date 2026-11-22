package aisdk_test

import (
	"encoding/json"
	"fmt"

	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/aisdk"
	"github.com/dungnh3/kitchen/sse/envelope"
)

// Options.Map turns service event types into AI SDK parts. Pass the factory
// as sse.ServeOptions.Encoder.
func ExampleNewEncoderWith() {
	factory := aisdk.NewEncoderWith(aisdk.Options{Map: func(t string, data json.RawMessage) ([]aisdk.Part, bool) {
		if t != "agent.tool_call" {
			return nil, false // default: data-<type>
		}
		var c struct {
			ID, Name string
			Args     json.RawMessage
		}
		_ = json.Unmarshal(data, &c)
		return []aisdk.Part{{"type": "tool-input-available", "toolCallId": c.ID, "toolName": c.Name, "input": c.Args}}, true
	}})
	opts := sse.ServeOptions{Encoder: factory}

	e, _ := envelope.NewBuilder("run_1", "chat", "user_1").WithEventID("01JD3BHZG0AAAAAAAAAAAAAAAA").
		BuildJSON("agent.tool_call", map[string]any{"ID": "call_1", "Name": "search", "Args": map[string]string{"q": "go"}})
	m, _ := e.Message()
	m.ID = "1732089600000-0"
	for _, ev := range opts.Encoder().Encode(nil, m) {
		fmt.Println(string(ev.Data))
	}
	// Output:
	// {"messageId":"01JD3BHZG0AAAAAAAAAAAAAAAA","type":"start"}
	// {"input":{"q":"go"},"toolCallId":"call_1","toolName":"search","type":"tool-input-available"}
}
