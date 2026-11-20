package sse_test

import (
	"fmt"
	"os"

	"github.com/dungnh3/kitchen/sse"
)

func ExampleAppendEvent() {
	b, _ := sse.AppendEvent(nil, sse.Event{ID: "1732089600000-0", Type: sse.TypeToken, Data: []byte("Hello\nworld")})
	_, _ = os.Stdout.Write(b)
	// Output:
	// id: 1732089600000-0
	// event: stream.token
	// data: Hello
	// data: world
}

func ExampleVisibleTypes() {
	visible := sse.VisibleTypes("run.", "approval.required")
	fmt.Println(visible(sse.TypeToken), visible("run.started"), visible("approval.granted"), visible("internal.audit"))
	// Output: true true false false
}
