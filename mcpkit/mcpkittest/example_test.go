package mcpkittest_test

import (
	"context"
	"testing"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/mcpkit"
	"github.com/dungnh3/kitchen/mcpkit/mcpkittest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// A tool test: register the tools, connect an in-memory client, call them
// and assert on IsError and the message the model sees.
func ExampleConnect() {
	type In struct {
		Name string `json:"name"`
	}
	greet := func(_ context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
		if in.Name == "" {
			return nil, nil, errs.ErrInvalidArgument.With(errs.Message("name must not be empty"))
		}
		return nil, map[string]string{"greeting": "Hello, " + in.Name}, nil
	}

	testGreet := func(t *testing.T) {
		srv, err := mcpkit.New(&mcp.Implementation{Name: "test"}, mcpkit.WithLogger(log.Discard()))
		if err != nil {
			t.Fatal(err)
		}
		mcpkit.AddTool(srv, &mcp.Tool{Name: "greet"}, greet)
		cs := mcpkittest.Connect(t, srv, "") // a token here is seen by mcpkit.TokenFrom

		res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "greet", Arguments: map[string]any{"name": ""}})
		if err != nil || !res.IsError {
			t.Fatalf("want a tool error, got %v, %v", res, err)
		}
		if msg := res.Content[0].(*mcp.TextContent).Text; msg != "name must not be empty" {
			t.Errorf("message = %q", msg)
		}
	}
	_ = testGreet // in a _test.go file: func TestGreet(t *testing.T) { ... }
}

func TestConnectToolError(t *testing.T) {
	// The tool-error assertion of ExampleConnect, run for real.
	type In struct {
		Name string `json:"name"`
	}
	srv, err := mcpkit.New(&mcp.Implementation{Name: "test"}, mcpkit.WithLogger(log.Discard()))
	if err != nil {
		t.Fatal(err)
	}
	mcpkit.AddTool(srv, &mcp.Tool{Name: "greet"},
		func(_ context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
			if in.Name == "" {
				return nil, nil, errs.ErrInvalidArgument.With(errs.Message("name must not be empty"))
			}
			return nil, nil, nil
		})
	cs := mcpkittest.Connect(t, srv, "")
	res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "greet", Arguments: map[string]any{"name": ""}})
	if err != nil || !res.IsError {
		t.Fatalf("want a tool error, got %v, %v", res, err)
	}
	if msg := res.Content[0].(*mcp.TextContent).Text; msg != "name must not be empty" {
		t.Errorf("message = %q", msg)
	}
}
