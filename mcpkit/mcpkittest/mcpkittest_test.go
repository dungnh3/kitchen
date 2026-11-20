package mcpkittest_test

import (
	"context"
	"testing"

	"github.com/dungnh3/kitchen/mcpkit"
	"github.com/dungnh3/kitchen/mcpkit/mcpkittest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestConnect(t *testing.T) {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	mcpkit.AddTool(srv, &mcp.Tool{Name: "token"},
		func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: mcpkit.TokenFrom(ctx)}}}, nil, nil
		})

	cs := mcpkittest.Connect(t, srv, "secret")
	res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "token", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Content[0].(*mcp.TextContent).Text; got != "secret" {
		t.Errorf("token = %q", got)
	}
}
