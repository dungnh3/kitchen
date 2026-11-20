// Package mcpkittest connects an in-memory MCP client to an mcpkit server,
// for unit tests of tool handlers.
package mcpkittest

import (
	"context"
	"testing"

	"github.com/dungnh3/kitchen/mcpkit"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Connect serves s over an in-memory transport and returns a connected
// client session. token, if not empty, is visible to tool handlers through
// mcpkit.TokenFrom. The server and session are shut down when the test ends.
func Connect(tb testing.TB, s *mcpkit.Server, token string) *mcp.ClientSession {
	tb.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	serverT, clientT := mcp.NewInMemoryTransports()

	var ts mcpkit.TokenSource
	if token != "" {
		ts = func(context.Context) (string, error) { return token, nil }
	}
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, serverT, ts) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "mcpkittest", Version: "v0.0.0"}, nil)
	cs, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		cancel()
		<-done //nolint:errcheck // already failing; the connect error is what matters
		tb.Fatalf("mcpkittest: connect: %v", err)
	}
	tb.Cleanup(func() {
		_ = cs.Close()
		cancel()
		if err := <-done; err != nil {
			tb.Errorf("mcpkittest: server: %v", err)
		}
	})
	return cs
}
