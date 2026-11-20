package mcpkit_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/dungnh3/kitchen/mcpkit"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

// Serve a typed tool over Streamable HTTP with upstream-token pass-through
// auth and protected-resource metadata.
func Example() {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "mail", Version: "v1.0.0"},
		mcpkit.WithToolTimeout(30*time.Second),
		mcpkit.WithAuth(mcpkit.Auth{
			Verifier:               mcpkit.PassThrough(),
			AllowMissingExpiration: true,
			Metadata: &oauthex.ProtectedResourceMetadata{
				Resource:             "https://mcp.example.com/mcp",
				AuthorizationServers: []string{"https://accounts.google.com"},
			},
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	type SearchIn struct {
		Query string `json:"query" jsonschema:"Gmail search query"`
	}
	type SearchOut struct {
		IDs []string `json:"ids"`
	}
	mcpkit.AddTool(srv, &mcp.Tool{Name: "search_mail", Description: "Search the user's mailbox."},
		func(ctx context.Context, _ *mcp.CallToolRequest, in SearchIn) (*mcp.CallToolResult, SearchOut, error) {
			_, _ = mcpkit.TokenFrom(ctx), in.Query // call the Gmail API with both
			return nil, SearchOut{IDs: []string{"m1"}}, nil
		})

	mux := http.NewServeMux()
	mux.Handle("/mcp", srv.Handler())
	mux.Handle("GET "+srv.MetadataPath(), srv.MetadataHandler())
	fmt.Println(srv.MetadataPath())

	// Run with your own server; no WriteTimeout, it would cut streamed responses.
	_ = &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	// Output: /.well-known/oauth-protected-resource/mcp
}

func ExampleAddTool() {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "greeter"})
	if err != nil {
		log.Fatal(err)
	}
	type In struct {
		Name string `json:"name"`
	}
	type Out struct {
		Message string `json:"message"`
	}
	mcpkit.AddTool(srv, &mcp.Tool{Name: "greet"},
		func(_ context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
			return nil, Out{Message: "hello " + in.Name}, nil
		})

	// Call it over an in-memory transport (mcpkittest.Connect does this in tests).
	ctx, cancel := context.WithCancel(context.Background())
	serverT, clientT := mcp.NewInMemoryTransports()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, serverT, nil) }()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "client"}, nil).Connect(ctx, clientT, nil)
	if err != nil {
		log.Fatal(err)
	}
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "greet", Arguments: map[string]any{"name": "ana"}})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res.Content[0].(*mcp.TextContent).Text)
	_ = cs.Close()
	cancel()
	if err := <-done; err != nil {
		log.Fatal(err)
	}
	// Output: {"message":"hello ana"}
}

func ExampleServer_RunStdio() {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "slack"})
	if err != nil {
		log.Fatal(err)
	}
	// The upstream credential comes from the environment, never from the
	// JSON-RPC payload.
	run := func(ctx context.Context) error { return srv.RunStdio(ctx, mcpkit.EnvToken("SLACK_TOKEN")) }
	_ = run
}
