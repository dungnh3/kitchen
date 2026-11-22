package mcpkit_test

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/dungnh3/kitchen/mcpkit"
	"github.com/dungnh3/kitchen/server"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

var (
	_ server.Service       = (*mcpkit.Service)(nil)
	_ server.HTTPRegistrar = (*mcpkit.Service)(nil)
)

func listen(t *testing.T) net.Listener {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func TestServiceOnKitchenServer(t *testing.T) {
	s := newServer(t, mcpkit.WithAuth(mcpkit.Auth{
		Verifier:               mcpkit.PassThrough(),
		AllowMissingExpiration: true,
		Metadata:               &oauthex.ProtectedResourceMetadata{Resource: "https://mcp.example.com/mcp"},
	}))
	mcpkit.AddTool(s, &mcp.Tool{Name: "greet"}, greet)
	svc := s.Service("/mcp")
	if got := svc.Patterns(); len(got) != 2 || got[0] != "/mcp" || got[1] != "GET /.well-known/oauth-protected-resource/mcp" {
		t.Fatalf("patterns = %v", got)
	}

	httpLn := listen(t)
	ks, err := server.New(server.WithServices(svc), server.WithHTTPListener(httpLn), server.WithGRPCAddr(""))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- ks.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("server: %v", err)
		}
	}()
	base := "http://" + httpLn.Addr().String()

	cs, err := connectHTTP(t, base+"/mcp", "tok")
	if err != nil {
		t.Fatal(err)
	}
	if got := text(call(t, cs, "greet", map[string]any{"name": "ana"})); got != `{"message":"hello ana"}` {
		t.Fatalf("got %q", got)
	}
	_ = cs.Close()

	for path, want := range map[string]int{
		"/.well-known/oauth-protected-resource/mcp": http.StatusOK,
		"/livez": http.StatusOK,
	} {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, base+path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("%s: %d, want %d", path, resp.StatusCode, want)
		}
	}
}
