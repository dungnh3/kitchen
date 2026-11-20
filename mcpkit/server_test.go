package mcpkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/mcpkit"
	"github.com/dungnh3/kitchen/mcpkit/mcpkittest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type greetIn struct {
	Name string `json:"name" jsonschema:"who to greet"`
}

type greetOut struct {
	Message string `json:"message"`
}

func greet(_ context.Context, _ *mcp.CallToolRequest, in greetIn) (*mcp.CallToolResult, greetOut, error) {
	if in.Name == "error" {
		return nil, greetOut{}, errors.New("cannot greet error")
	}
	return nil, greetOut{Message: "hello " + in.Name}, nil
}

func newServer(t *testing.T, opts ...mcpkit.Option) *mcpkit.Server {
	t.Helper()
	s, err := mcpkit.New(&mcp.Implementation{Name: "test", Version: "v1.0.0"}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func call(t *testing.T, cs *mcp.ClientSession, name string, args any) *mcp.CallToolResult {
	t.Helper()
	res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool(%s): %v", name, err)
	}
	return res
}

func text(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		impl *mcp.Implementation
		opts []mcpkit.Option
		want string
	}{
		{"nil impl", nil, nil, "name is required"},
		{"empty name", &mcp.Implementation{}, nil, "name is required"},
		{"negative body", &mcp.Implementation{Name: "x"}, []mcpkit.Option{mcpkit.WithMaxRequestBodyBytes(-1)}, "must not be negative"},
		{"negative timeout", &mcp.Implementation{Name: "x"}, []mcpkit.Option{mcpkit.WithToolTimeout(-time.Second)}, "must not be negative"},
		{"auth without verifier", &mcp.Implementation{Name: "x"}, []mcpkit.Option{mcpkit.WithAuth(mcpkit.Auth{})}, "Verifier is required"},
		{"relative resource", &mcp.Implementation{Name: "x"}, []mcpkit.Option{mcpkit.WithAuth(mcpkit.Auth{
			Verifier: mcpkit.PassThrough(),
			Metadata: &oauthex.ProtectedResourceMetadata{Resource: "/mcp"},
		})}, "absolute URL"},
		{"bad origin", &mcp.Implementation{Name: "x"}, []mcpkit.Option{mcpkit.WithAllowedOrigins("not a url")}, "allowed origin"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mcpkit.New(tt.impl, tt.opts...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestAddToolTyped(t *testing.T) {
	s := newServer(t)
	mcpkit.AddTool(s, &mcp.Tool{Name: "greet", Description: "Greets someone."}, greet)
	cs := mcpkittest.Connect(t, s, "")

	list, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Tools) != 1 || list.Tools[0].InputSchema == nil || list.Tools[0].OutputSchema == nil {
		t.Fatalf("expected inferred input and output schemas, got %+v", list.Tools)
	}
	schema, _ := json.Marshal(list.Tools[0].InputSchema)
	if !strings.Contains(string(schema), "who to greet") {
		t.Errorf("input schema lacks description: %s", schema)
	}

	tests := []struct {
		name      string
		args      any
		wantError bool
		wantText  string
	}{
		{"ok", map[string]any{"name": "ana"}, false, "hello ana"},
		{"handler error is a tool error", map[string]any{"name": "error"}, true, "cannot greet error"},
		{"invalid input is a tool error", map[string]any{"name": 42}, true, "validating"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := call(t, cs, "greet", tt.args)
			if res.IsError != tt.wantError {
				t.Fatalf("IsError = %v, want %v (%s)", res.IsError, tt.wantError, text(res))
			}
			if !strings.Contains(text(res), tt.wantText) {
				t.Errorf("text = %q, want containing %q", text(res), tt.wantText)
			}
			if !tt.wantError {
				b, _ := json.Marshal(res.StructuredContent)
				if string(b) != `{"message":"hello ana"}` {
					t.Errorf("structured content = %s", b)
				}
			}
		})
	}
}

func TestToolTimeout(t *testing.T) {
	block := func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	s := newServer(t, mcpkit.WithToolTimeout(time.Hour))
	mcpkit.AddTool(s, &mcp.Tool{Name: "slow"}, block, mcpkit.Timeout(20*time.Millisecond))
	cs := mcpkittest.Connect(t, s, "")

	res := call(t, cs, "slow", map[string]any{})
	if !res.IsError || !strings.Contains(text(res), `tool "slow" timed out after 20ms`) {
		t.Fatalf("got IsError=%v text=%q", res.IsError, text(res))
	}
}

func TestPanicRecovery(t *testing.T) {
	boom := func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, any, error) {
		panic("secret detail")
	}
	s := newServer(t, mcpkit.WithLogger(discardLogger()))
	mcpkit.AddTool(s, &mcp.Tool{Name: "boom"}, boom)
	mcpkit.AddTool(s, &mcp.Tool{Name: "greet"}, greet)
	cs := mcpkittest.Connect(t, s, "")

	res := call(t, cs, "boom", map[string]any{})
	if !res.IsError || text(res) != "internal error" {
		t.Fatalf("got IsError=%v text=%q, want generic internal error", res.IsError, text(res))
	}
	if got := text(call(t, cs, "greet", map[string]any{"name": "a"})); got != `{"message":"hello a"}` {
		t.Errorf("server unusable after panic: %q", got)
	}
}

func TestToolsListIsSorted(t *testing.T) {
	s := newServer(t)
	for _, n := range []string{"zeta", "alpha", "mid"} {
		mcpkit.AddTool(s, &mcp.Tool{Name: n}, greet)
	}
	cs := mcpkittest.Connect(t, s, "")
	list, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tl := range list.Tools {
		names = append(names, tl.Name)
	}
	if strings.Join(names, ",") != "alpha,mid,zeta" {
		t.Errorf("tools/list order = %v", names)
	}
}

func whoami(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "token=" + mcpkit.TokenFrom(ctx)}}}, nil, nil
}

func TestRunTokenSource(t *testing.T) {
	s := newServer(t)
	mcpkit.AddTool(s, &mcp.Tool{Name: "whoami"}, whoami)
	cs := mcpkittest.Connect(t, s, "xoxb-env")
	if got := text(call(t, cs, "whoami", map[string]any{})); got != "token=xoxb-env" {
		t.Errorf("got %q", got)
	}
}

func TestRunCancelReturnsNil(t *testing.T) {
	s := newServer(t)
	ctx, cancel := context.WithCancel(t.Context())
	serverT, clientT := mcp.NewInMemoryTransports()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, serverT, nil) }()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "c"}, nil).Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run after cancel = %v, want nil", err)
	}
	_ = cs.Close()
}

func TestRunTokenSourceError(t *testing.T) {
	s := newServer(t)
	t.Setenv("MCPKIT_TEST_UNSET", "")
	serverT, _ := mcp.NewInMemoryTransports()
	err := s.Run(t.Context(), serverT, mcpkit.EnvToken("MCPKIT_TEST_UNSET"))
	if err == nil || !strings.Contains(err.Error(), "MCPKIT_TEST_UNSET") {
		t.Fatalf("Run = %v, want env error", err)
	}
}

// bearerTransport adds an Authorization header to every request.
type bearerTransport struct{ token string }

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

func connectHTTP(t *testing.T, url, token string) (*mcp.ClientSession, error) {
	t.Helper()
	hc := &http.Client{}
	if token != "" {
		hc.Transport = bearerTransport{token}
	}
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "c"}, nil).Connect(t.Context(),
		&mcp.StreamableClientTransport{Endpoint: url, HTTPClient: hc}, nil)
	if err == nil {
		t.Cleanup(func() { _ = cs.Close() })
	}
	return cs, err
}

func TestHTTPStatelessNoAuth(t *testing.T) {
	s := newServer(t)
	mcpkit.AddTool(s, &mcp.Tool{Name: "greet"}, greet)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	cs, err := connectHTTP(t, ts.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	if cs.ID() != "" {
		t.Errorf("stateless server returned session id %q", cs.ID())
	}
	if got := text(call(t, cs, "greet", map[string]any{"name": "b"})); got != `{"message":"hello b"}` {
		t.Errorf("got %q", got)
	}
}

func TestHTTPStateful(t *testing.T) {
	s := newServer(t, mcpkit.WithStateful(time.Minute))
	mcpkit.AddTool(s, &mcp.Tool{Name: "greet"}, greet)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	cs, err := connectHTTP(t, ts.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := text(call(t, cs, "greet", map[string]any{"name": "c"})); got != `{"message":"hello c"}` {
		t.Errorf("got %q", got)
	}
	_ = cs.Close()
}

func authServer(t *testing.T) (*mcpkit.Server, *httptest.Server) {
	t.Helper()
	mux := http.NewServeMux()
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	s := newServer(t, mcpkit.WithAuth(mcpkit.Auth{
		Verifier:               mcpkit.PassThrough(),
		AllowMissingExpiration: true,
		Metadata: &oauthex.ProtectedResourceMetadata{
			Resource:             ts.URL + "/mcp",
			AuthorizationServers: []string{"https://idp.example.com"},
		},
	}))
	mcpkit.AddTool(s, &mcp.Tool{Name: "whoami"}, whoami)
	mux.Handle("/mcp", s.Handler())
	mux.Handle("GET "+s.MetadataPath(), s.MetadataHandler())
	return s, ts
}

func TestHTTPAuthMissingToken(t *testing.T) {
	s, ts := authServer(t)
	if s.MetadataPath() != "/.well-known/oauth-protected-resource/mcp" {
		t.Fatalf("MetadataPath = %q", s.MetadataPath())
	}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/mcp", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	want := `resource_metadata="` + ts.URL + `/.well-known/oauth-protected-resource/mcp"`
	if got := resp.Header.Get("WWW-Authenticate"); !strings.Contains(got, want) {
		t.Errorf("WWW-Authenticate = %q, want containing %q", got, want)
	}

	req, _ = http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+s.MetadataPath(), nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var prm oauthex.ProtectedResourceMetadata
	if err := json.NewDecoder(resp.Body).Decode(&prm); err != nil {
		t.Fatal(err)
	}
	if prm.Resource != ts.URL+"/mcp" {
		t.Errorf("metadata resource = %q", prm.Resource)
	}
}

func TestHTTPAuthTokenReachesTool(t *testing.T) {
	_, ts := authServer(t)
	cs, err := connectHTTP(t, ts.URL+"/mcp", "upstream-123")
	if err != nil {
		t.Fatal(err)
	}
	if got := text(call(t, cs, "whoami", map[string]any{})); got != "token=upstream-123" {
		t.Errorf("got %q", got)
	}
}

func TestHTTPScopes(t *testing.T) {
	s := newServer(t, mcpkit.WithAuth(mcpkit.Auth{
		Verifier:               mcpkit.PassThrough(),
		AllowMissingExpiration: true,
		Scopes:                 []string{"mail.read"},
	}))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL, strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer t")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403 for missing scope", resp.StatusCode)
	}
}

func TestCORS(t *testing.T) {
	s := newServer(t, mcpkit.WithAllowedOrigins("https://app.example.com"))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	tests := []struct {
		name       string
		method     string
		origin     string
		headers    map[string]string
		wantStatus int
		wantACAO   string
	}{
		{
			"allowed preflight", http.MethodOptions, "https://app.example.com",
			map[string]string{"Access-Control-Request-Method": "POST"},
			http.StatusNoContent, "https://app.example.com",
		},
		{
			"denied preflight", http.MethodOptions, "https://evil.example.com",
			map[string]string{"Access-Control-Request-Method": "POST"},
			http.StatusForbidden, "",
		},
		{
			"denied cross-site post", http.MethodPost, "https://evil.example.com",
			map[string]string{"Sec-Fetch-Site": "cross-site", "Content-Type": "application/json"},
			http.StatusForbidden, "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequestWithContext(t.Context(), tt.method, ts.URL, strings.NewReader(`{}`))
			req.Header.Set("Origin", tt.origin)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if got := resp.Header.Get("Access-Control-Allow-Origin"); got != tt.wantACAO {
				t.Errorf("ACAO = %q, want %q", got, tt.wantACAO)
			}
		})
	}
}
