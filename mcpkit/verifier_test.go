package mcpkit_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	kauth "github.com/dungnh3/kitchen/auth"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/mcpkit"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
)

const resource = "https://mcp.example.com/mcp"

// fakeVerifier accepts tokens listed in its map.
type fakeVerifier map[string]*kauth.Principal

func (fakeVerifier) Issuer() string { return "https://idp.example.com" }

func (f fakeVerifier) Verify(_ context.Context, token string) (*kauth.Principal, error) {
	switch token {
	case "down":
		return nil, errs.Wrap(fmt.Errorf("jwks fetch failed"), kauth.ErrAuthUnavailable)
	}
	p, ok := f[token]
	if !ok {
		return nil, kauth.ErrTokenInvalid
	}
	return p, nil
}

func TestVerifierValidation(t *testing.T) {
	if _, err := mcpkit.Verifier(nil, resource); err == nil {
		t.Fatal("nil verifier: want error")
	}
	if _, err := mcpkit.Verifier(fakeVerifier{}, ""); err == nil {
		t.Fatal("empty audience: want error")
	}
}

func TestVerifierHTTP(t *testing.T) {
	exp := time.Now().Add(time.Hour)
	v, err := mcpkit.Verifier(fakeVerifier{
		"good":      {Subject: "u1", Audience: []string{resource}, Scopes: []string{"tools.call"}, ExpiresAt: exp},
		"other-aud": {Subject: "u1", Audience: []string{"https://api.example.com"}, ExpiresAt: exp},
	}, resource)
	if err != nil {
		t.Fatal(err)
	}
	s := newServer(t, mcpkit.WithAuth(mcpkit.Auth{Verifier: v, Scopes: []string{"tools.call"}}))
	mcpkit.AddTool(s, &mcp.Tool{Name: "whoami"},
		func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
			p, ok := kauth.FromContext(ctx)
			if !ok {
				return nil, nil, errs.New(codes.Internal, "NO_PRINCIPAL")
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "sub=" + p.Subject}}}, nil, nil
		})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	cs, err := connectHTTP(t, ts.URL, "good")
	if err != nil {
		t.Fatal(err)
	}
	if got := text(call(t, cs, "whoami", map[string]any{})); got != "sub=u1" {
		t.Fatalf("got %q", got)
	}

	for token, want := range map[string]int{
		"":          http.StatusUnauthorized,
		"forged":    http.StatusUnauthorized,
		"other-aud": http.StatusUnauthorized,       // issued for another API
		"down":      http.StatusServiceUnavailable, // fail closed, errs UNAVAILABLE
	} {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("token %q: status %d, want %d", token, resp.StatusCode, want)
		}
		for _, leak := range []string{"jwks fetch failed", "TOKEN_INVALID", "audience"} {
			if strings.Contains(string(body), leak) {
				t.Errorf("token %q: body leaks %q: %s", token, leak, body)
			}
		}
		if token == "down" {
			if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
				t.Errorf("down: Content-Type %q, want errs JSON", ct)
			}
			if !strings.Contains(string(body), "AUTH_UNAVAILABLE") {
				t.Errorf("down: body %s, want reason AUTH_UNAVAILABLE", body)
			}
		}
	}
}
