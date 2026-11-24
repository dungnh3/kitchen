package mcpkit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

// Auth configures bearer-token authentication for the HTTP handler.
//
// Every request must carry "Authorization: Bearer <token>". A missing or
// rejected token gets 401 (a missing scope 403). When [Auth.Metadata] is
// set, the WWW-Authenticate header points at the protected-resource
// metadata, so spec-compliant clients can start the OAuth flow by
// themselves. Leave [WithAuth] out to serve without auth.
type Auth struct {
	// Verifier checks the token. Use [Verifier] to adapt a kitchen
	// auth.Verifier for tokens issued by your own IdP, or [PassThrough] when
	// the token belongs to an upstream API (Slack, Google, Atlassian, ...)
	// that the tool forwards it to. Required.
	Verifier auth.TokenVerifier

	// Metadata is served by [Server.MetadataHandler] (RFC 9728). Its
	// Resource field must be the absolute URL of the MCP endpoint, for
	// example "https://mcp.example.com/mcp". Optional, but without it
	// clients cannot discover the authorization server.
	Metadata *oauthex.ProtectedResourceMetadata

	// Scopes, if set, must all be present in the verified token (403
	// otherwise). They are also advertised in the WWW-Authenticate
	// challenge.
	Scopes []string

	// AllowMissingExpiration accepts tokens whose TokenInfo has no
	// expiration. It must be true with [PassThrough], which cannot know an
	// opaque upstream token's lifetime. Keep it false for JWTs.
	AllowMissingExpiration bool

	// ClockSkew tolerates small clock differences when checking expiry.
	ClockSkew time.Duration
}

func (a *Auth) validate() error {
	if a.Verifier == nil {
		return errors.New("mcpkit: Auth.Verifier is required")
	}
	if a.Metadata != nil {
		if _, err := metadataPath(a.Metadata.Resource); err != nil {
			return err
		}
	}
	return nil
}

// metadataPath returns the RFC 9728 well-known path for an absolute resource
// URL: "/.well-known/oauth-protected-resource" followed by the resource path.
func metadataPath(resource string) (string, error) {
	u, err := url.Parse(resource)
	if err != nil || !u.IsAbs() || u.Host == "" {
		return "", fmt.Errorf("mcpkit: Auth.Metadata.Resource must be an absolute URL, got %q", resource)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("mcpkit: Auth.Metadata.Resource must not have a query or fragment, got %q", resource)
	}
	return "/.well-known/oauth-protected-resource" + strings.TrimSuffix(u.EscapedPath(), "/"), nil
}

// PassThrough returns a verifier that accepts any non-empty bearer token
// without checking it. Use it only when the token is an upstream API's
// credential that the tool forwards, so the upstream rejects bad tokens.
// Combine it with AllowMissingExpiration: true.
//
// The raw token is available in tool handlers via [TokenFrom].
func PassThrough() auth.TokenVerifier {
	return func(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		if token == "" {
			return nil, auth.ErrInvalidToken
		}
		return &auth.TokenInfo{}, nil
	}
}

type rawTokenKey struct{}

// withToken stores the raw bearer token in ctx.
func withToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, rawTokenKey{}, token)
}

// TokenFrom returns the raw bearer token of the current request: the HTTP
// Authorization header on [Server.Handler], or the [TokenSource] value on
// [Server.Run]/[Server.RunStdio]. It returns "" when there is none.
func TokenFrom(ctx context.Context) string {
	s, _ := ctx.Value(rawTokenKey{}).(string)
	return s
}

// TokenInfoFrom returns the verified token information set by the HTTP
// handler's verifier, or nil (always nil on stdio).
func TokenInfoFrom(ctx context.Context) *auth.TokenInfo {
	return auth.TokenInfoFromContext(ctx)
}

// TokenSource supplies the upstream credential for stdio serving, where
// there is no Authorization header.
type TokenSource func(ctx context.Context) (string, error)

// EnvToken returns a TokenSource that reads the environment variable name.
// It fails if the variable is unset or empty.
func EnvToken(name string) TokenSource {
	return func(context.Context) (string, error) {
		v, ok := os.LookupEnv(name)
		if !ok || v == "" {
			return "", fmt.Errorf("mcpkit: environment variable %s is not set", name)
		}
		return v, nil
	}
}

// bearerToken extracts the token from an Authorization header value.
func bearerToken(h string) string {
	scheme, tok, ok := strings.Cut(strings.TrimSpace(h), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(tok)
}
