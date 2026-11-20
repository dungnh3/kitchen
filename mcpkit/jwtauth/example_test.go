package jwtauth_test

import (
	"log"

	"github.com/dungnh3/kitchen/mcpkit"
	"github.com/dungnh3/kitchen/mcpkit/jwtauth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

func ExampleNew() {
	v, err := jwtauth.New(jwtauth.Config{
		Issuer:   "https://idp.example.com",
		Audience: "https://mcp.example.com/mcp", // this server's resource URL
		// JWKSURL is discovered from the issuer metadata when empty.
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = mcpkit.New(&mcp.Implementation{Name: "agent-tools"},
		mcpkit.WithAuth(mcpkit.Auth{
			Verifier: v.Verify,
			Scopes:   []string{"tools.call"},
			Metadata: &oauthex.ProtectedResourceMetadata{
				Resource:             "https://mcp.example.com/mcp",
				AuthorizationServers: []string{"https://idp.example.com"},
				ScopesSupported:      []string{"tools.call"},
			},
		}))
	if err != nil {
		log.Fatal(err)
	}
}
