package auth_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/dungnh3/kitchen/auth"
	"github.com/dungnh3/kitchen/errs"
	"github.com/lestrrat-go/jwx/v3/jwa"
)

func Example() {
	ctx := context.Background()

	// HMAC-signed tokens from one issuer. For an OIDC provider use
	// auth.NewJWKS(ctx, auth.JWKSConfig{Issuer: ..., URL: ".../jwks.json"}) and Close it on shutdown.
	static, err := auth.NewStatic(auth.StaticConfig{
		Issuer:   "https://issuer-a.example.com",
		Audience: "api.example.com",
		Keys:     []auth.Key{{ID: "2026-11", Alg: jwa.HS256(), Material: []byte("0123456789abcdef0123456789abcdef")}},
	})
	if err != nil {
		panic(err)
	}
	authn, err := auth.New(auth.Config{
		Verifiers: []auth.Verifier{static},
		Public:    []string{"/grpc.health.v1.Health/"},
		Authorizer: auth.RequireScopes(map[string][]string{
			"/agent.v1.AgentService/Delete": {"agents:admin"},
		}),
	})
	if err != nil {
		panic(err)
	}
	// grpc.NewServer(grpc.ChainUnaryInterceptor(errs.UnaryServerInterceptor(...), authn.UnaryServerInterceptor()))

	// An unsigned ("alg":"none") token is rejected, never passed through.
	_, err = authn.Authenticate(ctx, "eyJhbGciOiJub25lIn0.eyJpc3MiOiJodHRwczovL2lzc3Vlci1hLmV4YW1wbGUuY29tIn0.")
	fmt.Println(errors.Is(err, auth.ErrTokenInvalid), errs.Code(err), errs.HTTPStatus(err))
	// Output: true Unauthenticated 401
}
