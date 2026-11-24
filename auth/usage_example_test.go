package auth_test

import (
	"context"
	"net/http"
	"os"

	"github.com/dungnh3/kitchen/auth"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/redisx"
	"github.com/dungnh3/kitchen/server"
)

// Tokens from an OIDC provider, verified with its published keys, on a
// kitchen server: WithAuth covers gRPC, REST and plain HTTP routes.
func ExampleNewJWKS() {
	ctx := context.Background()
	logger := log.Default() // main calls log.Setup once
	jwks, err := auth.NewJWKS(ctx, auth.JWKSConfig{
		Issuer:   "https://idp.example.com",
		Audience: "orders-api",
		URL:      "https://idp.example.com/.well-known/jwks.json",
		Types:    []string{"at+jwt"}, // reject ID tokens used as access tokens
	})
	if err != nil {
		logger.ErrorContext(ctx, "jwks", log.Err(err)) // fails if the first fetch fails
		return
	}
	defer func() { _ = jwks.Close(ctx) }()

	authn, err := auth.New(auth.Config{
		Verifiers: []auth.Verifier{jwks},
		Audience:  "api.example.com",
		Public:    []string{"/grpc.health.v1.Health/", "GET /healthz"},
		Logger:    logger,
	})
	if err != nil {
		logger.ErrorContext(ctx, "auth", log.Err(err))
		return
	}
	srv, err := server.New(server.WithAuth(authn), server.WithLogger(logger))
	if err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
		return
	}
	_ = srv // srv.Run(ctx)
}

// Opaque tokens checked against an RFC 7662 endpoint. Results are cached in
// process and, across the fleet, in Redis: entries are HMAC-signed with
// CacheKey, so whoever can write to Redis still cannot forge a principal.
func ExampleNewIntrospection() {
	ctx := context.Background()
	logger := log.Default()
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{"localhost:6379"}})
	if err != nil {
		logger.ErrorContext(ctx, "redis", log.Err(err))
		return
	}
	defer func() { _ = rdb.Close() }()

	in, err := auth.NewIntrospection(auth.IntrospectionConfig{
		Introspect: auth.RFC7662(auth.RFC7662Config{
			Endpoint:     "https://idp.example.com/oauth2/introspect",
			ClientID:     "orders-api",
			ClientSecret: os.Getenv("INTROSPECTION_CLIENT_SECRET"),
		}),
		Redis:    rdb,
		CacheKey: []byte(os.Getenv("INTROSPECTION_CACHE_KEY")), // ≥ 32 random bytes, from a secret
		Prefix:   "orders:introspection:idp:",
		Logger:   logger,
	})
	if err != nil {
		logger.ErrorContext(ctx, "introspection", log.Err(err))
		return
	}
	defer in.Close()

	authn, err := auth.New(auth.Config{Opaque: in, Audience: "orders-api", Logger: logger})
	if err != nil {
		logger.ErrorContext(ctx, "auth", log.Err(err))
		return
	}
	_ = authn // server.New(server.WithAuth(authn), ...)
}

// Handlers read the caller from the context. On Optional routes a request
// without a token runs anonymously; a bad token is still rejected.
func ExampleFromContext() {
	cfg := auth.Config{
		Audience: "api.example.com",
		Public:   []string{"GET /healthz"},
		Optional: []string{"GET /catalog"}, // personalised when signed in
	}
	_ = cfg

	catalog := func(w http.ResponseWriter, r *http.Request) {
		if p, ok := auth.FromContext(r.Context()); ok {
			_, _ = w.Write([]byte("hello " + p.Subject))
			return
		}
		_, _ = w.Write([]byte("hello guest"))
	}
	_ = catalog
}
