package example_test

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dungnh3/kitchen/auth"
	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/i18n"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/otelx/sdk"
	"github.com/dungnh3/kitchen/pg"
	"github.com/dungnh3/kitchen/ratelimit"
	rlredis "github.com/dungnh3/kitchen/ratelimit/redis"
	"github.com/dungnh3/kitchen/redisx"
	"github.com/dungnh3/kitchen/server"
	"golang.org/x/text/language"
)

// Example_service is the full wiring of a kitchen service: one process
// logger, telemetry, localized errors, auth, rate limits, dependency
// readiness checks and graceful shutdown on SIGTERM.
func Example_service() {
	if err := run(); err != nil {
		log.Default().Error("service stopped", log.Err(err))
		os.Exit(1)
	}
}

func run() error {
	// 1. Shutdown on SIGTERM (Kubernetes) or Ctrl-C.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. One process logger from LOG_* env vars. Every kitchen package logs
	// through it (log.Default) unless given another *log.Logger.
	opts, envErr := log.OptionsFromEnv()
	logger, err := log.Setup(opts)
	if err != nil {
		return err
	}
	if envErr != nil {
		logger.WarnContext(ctx, "invalid log config, using defaults", log.Err(envErr))
	}

	// 3. Telemetry (OTEL_* env vars). The server, httpx, pg and redisx use
	// the global providers it installs. Logs also go to OTLP.
	tel, err := sdk.Setup(ctx, sdk.Config{ServiceName: "agents", ServiceVersion: "1.0.0"})
	if err != nil {
		return err
	}
	defer func() { _ = tel.Shutdown(context.WithoutCancel(ctx)) }()
	opts.Handlers = append(opts.Handlers, tel.SlogHandler("agents"))
	if _, err := log.Setup(opts); err != nil {
		return err
	}

	// 4. Dependencies, opened with their canonical packages.
	db, err := pg.Open(ctx, pg.Config{URL: os.Getenv("DATABASE_URL")})
	if err != nil {
		return err
	}
	defer db.Close()
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{os.Getenv("REDIS_ADDR")}})
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	// 5. Localized errors: one AIP-193 format for gRPC, REST and plain HTTP.
	bundle, err := i18n.New(language.English, i18n.WithDefaults())
	if err != nil {
		return err
	}
	eh := errs.NewHandler(errs.Config{Localizer: bundle, Domain: "agents.example.com"})

	// 6. Authentication (fail-closed): tokens from the OIDC provider.
	jwks, err := auth.NewJWKS(ctx, auth.JWKSConfig{
		Issuer:   "https://login.example.com",
		Audience: "agents.example.com",
		URL:      "https://login.example.com/.well-known/jwks.json",
	})
	if err != nil {
		return err
	}
	defer func() { _ = jwks.Close(context.WithoutCancel(ctx)) }()
	authn, err := auth.New(auth.Config{Verifiers: []auth.Verifier{jwks}})
	if err != nil {
		return err
	}

	// 7. Rate limits shared by every instance (Redis GCRA), per user when
	// signed in, per client IP otherwise.
	store, err := rlredis.New(rdb)
	if err != nil {
		return err
	}
	limiter, err := ratelimit.New(ratelimit.Config{
		Store:     ratelimit.RemoteStore(store),
		Instances: 3, // replicas; each enforces 1/3 of a rule while Redis is down
		Default:   &ratelimit.Policy{Limits: ratelimit.DefaultLimits()},
	})
	if err != nil {
		return err
	}

	// 8. The client IP is only taken from X-Forwarded-For set by these.
	proxies, err := xcontext.NewTrustedProxies("10.0.0.0/8")
	if err != nil {
		return err
	}

	// 9. The server: gRPC on :10000, REST gateway and probes on :8080,
	// admin port :9090, readiness gated on Postgres and Redis.
	srv, err := server.New(
		server.WithServices( /* your services */ ),
		server.WithErrors(eh),
		server.WithAuth(authn),
		server.WithRateLimit(limiter),
		server.WithTrustedProxies(proxies),
		server.WithReadiness(
			server.Check{Name: "postgres", Func: db.Ping},
			server.Check{Name: "redis", Func: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
		),
		server.WithAdmin(":9090"),
		server.WithCompression(),
		server.WithShutdownDelay(5*time.Second),
	)
	if err != nil {
		return err
	}
	// Run returns after a graceful drain once ctx is cancelled.
	return srv.Run(ctx)
}
