package server_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/i18n"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/server"
	"golang.org/x/text/language"
)

func Example() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// One logger for the process: zap-backed JSON, schema from LOG_SCHEMA or
	// the platform. Every kitchen package (and slog's default) uses it; request
	// id and user id are on every record logged with a request context.
	opts, err := log.OptionsFromEnv()
	logger, _ := log.Setup(opts)
	if err != nil {
		logger.WarnContext(ctx, "invalid log config", log.Err(err))
	}

	proxies, err := xcontext.NewTrustedProxies("10.0.0.0/8") // the load balancer range
	if err != nil {
		logger.ErrorContext(ctx, "config", log.Err(err))
		return
	}
	bundle, err := i18n.New(language.English, i18n.WithDefaults())
	if err != nil {
		logger.ErrorContext(ctx, "i18n", log.Err(err))
		return
	}
	srv, err := server.New(
		// One error format for gRPC, REST and plain HTTP, localized. The
		// server and the handler log through log.Default().
		server.WithErrors(errs.NewHandler(errs.Config{Localizer: bundle, Domain: "agents.example.com"})),
		// With module github.com/dungnh3/kitchen/auth (fail-closed on gRPC,
		// the REST gateway and plain HTTP routes; sets the user id):
		//   server.WithAuth(authn),
		// Rate limits after auth, per user or per client IP (package ratelimit):
		//   server.WithRateLimit(limiter),
		server.WithServices( /* your services */ ),
		server.WithAdmin(":9090", server.WithPprof()),
		server.WithTrustedProxies(proxies),
		server.WithShutdownDelay(5*time.Second),
		server.WithStreamingMarshalers(),
	)
	if err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
		return
	}
	if err := srv.Run(ctx); err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
	}
}

func ExampleNewAdmin() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// A worker binary without a gRPC server still serves probes.
	adm := server.NewAdmin(":8080", server.WithAdminChecks(server.Check{
		Name: "queue",
		Func: func(context.Context) error { return nil },
	}))
	go func() { _ = adm.Run(ctx) }()
	// ... run the worker until ctx is done ...
}

func ExampleServer_Close() {
	srv, err := server.New(server.WithLogger(log.Discard()), server.WithHTTPAddr(":0"), server.WithGRPCAddr(":0"))
	if err != nil {
		return
	}
	// main failed before Run (a dependency did not open, say): release the
	// server. A running server needs no Close; cancelling Run's ctx is enough.
	if err := srv.Close(); err != nil {
		log.Default().Error("close", log.Err(err))
	}
	fmt.Println(errors.Is(srv.Run(context.Background()), server.ErrClosed))
	// Output: true
}
