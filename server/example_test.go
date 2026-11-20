package server_test

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dungnh3/kitchen/reqctx"
	"github.com/dungnh3/kitchen/server"
)

func Example() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	proxies, err := reqctx.NewTrustedProxies("10.0.0.0/8") // the load balancer range
	if err != nil {
		slog.Error("config", "error", err)
		return
	}
	srv, err := server.New(
		server.WithServices( /* your services */ ),
		server.WithAdmin(":9090", server.WithPprof()),
		server.WithTrustedProxies(proxies),
		server.WithShutdownDelay(5*time.Second),
		server.WithStreamingMarshalers(),
	)
	if err != nil {
		slog.Error("server", "error", err)
		return
	}
	if err := srv.Run(ctx); err != nil {
		slog.Error("server", "error", err)
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
