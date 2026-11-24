package log_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.uber.org/zap"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/log"
)

func ExampleNew() {
	logger := log.New(os.Stdout, log.Options{NoSource: true, NoStackTrace: true})
	logger = slog.New(dropTime{logger.Handler()}) // stable example output only

	ctx := log.WithAttrs(context.Background(), slog.String("x-request-id", "r1"))
	logger.ErrorContext(ctx, "charge failed", slog.String("order_id", "o1"), log.Err(errors.New("card declined")))
	// Output:
	// {"message":"charge failed","level":"ERROR","order_id":"o1","error":"card declined","x-request-id":"r1"}
}

func ExampleSchema() {
	// Pick the platform schema explicitly (or let OptionsFromEnv detect it).
	logger := log.New(os.Stdout, log.Options{Schema: log.SchemaGCP, NoSource: true})
	logger = slog.New(dropTime{logger.Handler()})
	logger.Warn("quota low", log.String("message", "user text cannot spoof the message"))
	// Output:
	// {"message":"quota low","severity":"WARNING","attr.message":"user text cannot spoof the message"}
}

func Example_requestContext() {
	// Package kitchen/context stores the request id and the user id as log
	// attributes: every kitchen logger writes them on records logged with
	// the request context, with no logger configuration.
	logger := log.New(os.Stdout, log.Options{NoSource: true})
	logger = slog.New(dropTime{logger.Handler()})

	// A server stores this once per request; auth sets the user id.
	ctx := xcontext.NewContext(context.Background(), xcontext.Info{RequestID: "r-1"})
	ctx = xcontext.WithUserID(ctx, "u-42")
	logger.InfoContext(ctx, "order placed", log.String("order_id", "o1"))
	// Output:
	// {"message":"order placed","level":"INFO","order_id":"o1","request_id":"r-1","user_id":"u-42"}
}

func ExampleSetup() {
	// In main: one logger for the process. log.Default(), every kitchen
	// package (their Logger options default to it), log/slog's default and
	// the standard library's log package all write through it.
	opts, err := log.OptionsFromEnv()
	logger, _ := log.Setup(opts)
	if err != nil {
		logger.Warn("invalid logging environment", log.Err(err))
	}
	logger.Info("service starting", log.String("version", "v1.2.3"))
}

func ExampleBackend() {
	// zap is the default engine; pick slog's own handlers explicitly.
	logger := log.New(os.Stdout, log.Options{Backend: log.BackendSlog, NoSource: true})
	logger = slog.New(dropTime{logger.Handler()})
	logger.Info("ready", slog.Int("port", 8080))
	// Output:
	// {"level":"INFO","message":"ready","port":8080}
}

func ExampleNewZap() {
	// For code that prefers zap's API. Libraries should still get a *slog.Logger.
	z := log.NewZap(os.Stdout, log.Options{NoSource: true})
	defer func() { _ = z.Sync() }()
	z.Info("ready", zap.Int("port", 8080))
}

func ExampleOptionsFromEnv() {
	opts, err := log.OptionsFromEnv() // LOG_LEVEL, LOG_FORMAT, LOG_SCHEMA (else detected), LOG_BACKEND, LOG_OUTPUT
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid log config:", err)
	}
	logger := log.New(os.Stdout, opts)
	slog.SetDefault(logger) // the application, not a library, decides this

	// Change the level at runtime.
	opts.Level.(*slog.LevelVar).Set(log.LevelDebug)
}

// dropTime zeroes the record time; slog then omits the time key.
type dropTime struct{ slog.Handler }

func (h dropTime) Handle(ctx context.Context, r slog.Record) error {
	r.Time = time.Time{}
	return h.Handler.Handle(ctx, r)
}

func (h dropTime) WithAttrs(as []slog.Attr) slog.Handler { return dropTime{h.Handler.WithAttrs(as)} }
func (h dropTime) WithGroup(n string) slog.Handler       { return dropTime{h.Handler.WithGroup(n)} }
