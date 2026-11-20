package log_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/dungnh3/kitchen/log"
)

func ExampleNew() {
	logger := log.New(os.Stdout, log.Options{NoSource: true})
	logger = slog.New(dropTime{logger.Handler()}) // stable example output only

	ctx := log.WithAttrs(context.Background(), slog.String("x-request-id", "r1"))
	logger.ErrorContext(ctx, "charge failed", slog.String("order_id", "o1"), log.Err(errors.New("card declined")))
	// Output:
	// {"severity":"ERROR","message":"charge failed","order_id":"o1","error":"card declined","x-request-id":"r1"}
}

func ExampleOptionsFromEnv() {
	opts, err := log.OptionsFromEnv() // LOG_LEVEL, LOG_FORMAT, GOOGLE_CLOUD_PROJECT
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
