package log_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/dungnh3/kitchen/log"
)

// worker is a library type: it takes a *log.Logger and falls back to the
// process logger.
type worker struct{ log *log.Logger }

type workerConfig struct {
	// Logger receives the worker's records. nil means log.Default().
	Logger *log.Logger
}

func newWorker(cfg workerConfig) *worker { return &worker{log: log.OrDefault(cfg.Logger)} }

func (w *worker) run(ctx context.Context, jobID string) {
	// Scope a value to everything logged with ctx.
	ctx = log.WithAttrs(ctx, log.String("job_id", jobID))
	w.log.InfoContext(ctx, "job started", log.Int("attempt", 1))
}

func Example_library() {
	// A package takes a *log.Logger in its Config (nil: log.Default()).
	logger := log.New(os.Stdout, log.Options{NoSource: true})
	logger = slog.New(dropTime{logger.Handler()}) // stable example output only

	w := newWorker(workerConfig{Logger: logger})
	w.run(context.Background(), "j-7")
	// Output:
	// {"message":"job started","level":"INFO","attempt":1,"job_id":"j-7"}
}

func ExampleWithAttrs() {
	logger := log.New(os.Stdout, log.Options{NoSource: true})
	logger = slog.New(dropTime{logger.Handler()})

	ctx := log.WithAttrs(context.Background(), log.String("tenant", "acme"))
	ctx = log.WithAttrs(ctx, log.String("tenant", "globex")) // same key: replaced
	logger.InfoContext(ctx, "invoice sent", log.Group("invoice", log.String("id", "inv-1"), log.Float64("total", 12.5)))
	// Output:
	// {"message":"invoice sent","level":"INFO","invoice":{"id":"inv-1","total":12.5},"tenant":"globex"}
}

func Example_stdioProgram() {
	// An MCP server over stdio or a CLI that prints results: stdout is the
	// data channel, so logs go to stderr. LOG_OUTPUT=stderr does the same
	// and also covers records logged before Setup.
	opts, err := log.OptionsFromEnv()
	if opts.Output == nil {
		opts.Output = os.Stderr
	}
	logger, _ := log.Setup(opts)
	if err != nil {
		logger.Warn("invalid logging environment", log.Err(err))
	}
}

func Example_extraSink() {
	// Handlers are extra sinks: every record also goes to them (for example
	// the OpenTelemetry log bridge, tel.SlogHandler("app") from otelx/sdk).
	// The kitchen JSON on Output stays first and unchanged.
	var audit bytes.Buffer
	sink := log.New(&audit, log.Options{Backend: log.BackendSlog, Format: log.FormatText}).Handler()

	opts, _ := log.OptionsFromEnv()
	opts.Handlers = append(opts.Handlers, sink)
	logger, _ := log.Setup(opts)
	logger.Info("service starting")
}

func ExampleStdLogger() {
	// For APIs that take a standard library *log.Logger.
	logger := log.New(os.Stdout, log.Options{NoSource: true})
	logger = slog.New(dropTime{logger.Handler()})

	srv := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          log.StdLogger(logger, log.LevelWarn),
	}
	srv.ErrorLog.Print("http: TLS handshake error")
	// Output:
	// {"message":"http: TLS handshake error","level":"WARN"}
}

func ExampleDiscard() {
	// Tests that do not look at logs.
	w := newWorker(workerConfig{Logger: log.Discard()})
	w.run(context.Background(), "j-1")
}

func Example_assertLogs() {
	// Tests that assert on logs: write JSON to a buffer.
	var buf bytes.Buffer
	w := newWorker(workerConfig{Logger: log.New(&buf, log.Options{NoSource: true})})
	w.run(context.Background(), "j-2")
	fmt.Println(strings.Contains(buf.String(), `"job_id":"j-2"`))
	// Output: true
}
