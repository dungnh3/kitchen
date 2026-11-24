package log

import (
	"context"
	"fmt"
	"io"
	stdlog "log"
	"log/slog"
	"os"
	"slices"
	"sync/atomic"
	"time"
)

// Logger is the logger every kitchen package takes and the type
// applications pass around. It is an alias of *slog.Logger's type, so a
// *log.Logger can be handed to third-party libraries that take a
// *slog.Logger (OpenTelemetry, pgx, gorm) without conversion.
type Logger = slog.Logger

// Aliases of the log/slog types packages need, so that no package outside
// kitchen/log imports log/slog.
type (
	// Attr is a key-value pair (slog.Attr).
	Attr = slog.Attr
	// Value is an attribute value (slog.Value).
	Value = slog.Value
	// Level is a log level (slog.Level); see LevelDebug ... LevelEmergency.
	Level = slog.Level
	// Leveler provides a Level (slog.Leveler).
	Leveler = slog.Leveler
	// LevelVar is a Level that can change at runtime (slog.LevelVar).
	LevelVar = slog.LevelVar
	// Handler handles records (slog.Handler), for adapters such as bridges.
	Handler = slog.Handler
	// Record is a log record (slog.Record), for Handler implementations.
	Record = slog.Record
)

// String returns an Attr for a string value.
func String(key, value string) Attr { return slog.String(key, value) }

// Int returns an Attr for an int value.
func Int(key string, value int) Attr { return slog.Int(key, value) }

// Int64 returns an Attr for an int64 value.
func Int64(key string, value int64) Attr { return slog.Int64(key, value) }

// Uint64 returns an Attr for a uint64 value.
func Uint64(key string, value uint64) Attr { return slog.Uint64(key, value) }

// Float64 returns an Attr for a float64 value.
func Float64(key string, value float64) Attr { return slog.Float64(key, value) }

// Bool returns an Attr for a bool value.
func Bool(key string, value bool) Attr { return slog.Bool(key, value) }

// Duration returns an Attr for a time.Duration value.
func Duration(key string, value time.Duration) Attr { return slog.Duration(key, value) }

// Time returns an Attr for a time.Time value.
func Time(key string, value time.Time) Attr { return slog.Time(key, value) }

// Any returns an Attr for any value (see slog.Any).
func Any(key string, value any) Attr { return slog.Any(key, value) }

// Group returns an attribute holding attrs under key.
func Group(key string, attrs ...Attr) Attr {
	return Attr{Key: key, Value: slog.GroupValue(attrs...)}
}

// The process logger. Its handler forwards to the handler currently
// installed (lazily the environment default, or the one given to
// Setup/SetDefault), so a *Logger taken from Default before main called
// Setup, and every logger derived from it with With or WithGroup, follows
// the installed one.
var (
	installed     atomic.Pointer[handlerBox]
	processLogger           = slog.New(&swapHandler{})
	stdout        io.Writer = os.Stdout // the process logger's output; a variable for tests
)

type handlerBox struct{ h slog.Handler }

// current returns the installed handler, building the environment default
// on first use.
func current() slog.Handler {
	if b := installed.Load(); b != nil {
		return b.h
	}
	// First use without Setup: build the environment default. Concurrent
	// first callers may each build one; only the first installed is kept.
	opts, err := OptionsFromEnv() // usable defaults even on error
	h := NewHandler(outputOf(opts), opts)
	if installed.CompareAndSwap(nil, &handlerBox{h}) && err != nil {
		slog.New(h).Warn("invalid logging environment, using defaults", Err(err))
	}
	return installed.Load().h
}

// swapHandler applies its With/WithGroup operations to the installed
// handler, caching the result until another handler is installed.
type swapHandler struct {
	ops   []swapOp
	cache atomic.Pointer[swapCache]
}

type swapOp struct {
	group string // "" for attrs
	attrs []slog.Attr
}

type swapCache struct{ base, h slog.Handler }

func (s *swapHandler) handler() slog.Handler {
	base := current()
	if c := s.cache.Load(); c != nil && c.base == base {
		return c.h
	}
	h := base
	for _, op := range s.ops {
		if op.group != "" {
			h = h.WithGroup(op.group)
		} else {
			h = h.WithAttrs(op.attrs)
		}
	}
	s.cache.Store(&swapCache{base: base, h: h})
	return h
}

func (s *swapHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return s.handler().Enabled(ctx, l)
}

func (s *swapHandler) Handle(ctx context.Context, r slog.Record) error {
	return s.handler().Handle(ctx, r)
}

func (s *swapHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return s
	}
	return &swapHandler{ops: append(slices.Clip(s.ops), swapOp{attrs: slices.Clone(attrs)})}
}

func (s *swapHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return s
	}
	return &swapHandler{ops: append(slices.Clip(s.ops), swapOp{group: name})}
}

// Default returns the process logger. Until main calls [Setup] or
// [SetDefault] it writes the kitchen format (from [OptionsFromEnv]) to
// stdout (stderr with LOG_OUTPUT=stderr), not log/slog's default output.
// Packages use it when their Logger option is nil.
// It always returns the same *Logger, which follows the installed handler,
// so a later Setup also reaches loggers taken from it earlier. It is safe
// for concurrent use.
func Default() *Logger { return processLogger }

// Setup builds the process logger from opts (typically from
// [OptionsFromEnv]), writing to opts.Output (stdout when nil), installs it
// with [SetDefault] and returns [Default]. Call it once, early in main;
// library packages should not call it.
//
//	opts, err := log.OptionsFromEnv()
//	logger, _ := log.Setup(opts)
//	if err != nil {
//		logger.Warn("invalid logging environment", log.Err(err))
//	}
func Setup(opts Options) (*Logger, error) {
	switch opts.Backend {
	case "", BackendZap, BackendSlog:
	default:
		return nil, fmt.Errorf("log: unknown backend %q", opts.Backend)
	}
	switch opts.Format {
	case "", FormatJSON, FormatText:
	default:
		return nil, fmt.Errorf("log: unknown format %q", opts.Format)
	}
	SetDefault(New(outputOf(opts), opts))
	return processLogger, nil
}

// SetDefault installs l's handler as the process logger: [Default] (and
// every logger taken from it) writes through it, and so do log/slog's
// default logger (third-party code logging through slog) and the standard
// library's log package (through slog.SetDefault). A nil l is ignored. Only
// main should call it.
func SetDefault(l *Logger) {
	if l == nil || l.Handler() == processLogger.Handler() {
		return
	}
	installed.Store(&handlerBox{l.Handler()})
	slog.SetDefault(processLogger)
}

// Discard returns a logger that writes nothing, for tests.
func Discard() *Logger { return slog.New(slog.DiscardHandler) }

// StdLogger returns a standard library *log.Logger that writes through l at
// level, for APIs that take one (http.Server.ErrorLog).
func StdLogger(l *Logger, level Level) *stdlog.Logger {
	if l == nil {
		l = Default()
	}
	return slog.NewLogLogger(l.Handler(), level)
}

// OrDefault returns l, or [Default] when l is nil. Packages use it for their
// Logger option.
func OrDefault(l *Logger) *Logger {
	if l != nil {
		return l
	}
	return Default()
}

// outputOf returns opts.Output, or the process stdout.
func outputOf(opts Options) io.Writer {
	if opts.Output != nil {
		return opts.Output
	}
	return stdout
}
