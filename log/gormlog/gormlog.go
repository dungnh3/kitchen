// Package gormlog implements gorm's logger.Interface on top of kitchen/log
// (*log.Logger).
//
// Records carry the context passed to gorm (so trace ids and context
// attributes are added by the slog handler) and the source location of the
// first caller outside gorm. SQL is logged with placeholders unless
// Config.LogParams is set, so query parameter values (often personal data or
// secrets) are not written into the logged SQL by default. Driver error
// messages may still contain values.
//
// For tracing: a gorm opened with github.com/dungnh3/kitchen/pg/gorm on a
// pg.Open pool is already traced by the pool's otelpgx tracer (and uses this
// logger by default); for other drivers use gorm.io/plugin/opentelemetry.
package gormlog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"time"

	klog "github.com/dungnh3/kitchen/log"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Config configures [New]. The zero value logs errors and slow queries
// (>200ms) to log.Default() (package kitchen/log) without parameter values.
type Config struct {
	// Logger receives the records. nil means log.Default() of package
	// kitchen/log, the process logger.
	Logger *klog.Logger
	// Level is the gorm log level. Zero means logger.Warn (errors and slow
	// queries); logger.Info logs every query at INFO.
	Level logger.LogLevel
	// SlowThreshold marks queries slower than this as slow (WARN). Zero means
	// 200ms; a negative value disables slow-query logging.
	SlowThreshold time.Duration
	// IgnoreRecordNotFound skips gorm.ErrRecordNotFound errors.
	IgnoreRecordNotFound bool
	// LogParams writes SQL with interpolated parameter values. Off by
	// default for safety.
	LogParams bool
}

// Attribute keys.
const (
	KeySQL       = "db.query.text"
	KeyRows      = "db.response.returned_rows"
	KeyElapsed   = "elapsed"
	KeyThreshold = "slow_threshold"
)

// Logger implements logger.Interface and gorm.ParamsFilter.
type Logger struct{ cfg Config }

var (
	_ logger.Interface  = (*Logger)(nil)
	_ gorm.ParamsFilter = (*Logger)(nil)
)

// New returns a gorm logger.
func New(cfg Config) *Logger {
	if cfg.Logger == nil {
		cfg.Logger = klog.Default()
	}
	if cfg.Level == 0 {
		cfg.Level = logger.Warn
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = 200 * time.Millisecond
	}
	return &Logger{cfg: cfg}
}

// LogMode returns a copy with the given level.
func (l *Logger) LogMode(level logger.LogLevel) logger.Interface {
	c := *l
	c.cfg.Level = level
	return &c
}

// Info logs a formatted message at INFO when the level is logger.Info.
func (l *Logger) Info(ctx context.Context, msg string, args ...any) {
	if l.cfg.Level >= logger.Info {
		l.log(ctx, slog.LevelInfo, fmt.Sprintf(msg, args...))
	}
}

// Warn logs a formatted message at WARN when the level is at least logger.Warn.
func (l *Logger) Warn(ctx context.Context, msg string, args ...any) {
	if l.cfg.Level >= logger.Warn {
		l.log(ctx, slog.LevelWarn, fmt.Sprintf(msg, args...))
	}
}

// Error logs a formatted message at ERROR when the level is at least
// logger.Error.
func (l *Logger) Error(ctx context.Context, msg string, args ...any) {
	if l.cfg.Level >= logger.Error {
		l.log(ctx, slog.LevelError, fmt.Sprintf(msg, args...))
	}
}

// Trace logs a finished query: failures at ERROR, slow queries at WARN, and
// every query at INFO when the level is logger.Info.
func (l *Logger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	if l.cfg.Level <= logger.Silent {
		return
	}
	elapsed := time.Since(begin)
	var (
		level slog.Level
		msg   string
		extra []slog.Attr
	)
	switch {
	case err != nil && l.cfg.Level >= logger.Error &&
		(!l.cfg.IgnoreRecordNotFound || !errors.Is(err, gorm.ErrRecordNotFound)):
		level, msg = slog.LevelError, "gorm query failed"
		extra = []slog.Attr{slog.String("error", err.Error())}
	case l.cfg.SlowThreshold > 0 && elapsed > l.cfg.SlowThreshold && l.cfg.Level >= logger.Warn:
		level, msg = slog.LevelWarn, "gorm slow query"
		extra = []slog.Attr{slog.Duration(KeyThreshold, l.cfg.SlowThreshold)}
	case l.cfg.Level >= logger.Info:
		level, msg = slog.LevelInfo, "gorm query"
	default:
		return
	}
	if !l.cfg.Logger.Enabled(ctx, level) {
		return
	}
	sql, rows := fc()
	attrs := append([]slog.Attr{
		slog.String(KeySQL, sql),
		slog.Duration(KeyElapsed, elapsed),
	}, extra...)
	if rows >= 0 {
		attrs = append(attrs, slog.Int64(KeyRows, rows))
	}
	l.log(ctx, level, msg, attrs...)
}

// ParamsFilter drops parameter values unless Config.LogParams is set; gorm
// then logs the SQL with placeholders.
func (l *Logger) ParamsFilter(_ context.Context, sql string, params ...any) (string, []any) {
	if l.cfg.LogParams {
		return sql, params
	}
	return sql, nil
}

func (l *Logger) log(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) { //nolint:contextcheck // Background only when gorm passes a nil ctx.
	if ctx == nil {
		ctx = context.Background()
	}
	h := l.cfg.Logger.Handler()
	if !h.Enabled(ctx, level) {
		return
	}
	r := slog.NewRecord(time.Now(), level, msg, callerPC())
	r.AddAttrs(attrs...)
	_ = h.Handle(ctx, r)
}

// callerPC returns the program counter of the first frame outside this
// package and outside gorm.io modules (gorm, drivers, plugins), i.e. the
// application code that issued the query.
func callerPC() uintptr {
	var pcs [64]uintptr
	n := runtime.Callers(3, pcs[:]) // skip runtime.Callers, callerPC, log
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if !strings.HasPrefix(f.Function, "gorm.io/") &&
			!strings.HasPrefix(f.Function, "github.com/dungnh3/kitchen/log/gormlog.") {
			return f.PC
		}
		if !more {
			return 0
		}
	}
}
