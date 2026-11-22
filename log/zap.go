package log

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Backend selects the engine that encodes and writes records. Either way
// the API is *slog.Logger, so libraries do not depend on the choice.
type Backend string

// Backends.
const (
	// BackendZap encodes with go.uber.org/zap's zapcore. It is the default.
	BackendZap Backend = "zap"
	// BackendSlog encodes with the standard library's slog handlers.
	BackendSlog Backend = "slog"
)

// newZapHandler returns a slog.Handler that writes through a zapcore.Core
// with the schema's keys. It keeps the exact slog level (the schema renders
// it), unlike go.uber.org/zap/exp/zapslog, which folds every level above
// ERROR into ERROR.
func newZapHandler(w io.Writer, opts Options, schema Schema) slog.Handler {
	level := opts.Level
	if level == nil {
		level = LevelInfo
	}
	console := opts.Format == FormatText
	return &zapHandler{
		core:      zapcore.NewCore(zapEncoder(schema, console), zapcore.Lock(zapcore.AddSync(w)), zapcore.DebugLevel),
		level:     level,
		schema:    schema,
		console:   console,
		addSource: !opts.NoSource && (console || schema.SourceKey != ""),
		replace:   opts.ReplaceAttr,
	}
}

// zapEncoder builds the JSON (or, for development, console) encoder. In JSON
// mode the level and the source are added as fields by zapHandler, so that
// they use the schema's names and formats.
func zapEncoder(s Schema, console bool) zapcore.Encoder {
	cfg := zapcore.EncoderConfig{
		TimeKey:        s.TimeKey,
		MessageKey:     s.MessageKey,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeTime:     utcRFC3339Nano,
		EncodeDuration: zapcore.NanosDurationEncoder, // as slog's JSON handler
		EncodeLevel:    zapcore.CapitalLevelEncoder,
	}
	if console {
		cfg.TimeKey, cfg.LevelKey, cfg.MessageKey = "time", "level", "msg"
		cfg.EncodeTime = zapcore.ISO8601TimeEncoder
		cfg.EncodeDuration = zapcore.StringDurationEncoder
		cfg.ConsoleSeparator = " "
		return zapcore.NewConsoleEncoder(cfg)
	}
	return zapcore.NewJSONEncoder(cfg)
}

func utcRFC3339Nano(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.UTC().Format(time.RFC3339Nano))
}

// zapLevel maps a slog level to the nearest zap level, used only for zap's
// own behaviour (the console level name, syncing); the written level comes
// from the schema.
func zapLevel(l slog.Level) zapcore.Level {
	switch {
	case l < LevelInfo:
		return zapcore.DebugLevel
	case l < LevelWarn:
		return zapcore.InfoLevel
	case l < LevelError:
		return zapcore.WarnLevel
	default:
		return zapcore.ErrorLevel
	}
}

type zapHandler struct {
	core      zapcore.Core
	level     slog.Leveler
	schema    Schema
	console   bool
	addSource bool
	replace   func([]string, slog.Attr) slog.Attr
	groups    []string // only for direct use; NewHandler buffers groups itself
}

func (h *zapHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level.Level()
}

func (h *zapHandler) Handle(_ context.Context, r slog.Record) error {
	ce := h.core.Check(zapcore.Entry{Level: zapLevel(r.Level), Time: r.Time, Message: r.Message}, nil)
	if ce == nil {
		return nil
	}
	fields := make([]zapcore.Field, 0, r.NumAttrs()+2)
	if !h.console {
		fields = append(fields, zap.String(h.schema.LevelKey, h.schema.Level(r.Level)))
	}
	if h.addSource {
		if src := r.Source(); src != nil && src.File != "" {
			if h.console {
				fields = append(fields, zap.String(slog.SourceKey, fileLine(src).String()))
			} else if f, ok := h.field(nil, slog.Attr{Key: h.schema.SourceKey, Value: h.schema.Source(src)}, false); ok {
				fields = append(fields, f)
			}
		}
	}
	r.Attrs(func(a slog.Attr) bool {
		if f, ok := h.field(h.groups, a, true); ok {
			fields = append(fields, f)
		}
		return true
	})
	ce.Write(fields...)
	return nil
}

func (h *zapHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	fields := make([]zapcore.Field, 0, len(attrs))
	for _, a := range attrs {
		if f, ok := h.field(h.groups, a, true); ok {
			fields = append(fields, f)
		}
	}
	c := *h
	c.core = h.core.With(fields)
	return &c
}

func (h *zapHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := *h
	c.core = h.core.With([]zapcore.Field{zap.Namespace(name)})
	c.groups = append(slices.Clip(h.groups), name)
	return &c
}

// field converts an attribute the way slog's JSON handler would: empty
// attributes and empty groups are dropped, groups with an empty key are
// inlined, errors are written as their message. user selects whether the
// ReplaceAttr option applies.
func (h *zapHandler) field(groups []string, a slog.Attr, user bool) (zapcore.Field, bool) {
	a.Value = a.Value.Resolve()
	if user && h.replace != nil && a.Value.Kind() != slog.KindGroup {
		a = h.replace(groups, a)
		a.Value = a.Value.Resolve()
	}
	if a.Equal(slog.Attr{}) {
		return zapcore.Field{}, false
	}
	switch a.Value.Kind() {
	case slog.KindString:
		return zap.String(a.Key, a.Value.String()), true
	case slog.KindInt64:
		return zap.Int64(a.Key, a.Value.Int64()), true
	case slog.KindUint64:
		return zap.Uint64(a.Key, a.Value.Uint64()), true
	case slog.KindFloat64:
		return zap.Float64(a.Key, a.Value.Float64()), true
	case slog.KindBool:
		return zap.Bool(a.Key, a.Value.Bool()), true
	case slog.KindDuration:
		return zap.Duration(a.Key, a.Value.Duration()), true
	case slog.KindTime:
		return zap.Time(a.Key, a.Value.Time()), true
	case slog.KindGroup:
		attrs := a.Value.Group()
		if len(attrs) == 0 {
			return zapcore.Field{}, false
		}
		inner := groups
		if a.Key != "" {
			inner = append(slices.Clip(groups), a.Key)
		}
		obj := zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
			for _, ga := range attrs {
				if f, ok := h.field(inner, ga, user); ok {
					f.AddTo(enc)
				}
			}
			return nil
		})
		if a.Key == "" {
			return zap.Inline(obj), true
		}
		return zap.Object(a.Key, obj), true
	}
	if err, ok := a.Value.Any().(error); ok {
		return zap.String(a.Key, err.Error()), true
	}
	return zap.Any(a.Key, a.Value.Any()), true
}

// NewZap returns a *zap.Logger that writes the same schema as New with
// BackendZap, for code that prefers zap's API. zap has no context
// parameter, so trace correlation and Options.ContextAttrs do not apply:
// add those fields with With. Prefer New for libraries.
func NewZap(w io.Writer, opts Options) *zap.Logger {
	schema := opts.Schema.withDefaults()
	level := opts.Level
	if level == nil {
		level = LevelInfo
	}
	cfg := zapcore.EncoderConfig{
		TimeKey:        schema.TimeKey,
		LevelKey:       schema.LevelKey,
		MessageKey:     schema.MessageKey,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeTime:     utcRFC3339Nano,
		EncodeDuration: zapcore.NanosDurationEncoder,
		EncodeLevel: func(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(schema.Level(slogLevel(l)))
		},
	}
	var zopts []zap.Option
	if !opts.NoSource && schema.SourceKey != "" {
		cfg.CallerKey = schema.SourceKey
		cfg.EncodeCaller = zapcore.ShortCallerEncoder
		zopts = append(zopts, zap.AddCaller())
	}
	enabled := zap.LevelEnablerFunc(func(l zapcore.Level) bool { return slogLevel(l) >= level.Level() })
	core := zapcore.NewCore(zapcore.NewJSONEncoder(cfg), zapcore.Lock(zapcore.AddSync(w)), enabled)
	return zap.New(core, zopts...)
}

// slogLevel maps a zap level to slog's scale.
func slogLevel(l zapcore.Level) slog.Level {
	switch {
	case l <= zapcore.DebugLevel:
		return LevelDebug
	case l == zapcore.InfoLevel:
		return LevelInfo
	case l == zapcore.WarnLevel:
		return LevelWarn
	case l == zapcore.ErrorLevel:
		return LevelError
	default: // DPanic, Panic, Fatal
		return LevelCritical
	}
}
