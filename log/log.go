package log

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Format selects the record encoding.
type Format string

// Formats.
const (
	// FormatJSON writes one JSON object per line with the keys of
	// Options.Schema. It is the default.
	FormatJSON Format = "json"
	// FormatText writes human-readable text, for local development. Keys
	// keep slog's names (time, level, msg, source). Levels use the schema's
	// level names with BackendSlog and zap's level names with BackendZap.
	FormatText Format = "text"
)

// Options configures [New] and [NewHandler]. The zero value logs
// SchemaOTel JSON at INFO with source locations, encoded by zap.
type Options struct {
	// Backend is BackendZap (default, also used for "") or BackendSlog. Both
	// produce the same JSON for a schema (key order aside). Behind the slog
	// API they cost about the same; zap allocates more because attributes
	// are converted to zap fields. zap's own API (NewZap) is about 3x faster
	// but has no context, so no trace correlation.
	Backend Backend
	// Level is the minimum level. nil means INFO. Pass a *slog.LevelVar to
	// change the level at runtime.
	Level slog.Leveler
	// Format is FormatJSON (default, also used for "") or FormatText.
	Format Format
	// Schema selects the JSON keys and trace correlation (SchemaOTel,
	// SchemaGCP, SchemaAWS or a custom one). The zero value is SchemaOTel.
	Schema Schema
	// NoSource disables the source location.
	NoSource bool
	// ContextAttrs returns request-scoped attributes for a context, for
	// example the request id and user id of package kitchen/context. It is
	// called for every record logged with a context and must be cheap and
	// safe for concurrent use. Nil adds nothing.
	ContextAttrs func(context.Context) []slog.Attr
	// StackTraceLevel is the lowest level whose records carry a stack trace
	// (under Schema.StackKey). nil means ERROR.
	StackTraceLevel slog.Leveler
	// NoStackTrace disables stack traces.
	NoStackTrace bool
	// ReplaceAttr rewrites user attributes. It is not called for the
	// built-in time, level, message and source attributes, whose keys come
	// from Schema.
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
	// Handlers are extra sinks every record is also sent to, after the
	// kitchen handler, for example the OpenTelemetry log bridge of
	// otelx/sdk (Telemetry.SlogHandler). They get records at or above Level
	// with the context attributes (request_id, user_id, WithAttrs and
	// ContextAttrs) added; the schema, trace keys and stack traces are the
	// kitchen handler's, so a sink writes its own format. The kitchen
	// handler always stays first: Handlers add outputs, they never replace
	// the schema.
	Handlers []Handler
	// Output is where Setup and the environment default of Default write.
	// nil means stdout. Programs whose stdout is a data channel (an MCP
	// server over stdio, a CLI printing results) set os.Stderr, or set
	// LOG_OUTPUT=stderr so that also logs written before Setup go there.
	// New and NewHandler write to their own w argument.
	Output io.Writer
}

// New returns a logger that writes to w. See [NewHandler].
func New(w io.Writer, opts Options) *slog.Logger {
	return slog.New(NewHandler(w, opts))
}

// NewHandler returns a handler that writes records to w in the configured
// format and schema, and adds trace correlation and context attributes (see
// [NewContextHandler]).
func NewHandler(w io.Writer, opts Options) slog.Handler {
	h := newHandler(w, opts)
	if len(opts.Handlers) == 0 {
		return h
	}
	level := opts.Level
	if level == nil {
		level = LevelInfo
	}
	hs := make([]slog.Handler, 0, 1+len(opts.Handlers))
	hs = append(hs, h)
	for _, x := range opts.Handlers {
		if x != nil {
			hs = append(hs, &sinkHandler{next: x, level: level, extract: opts.ContextAttrs})
		}
	}
	return slog.NewMultiHandler(hs...)
}

func newHandler(w io.Writer, opts Options) slog.Handler {
	schema := opts.Schema.withDefaults()
	level := opts.Level
	if level == nil {
		level = LevelInfo
	}
	if opts.Backend != BackendSlog {
		return NewContextHandler(newZapHandler(w, opts, schema), opts)
	}
	ho := &slog.HandlerOptions{
		AddSource: !opts.NoSource && schema.SourceKey != "",
		Level:     level,
	}
	var h slog.Handler
	if opts.Format == FormatText {
		ho.ReplaceAttr = textReplacer(schema, opts.ReplaceAttr)
		h = slog.NewTextHandler(w, ho)
	} else {
		ho.ReplaceAttr = jsonReplacer(schema, opts.ReplaceAttr)
		h = slog.NewJSONHandler(w, ho)
	}
	return NewContextHandler(h, opts)
}

// OptionsFromEnv builds Options from the environment:
//
//	LOG_LEVEL   debug|info|notice|warn|error|critical|alert|emergency (default info)
//	LOG_FORMAT  json|text (default json)
//	LOG_SCHEMA  otel|gcp|aws (default: detected, see DetectSchema)
//	LOG_BACKEND zap|slog (default zap)
//	LOG_OUTPUT  stdout|stderr (default stdout; Options.Output)
//
// Level is a *slog.LevelVar, so callers can change it later. An invalid value
// is an error; the returned Options are still usable with the defaults.
func OptionsFromEnv() (Options, error) {
	lv := new(slog.LevelVar)
	opts := Options{Level: lv, Schema: DetectSchema()}
	var errs []error
	if s := os.Getenv("LOG_LEVEL"); s != "" {
		l, err := ParseLevel(s)
		if err != nil {
			errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
		} else {
			lv.Set(l)
		}
	}
	switch f := Format(strings.ToLower(os.Getenv("LOG_FORMAT"))); f {
	case "", FormatJSON:
		opts.Format = FormatJSON
	case FormatText:
		opts.Format = FormatText
	default:
		opts.Format = FormatJSON
		errs = append(errs, fmt.Errorf("LOG_FORMAT: invalid format %q", f))
	}
	if name := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_SCHEMA"))); name != "" {
		if sc, ok := SchemaByName(name); ok {
			opts.Schema = sc
		} else {
			errs = append(errs, fmt.Errorf("LOG_SCHEMA: invalid schema %q", name))
		}
	}
	switch b := Backend(strings.ToLower(strings.TrimSpace(os.Getenv("LOG_BACKEND")))); b {
	case "", BackendZap:
		opts.Backend = BackendZap
	case BackendSlog:
		opts.Backend = BackendSlog
	default:
		opts.Backend = BackendZap
		errs = append(errs, fmt.Errorf("LOG_BACKEND: invalid backend %q", b))
	}
	switch o := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_OUTPUT"))); o {
	case "", "stdout":
	case "stderr":
		opts.Output = os.Stderr
	default:
		errs = append(errs, fmt.Errorf("LOG_OUTPUT: invalid output %q (stdout or stderr)", o))
	}
	if len(errs) > 0 {
		return opts, errors.Join(errs...)
	}
	return opts, nil
}

// DetectSchema picks a built-in schema from environment variables that the
// platforms set: Cloud Run (K_SERVICE), Cloud Functions (FUNCTION_TARGET), App
// Engine (GAE_SERVICE) or GOOGLE_CLOUD_PROJECT select SchemaGCP; Lambda
// (AWS_LAMBDA_FUNCTION_NAME), ECS (ECS_CONTAINER_METADATA_URI_V4) or
// AWS_EXECUTION_ENV select SchemaAWS. Otherwise, and on GKE or EKS where
// none of these is set, it returns SchemaOTel: set LOG_SCHEMA there.
func DetectSchema() Schema {
	has := func(keys ...string) bool {
		for _, k := range keys {
			if os.Getenv(k) != "" {
				return true
			}
		}
		return false
	}
	switch {
	case has("K_SERVICE", "FUNCTION_TARGET", "GAE_SERVICE", "GOOGLE_CLOUD_PROJECT"):
		return SchemaGCP
	case has("AWS_LAMBDA_FUNCTION_NAME", "ECS_CONTAINER_METADATA_URI_V4", "AWS_EXECUTION_ENV"):
		return SchemaAWS
	}
	return SchemaOTel
}

// Err returns an attribute with key "error" whose value is err.Error(),
// computed only if the record is written. A nil err yields an empty attribute,
// which handlers drop.
func Err(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}
	return slog.Any(KeyError, errValue{err})
}

type errValue struct{ err error }

func (e errValue) LogValue() slog.Value { return slog.StringValue(e.err.Error()) }

// jsonReplacer maps slog's built-in keys to the schema. User attributes with
// the same keys were renamed by the context handler, so a top-level built-in
// key here is always the built-in attribute.
func jsonReplacer(s Schema, user func([]string, slog.Attr) slog.Attr) func([]string, slog.Attr) slog.Attr {
	return func(groups []string, a slog.Attr) slog.Attr {
		if len(groups) == 0 {
			switch a.Key {
			case slog.TimeKey:
				if a.Value.Kind() == slog.KindTime {
					// Z-normalized RFC 3339 with up to 9 fractional digits.
					return slog.Time(s.TimeKey, a.Value.Time().UTC())
				}
			case slog.LevelKey:
				if l, ok := a.Value.Any().(slog.Level); ok {
					return slog.String(s.LevelKey, s.Level(l))
				}
			case slog.MessageKey:
				a.Key = s.MessageKey
				return a
			case slog.SourceKey:
				if src, ok := a.Value.Any().(*slog.Source); ok {
					if src == nil || src.File == "" {
						return slog.Attr{}
					}
					return slog.Attr{Key: s.SourceKey, Value: s.Source(src)}
				}
			}
		}
		if user != nil {
			return user(groups, a)
		}
		return a
	}
}

// textReplacer keeps slog's text keys for readability and only renders the
// level with the schema's names and the source as file:line.
func textReplacer(s Schema, user func([]string, slog.Attr) slog.Attr) func([]string, slog.Attr) slog.Attr {
	return func(groups []string, a slog.Attr) slog.Attr {
		if len(groups) == 0 {
			switch a.Key {
			case slog.TimeKey, slog.MessageKey:
				return a
			case slog.LevelKey:
				if l, ok := a.Value.Any().(slog.Level); ok {
					return slog.String(slog.LevelKey, s.Level(l))
				}
			case slog.SourceKey:
				if src, ok := a.Value.Any().(*slog.Source); ok {
					if src == nil || src.File == "" {
						return slog.Attr{}
					}
					return slog.Attr{Key: slog.SourceKey, Value: fileLine(src)}
				}
			}
		}
		if user != nil {
			return user(groups, a)
		}
		return a
	}
}
