package log

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// Format selects the record encoding.
type Format string

// Formats.
const (
	// FormatJSON writes one JSON object per line in the Cloud Logging schema.
	// It is the default.
	FormatJSON Format = "json"
	// FormatText writes slog's key=value text, for local development. Keys
	// keep slog's names (time, level, msg, source); levels use severity names.
	FormatText Format = "text"
)

// Keys written by the handler. They are the special fields Cloud Logging
// lifts into the LogEntry when it ingests JSON from stdout.
const (
	KeySeverity       = "severity"
	KeyMessage        = "message"
	KeySourceLocation = "logging.googleapis.com/sourceLocation"
	KeyTrace          = "logging.googleapis.com/trace"
	KeySpanID         = "logging.googleapis.com/spanId"
	KeyTraceSampled   = "logging.googleapis.com/trace_sampled"
	KeyError          = "error"
)

// Options configures [New] and [NewHandler]. The zero value logs JSON at INFO
// with source locations.
type Options struct {
	// Level is the minimum level. nil means INFO. Pass a *slog.LevelVar to
	// change the level at runtime.
	Level slog.Leveler
	// Format is FormatJSON (default, also used for "") or FormatText.
	Format Format
	// NoSource disables the source location (file, line, function).
	NoSource bool
	// GCPProjectID, when set, formats the trace key as
	// projects/<GCPProjectID>/traces/<trace-id>. Otherwise the bare trace id
	// is written.
	GCPProjectID string
	// ReplaceAttr rewrites user attributes. It is not called for the built-in
	// time, level, message and source attributes, whose keys are fixed.
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

// New returns a logger that writes to w. See [NewHandler].
func New(w io.Writer, opts Options) *slog.Logger {
	return slog.New(NewHandler(w, opts))
}

// NewHandler returns a handler that writes records to w in the configured
// format and adds trace correlation and context attributes (see
// [NewContextHandler]).
func NewHandler(w io.Writer, opts Options) slog.Handler {
	level := opts.Level
	if level == nil {
		level = LevelInfo
	}
	ho := &slog.HandlerOptions{
		AddSource: !opts.NoSource,
		Level:     level,
	}
	var h slog.Handler
	if opts.Format == FormatText {
		ho.ReplaceAttr = textReplacer(opts.ReplaceAttr)
		h = slog.NewTextHandler(w, ho)
	} else {
		ho.ReplaceAttr = gcpReplacer(opts.ReplaceAttr)
		h = slog.NewJSONHandler(w, ho)
	}
	return NewContextHandler(h, opts.GCPProjectID)
}

// OptionsFromEnv builds Options from the environment:
//
//	LOG_LEVEL             debug|info|notice|warn|error|critical|alert|emergency (default info)
//	LOG_FORMAT            json|text (default json)
//	GOOGLE_CLOUD_PROJECT  Options.GCPProjectID
//
// Level is a *slog.LevelVar, so callers can change it later. An invalid value
// is an error; the returned Options are still usable with the defaults.
func OptionsFromEnv() (Options, error) {
	lv := new(slog.LevelVar)
	opts := Options{Level: lv, GCPProjectID: os.Getenv("GOOGLE_CLOUD_PROJECT")}
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
	if len(errs) > 0 {
		return opts, errors.Join(errs...)
	}
	return opts, nil
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

func gcpReplacer(user func([]string, slog.Attr) slog.Attr) func([]string, slog.Attr) slog.Attr {
	return func(groups []string, a slog.Attr) slog.Attr {
		if len(groups) == 0 {
			switch a.Key {
			case slog.TimeKey:
				if a.Value.Kind() == slog.KindTime {
					return a
				}
			case slog.LevelKey:
				if l, ok := a.Value.Any().(slog.Level); ok {
					return slog.String(KeySeverity, Severity(l))
				}
			case slog.MessageKey:
				if a.Value.Kind() == slog.KindString {
					a.Key = KeyMessage
					return a
				}
			case slog.SourceKey:
				if src, ok := a.Value.Any().(*slog.Source); ok {
					return sourceLocation(src)
				}
			}
		}
		if user != nil {
			return user(groups, a)
		}
		return a
	}
}

func textReplacer(user func([]string, slog.Attr) slog.Attr) func([]string, slog.Attr) slog.Attr {
	return func(groups []string, a slog.Attr) slog.Attr {
		if len(groups) == 0 {
			switch a.Key {
			case slog.TimeKey, slog.MessageKey:
				return a
			case slog.LevelKey:
				if l, ok := a.Value.Any().(slog.Level); ok {
					return slog.String(slog.LevelKey, Severity(l))
				}
			case slog.SourceKey:
				if src, ok := a.Value.Any().(*slog.Source); ok {
					if src == nil || src.File == "" {
						return slog.Attr{}
					}
					return slog.String(slog.SourceKey, src.File+":"+strconv.Itoa(src.Line))
				}
			}
		}
		if user != nil {
			return user(groups, a)
		}
		return a
	}
}

func sourceLocation(src *slog.Source) slog.Attr {
	if src == nil || src.File == "" {
		return slog.Attr{}
	}
	return slog.Group(KeySourceLocation,
		slog.String("file", src.File),
		slog.String("line", strconv.Itoa(src.Line)),
		slog.String("function", src.Function),
	)
}
