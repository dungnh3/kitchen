package log

import (
	"log/slog"
	"strconv"

	"go.opentelemetry.io/otel/trace"
)

// Schema describes the JSON a log record is written as: the names of the
// built-in keys, the level names, how the source location is rendered and
// which keys correlate the record with a trace. The built-in schemas cover
// OpenTelemetry-style generic JSON, Google Cloud Logging and AWS CloudWatch;
// a schema for another platform is a struct literal.
type Schema struct {
	// Name identifies the schema ("otel", "gcp", "aws").
	Name string
	// TimeKey, LevelKey and MessageKey name the record time, level and
	// message.
	TimeKey, LevelKey, MessageKey string
	// SourceKey names the source location; "" drops it.
	SourceKey string
	// Level renders a level as written under LevelKey.
	Level func(slog.Level) string
	// Source renders the source location written under SourceKey.
	Source func(*slog.Source) slog.Value
	// Trace returns the attributes that correlate a record with a valid span.
	Trace func(trace.SpanContext) []slog.Attr
	// StackKey names the stack trace written on records at or above
	// Options.StackTraceLevel, in the format of runtime/debug.Stack; "" writes
	// none.
	StackKey string
	// Reserved lists the other top-level keys the schema owns (the keys
	// Trace writes and platform special fields). A user attribute with one
	// of these keys, or with TimeKey, LevelKey, MessageKey or SourceKey, is
	// written as "attr.<key>".
	Reserved []string
}

// Cloud Logging special keys (SchemaGCP).
const (
	KeyGCPSeverity       = "severity"
	KeyGCPMessage        = "message"
	KeyGCPSourceLocation = "logging.googleapis.com/sourceLocation"
	KeyGCPTrace          = "logging.googleapis.com/trace"
	KeyGCPSpanID         = "logging.googleapis.com/spanId"
	KeyGCPTraceSampled   = "logging.googleapis.com/trace_sampled"
)

// Trace keys recommended by OpenTelemetry for non-OTLP log formats (SchemaOTel
// and SchemaAWS), plus the X-Ray form of the trace id (SchemaAWS).
const (
	KeyTraceID     = "trace_id"
	KeySpanID      = "span_id"
	KeyTraceFlags  = "trace_flags"
	KeyXRayTraceID = "xray_trace_id"
)

// KeyError is the key Err writes.
const KeyError = "error"

// Stack trace keys: the OpenTelemetry exception attribute (stable semantic
// convention) and the field Google Cloud Error Reporting reads first.
const (
	KeyExceptionStacktrace = "exception.stacktrace"
	KeyStackTrace          = "stack_trace"
)

// SchemaOTel is generic JSON following the OpenTelemetry log data model
// names: "timestamp", "level" (TRACE, DEBUG, INFO, WARN, ERROR, FATAL),
// "message", "source" ("file:line"), trace_id / span_id / trace_flags as
// lower-case hex (W3C) and exception.stacktrace. It is the default.
var SchemaOTel = Schema{
	Name:       "otel",
	TimeKey:    "timestamp",
	LevelKey:   "level",
	MessageKey: "message",
	SourceKey:  "source",
	Level:      otelLevel,
	Source:     fileLine,
	Trace:      w3cTrace,
	StackKey:   KeyExceptionStacktrace,
	Reserved:   []string{KeyTraceID, KeySpanID, KeyTraceFlags},
}

// SchemaGCP is the Google Cloud Logging structured-logging format: the
// special fields "time", "severity" (DEBUG ... EMERGENCY), "message",
// logging.googleapis.com/sourceLocation and the trace keys, with the bare
// trace id (Cloud Logging's preferred form). The agents on Cloud Run, GKE and
// Cloud Functions lift them into the LogEntry. Stack traces go to
// "stack_trace", the field Error Reporting evaluates first, so error records
// show up in Error Reporting.
var SchemaGCP = Schema{
	Name:       "gcp",
	TimeKey:    "time",
	LevelKey:   KeyGCPSeverity,
	MessageKey: KeyGCPMessage,
	SourceKey:  KeyGCPSourceLocation,
	Level:      Severity,
	Source:     gcpSourceLocation,
	Trace:      gcpTrace,
	StackKey:   KeyStackTrace,
	Reserved: []string{
		KeyGCPTrace, KeyGCPSpanID, KeyGCPTraceSampled,
		"timestamp", "timestampSeconds", "timestampNanos",
	},
}

// SchemaAWS is JSON for CloudWatch Logs in the shape of Lambda's JSON log
// format: "timestamp", "level" (TRACE, DEBUG, INFO, WARN, ERROR, FATAL),
// "message", "location" ("file:line"). Trace correlation carries both the
// W3C ids (trace_id, span_id, trace_flags) for OpenTelemetry and CloudWatch
// Transaction Search, and xray_trace_id ("1-xxxxxxxx-yyyyyyyyyyyyyyyyyyyyyyyy")
// for X-Ray-style queries. Stack traces go to "stack_trace".
var SchemaAWS = Schema{
	Name:       "aws",
	TimeKey:    "timestamp",
	LevelKey:   "level",
	MessageKey: "message",
	SourceKey:  "location",
	Level:      awsLevel,
	Source:     fileLine,
	Trace:      awsTrace,
	StackKey:   KeyStackTrace,
	Reserved:   []string{KeyTraceID, KeySpanID, KeyTraceFlags, KeyXRayTraceID},
}

// SchemaByName returns the built-in schema with the given name.
func SchemaByName(name string) (Schema, bool) {
	switch name {
	case SchemaOTel.Name:
		return SchemaOTel, true
	case SchemaGCP.Name:
		return SchemaGCP, true
	case SchemaAWS.Name:
		return SchemaAWS, true
	}
	return Schema{}, false
}

// reserved returns the set of top-level keys user attributes must not use.
func (s Schema) reserved() map[string]bool {
	m := map[string]bool{
		// slog's built-in keys: ReplaceAttr cannot tell them from user keys.
		slog.TimeKey: true, slog.LevelKey: true, slog.MessageKey: true, slog.SourceKey: true,
		s.TimeKey: true, s.LevelKey: true, s.MessageKey: true,
	}
	if s.SourceKey != "" {
		m[s.SourceKey] = true
	}
	if s.StackKey != "" {
		m[s.StackKey] = true
	}
	for _, k := range s.Reserved {
		m[k] = true
	}
	return m
}

// withDefaults returns SchemaOTel for the zero Schema, and otherwise fills
// empty fields (except SourceKey, where "" means no source) from SchemaOTel.
func (s Schema) withDefaults() Schema {
	d := SchemaOTel
	if s.Name == "" && s.TimeKey == "" && s.LevelKey == "" && s.MessageKey == "" && s.SourceKey == "" &&
		s.StackKey == "" && s.Level == nil && s.Source == nil && s.Trace == nil && len(s.Reserved) == 0 {
		return d
	}
	if s.Name == "" {
		s.Name = "custom"
	}
	if s.TimeKey == "" {
		s.TimeKey = d.TimeKey
	}
	if s.LevelKey == "" {
		s.LevelKey = d.LevelKey
	}
	if s.MessageKey == "" {
		s.MessageKey = d.MessageKey
	}
	if s.Level == nil {
		s.Level = d.Level
	}
	if s.Source == nil {
		s.Source = d.Source
	}
	if s.Trace == nil {
		s.Trace = d.Trace
	}
	return s
}

func otelLevel(l slog.Level) string {
	switch {
	case l < LevelDebug:
		return "TRACE"
	case l < LevelInfo:
		return "DEBUG"
	case l < LevelWarn:
		return "INFO"
	case l < LevelError:
		return "WARN"
	case l < LevelCritical:
		return "ERROR"
	default:
		return "FATAL"
	}
}

func awsLevel(l slog.Level) string {
	if l < LevelDebug {
		return "TRACE"
	}
	return otelLevel(l)
}

func fileLine(src *slog.Source) slog.Value {
	return slog.StringValue(src.File + ":" + strconv.Itoa(src.Line))
}

func gcpSourceLocation(src *slog.Source) slog.Value {
	return slog.GroupValue(
		slog.String("file", src.File),
		slog.String("line", strconv.Itoa(src.Line)),
		slog.String("function", src.Function),
	)
}

func w3cTrace(sc trace.SpanContext) []slog.Attr {
	return []slog.Attr{
		slog.String(KeyTraceID, sc.TraceID().String()),
		slog.String(KeySpanID, sc.SpanID().String()),
		slog.String(KeyTraceFlags, sc.TraceFlags().String()),
	}
}

func gcpTrace(sc trace.SpanContext) []slog.Attr {
	return []slog.Attr{
		slog.String(KeyGCPTrace, sc.TraceID().String()),
		slog.String(KeyGCPSpanID, sc.SpanID().String()),
		slog.Bool(KeyGCPTraceSampled, sc.IsSampled()),
	}
}

func awsTrace(sc trace.SpanContext) []slog.Attr {
	tid := sc.TraceID().String()
	return append(w3cTrace(sc), slog.String(KeyXRayTraceID, "1-"+tid[:8]+"-"+tid[8:]))
}
