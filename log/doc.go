// Package log is the logging package of kitchen: every package logs through
// it, so the output format, schema and backend are chosen in one place.
//
// [Logger] is an alias of slog.Logger: packages take a *log.Logger
// (Config.Logger or WithLogger; nil means [Default], see [OrDefault]) and
// build attributes with [String], [Err], [Int], ...; only this package
// imports log/slog. A *log.Logger can still be passed to libraries that take
// a *slog.Logger.
//
// main installs the process logger once:
//
//	opts, err := log.OptionsFromEnv()
//	logger, _ := log.Setup(opts)
//
// [Setup] (or [SetDefault]) makes [Default] write through it, also loggers
// taken from Default before the call and loggers derived from them, and
// installs it as log/slog's default and the standard library log package's
// output. Library packages never call Setup. Without it, Default writes the
// kitchen format configured from the environment ([OptionsFromEnv]), not
// log/slog's default output.
// [Discard] is for tests.
//
// Records written by this package's handlers are structured JSON (unless
// FormatText is selected) for the platform the service runs on, correlated
// with the active OpenTelemetry span.
//
// # Backends
//
// [New] returns a *slog.Logger whose records are encoded by zap's zapcore
// ([BackendZap], the default) or by the standard library's slog handlers
// ([BackendSlog]); LOG_BACKEND selects it in [OptionsFromEnv]. The output is
// the same JSON for a given schema. [NewZap] returns a *zap.Logger writing the
// same schema for code that prefers zap's API (without context, so without
// trace correlation).
//
// # Schemas
//
// A [Schema] names the built-in keys and the trace-correlation keys:
//
//	            time        level              message   source                                 trace
//	SchemaOTel  timestamp   level (WARN)       message   source "file:line"                     trace_id, span_id, trace_flags
//	SchemaGCP   time        severity (WARNING) message   logging.googleapis.com/sourceLocation  logging.googleapis.com/trace (bare id), spanId, trace_sampled
//	SchemaAWS   timestamp   level (WARN)       message   location "file:line"                   trace_id, span_id, trace_flags, xray_trace_id
//
// Times are RFC 3339 in UTC ("Z") with up to nanosecond precision. SchemaOTel
// is the default; [OptionsFromEnv] reads LOG_SCHEMA or detects the platform
// ([DetectSchema]). A custom schema is a struct literal.
//
// Trace keys are only written by the *Context logging methods (InfoContext,
// ErrorContext, ...) when the context carries a valid span. Attributes from
// Options.ContextAttrs and [WithAttrs] are added the same way. All of them
// stay at the top level of the record even when the logger has groups.
//
// Records at ERROR and above carry a stack trace in the format of
// runtime/debug.Stack, starting at the caller (logging frames removed):
// exception.stacktrace (OpenTelemetry semantic convention) for SchemaOTel,
// stack_trace for SchemaGCP (the field Cloud Error Reporting reads first, so
// error records appear there) and SchemaAWS. See Options.StackTraceLevel and
// Options.NoStackTrace.
//
// Request-scoped attributes come from [WithAttrs] (package kitchen/context
// stores request_id and user_id this way, so every record logged with a
// request context carries them) and from Options.ContextAttrs. A context
// attribute whose key the record's own attributes already have is skipped;
// attributes added with Logger.With are not checked.
//
// A top-level user attribute whose key the schema owns is written as
// "attr.<key>", so logged data cannot override the time, level, message or
// trace of a record. Other platform special fields (for example Cloud
// Logging's httpRequest or logging.googleapis.com/labels) may be set on
// purpose.
//
// Logging never creates span events and never changes span status; use
// package otelx to record errors on spans explicitly.
package log
