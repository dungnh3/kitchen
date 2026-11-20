// Package log builds [*slog.Logger] values that write structured JSON in the
// Google Cloud Logging schema and correlate every record with the active
// OpenTelemetry span.
//
// The package has no global state: [New] returns a plain *slog.Logger that the
// application passes to the libraries it constructs. Libraries accept a
// *slog.Logger and never import this package.
//
// # Output schema (JSON)
//
//	time                                    RFC 3339 with nanoseconds
//	severity                                DEBUG|INFO|NOTICE|WARNING|ERROR|CRITICAL|ALERT|EMERGENCY
//	message                                 the log message
//	logging.googleapis.com/sourceLocation   {"file","line","function"} (unless Options.NoSource)
//	logging.googleapis.com/trace            trace id, or projects/<p>/traces/<id> when Options.GCPProjectID is set
//	logging.googleapis.com/spanId           span id
//	logging.googleapis.com/trace_sampled    true|false
//
// Trace keys are only written by the *Context logging methods (InfoContext,
// ErrorContext, ...) when the context carries a valid span. Attributes stored
// with [WithAttrs] are added the same way. Both stay at the top level of the
// record even when the logger has groups.
//
// Logging never creates span events and never changes span status; use
// package otelx to record errors on spans explicitly.
package log
