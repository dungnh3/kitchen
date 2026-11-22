# log: package rules

This is the only logging package in kitchen. Every other package logs through
it (D5). Read `doc.go` and `docs/design/observability.md` first.

- **Only this package imports `log/slog`.**
  - `log/slog`, zap, `slog.New`, `slog.Default` and `slog.SetDefault` are allowed
    here and banned everywhere else by lint.
  - If a package needs something slog has, re-export it from this package. Do
    not add a lint exception.
- **`Logger` stays an alias of `slog.Logger`.** It must keep working with libraries
  that take `*slog.Logger` (otel, gorm, goose, the MCP SDK).
- **The output is a contract.** Monitoring and log pipelines depend on:
  - the schemas (`otel`, `gcp`, `aws`): key names, level names, trace keys;
  - the `LOG_*` environment variables.

  Any change to these must:
  - apply to both backends (zap and slog) identically;
  - keep `testing/slogtest` passing for both backends;
  - keep the root `consistency_test.go` passing;
  - be recorded in `docs/migration/observability.md`.
- **Defaults.**
  - Without `Setup`, `Default()` must still emit kitchen JSON, never Go's text
    handler.
  - Stdio programs (MCP servers, `kmigrate`) set `LOG_OUTPUT=stderr` so that
    stdout stays clean.
- **Safety and performance.**
  - Keep the key-spoofing guard: user attrs must not override reserved keys.
  - Logging is on every request path: run `BenchmarkBackends` and
    `BenchmarkInfoContext` before and after any change to a handler.
