# errs: package rules

`errs` is kitchen's error model and wire contract (AIP-193). Clients parse its
output, so every change here is an API change. Read `doc.go` and
`docs/design/errors-auth.md` first.

- **Wire format.** What crosses the wire is a `google.rpc.Status` carrying
  `ErrorInfo` (reason, domain, metadata), plus the optional standard details:
  `LocalizedMessage`, `BadRequest` and `RetryInfo`. Do not add custom proto
  types.
- **HTTP status.** It comes from the gRPC code (`HTTPStatusFromCode`). The only
  exceptions are 413 `PAYLOAD_TOO_LARGE` and 415 `UNSUPPORTED_MEDIA_TYPE`.
  Changing this mapping is a breaking change and needs a note in
  `docs/migration/errors-auth.md`.
- **Never leak internals.** A wrapped cause is for logs only. Unknown errors are
  sanitised to INTERNAL before they reach clients. Never put SQL, stack
  traces, tokens or upstream bodies into messages or metadata.
- **Reasons are stable identifiers** (`UPPER_SNAKE`). Never rename or reuse a
  reason. Every new reason gets English and Vietnamese messages in `i18n`.
- **`errors.Is` semantics:** code sentinels match by gRPC code, and every
  other sentinel matches by reason + domain, including after
  `Wrap`/`With`. Keep both behaviours covered by tests.
- **Single construction point.** Other packages build client-facing errors
  only through `errs`. Constructing gRPC `status` values directly is banned by
  D7 lint. Database adapters map their errors through `pgx.MapError`,
  `mysqlx` and `mongox`.
