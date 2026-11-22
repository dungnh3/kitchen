# objstore: package rules

`objstore` is the backend-neutral object storage API. Backends live in
`objstore/memstore`, plus the `objstore/gcs` and `objstore/s3` submodules.
Every backend must behave the same way, whatever its SDK does. When you
change behaviour, change it in every backend and test it in each.

- **Keys.** Every operation on every backend calls `ValidKey` first. Never
  accept path traversal, invalid UTF-8 or empty keys.
- **Browser uploads fail closed.** These rules apply to signed PUT URLs and
  POST policies:
  - A signed PUT URL needs a `ContentType`.
  - A signed PUT URL must be size-bounded. GCS uses
    `x-goog-content-length-range`. S3 refuses unless `AllowUnboundedSize` is
    set.
  - A signed PUT URL is create-only.
  - A POST policy needs a `ContentType` or a `ContentTypePrefix`.

  Never add a shortcut that signs an unbounded or any-type upload by default.
- **Conditional writes.** `WriterOptions.IfNotExists` must be atomic in
  every backend (GCS `DoesNotExist`, S3 `If-None-Match` on Put and on
  Complete). A losing writer gets `ErrExists`.
- **Errors.** Backends map SDK errors to `ErrNotFound`, `ErrExists`,
  `ErrInvalidKey` and `ErrInvalidOptions`, all of which are `errs` errors.
  SDK error types never reach callers.
- **Writers.** A failed or cancelled write must leave no partial object
  (S3 aborts the multipart upload). `Close` reports the real outcome.
- **Telemetry.** Tracing comes from `objstore.Instrument` (`otelx`), not
  from inside a backend.
- **Tests.** Backend tests use emulators and fakes (gofakes3, the GCS
  emulator, memstore), never real buckets.
