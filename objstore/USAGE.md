# objstore

`objstore` is kitchen's backend-neutral object storage API. It reads, writes
and deletes objects, and signs browser-direct uploads and downloads. Use it
for files and blobs. Do not call the GCS or S3 SDKs (or gocloud.dev) from
services. It is the canonical package for object storage
([D7](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22)).

Two interfaces cover everything:

- `Bucket`: `NewWriter`, `NewReader`, `Attributes`, `Delete`, `Close`.
- `Signer`: `SignedURL` and `PostPolicy`, for clients that talk to storage
  directly.

Helpers work on any `Bucket`: `Put`, `ReadAll`, `Exists`. Backends:
`memstore` (in memory, for tests and dev), and the `gcs` and `s3`
submodules. Godoc has the full API.

## Install

```sh
go get github.com/dungnh3/kitchen              # objstore, objstore/memstore
go get github.com/dungnh3/kitchen/objstore/gcs # Google Cloud Storage
go get github.com/dungnh3/kitchen/objstore/s3  # Amazon S3, MinIO, R2
```

## Quick start

Write and read with any backend. `memstore` needs no setup:

```go
ctx := context.Background()
var b objstore.Bucket = memstore.New() // or gcs.Open(...)
defer func() { _ = b.Close() }()

key := "reports/q1.csv"
if err := objstore.Put(ctx, b, key, strings.NewReader("a,b\n1,2\n"), &objstore.WriterOptions{ContentType: "text/csv"}); err != nil {
	panic(err)
}
attrs, _ := b.Attributes(ctx, key)
fmt.Println(attrs.Size, attrs.ContentType)
```

The defaults are safe:

- Every operation checks the key with `ValidKey` first. Traversal (`..`),
  a leading slash, empty segments, control characters and invalid UTF-8
  are rejected.
- An object becomes visible only after the writer's `Close` returns nil.
  A failed or cancelled write leaves nothing behind.
- Signed upload URLs fail closed: they need a content type, a size bound,
  and they only create (never overwrite).
- Errors are `errs` errors, so a kitchen server answers 404, 409 or 400,
  not 500.

## Configuration

`objstore` itself has no config. Each backend has a `Config` struct:

| Field | Backends | Default | When to change |
|---|---|---|---|
| `Bucket` | gcs, s3 | required | always |
| `CDNBaseURL` | gcs, s3 | empty (`PublicURL` fails) | objects are served from a CDN |
| `SignExpiry` | gcs, s3 | 5m (max 7 days) | slow clients; keep it short |
| `MaxUploadBytes` | gcs, s3 | 20 MiB | default size bound for signed uploads |
| `GoogleAccessID`, `PrivateKey` | gcs | attached service account | you sign with a key file |
| `PartSize` | s3 | 8 MiB (min 5 MiB) | very large objects; one part is buffered in memory |

Per call, `WriterOptions` sets `ContentType`, `CacheControl`,
`ContentDisposition`, `Metadata`, `ChunkSize` (GCS resumable chunks) and
`IfNotExists`.

## Integration with kitchen

### Errors and HTTP status

Backends map SDK errors to four `errs` sentinels. SDK error types never
reach your code.

| Sentinel | Reason | gRPC code | HTTP |
|---|---|---|---|
| `ErrNotFound` | `OBJECT_NOT_FOUND` | NotFound | 404 |
| `ErrExists` | `OBJECT_ALREADY_EXISTS` | AlreadyExists | 409 |
| `ErrInvalidKey` | `INVALID_OBJECT_KEY` | InvalidArgument | 400 |
| `ErrInvalidOptions` | `INVALID_UPLOAD_OPTIONS` | InvalidArgument | 400 |

Match them with `errors.Is`. Return them as they are, and `errs.WriteHTTP`
(or the gRPC server) picks the status:

```go
h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	_, err := b.Attributes(ctx, strings.TrimPrefix(r.URL.Path, "/files/"))
	if err != nil {
		errs.WriteHTTP(w, r, err) // ErrNotFound → 404, ErrInvalidKey → 400
		return
	}
	w.WriteHeader(http.StatusNoContent) // the object exists
})
```

In a kitchen server, return the error from your handler instead. Service
wiring is in [server/USAGE.md](../server/USAGE.md).

### Tracing with otelx

Wrap a bucket with `Instrument`. Every operation runs in an
`objstore.<Op>` span, and errors are recorded with `otelx`. Backends never
trace on their own.

```go
ctx := context.Background()
b := objstore.Instrument(memstore.New(), objstore.WithSystem("memstore"))
defer func() { _ = b.Close() }()

// Every call runs in an "objstore.<Op>" span; errors are recorded with otelx.
_ = objstore.Put(ctx, b, "reports/q1.csv", strings.NewReader("a,b\n"), &objstore.WriterOptions{ContentType: "text/csv"})
data, err := objstore.ReadAll(ctx, b, "reports/q1.csv")
```

The result is both a `Bucket` and a `Signer`. If the wrapped bucket does
not sign, the signing methods return `errs.ErrUnimplemented`. The tracer
provider defaults to the global one, which `otelx/sdk` sets up; pass
`WithTracerProvider` to override it.

## Advanced

### Keys

Build keys with `NewObjectKey(ext, prefix...)`. It returns
`<prefix>/<YYYYMMDD UTC>/<xid><ext>`: unique, sortable by time, URL-safe.
Never build a key from a raw file name the user sent.
`ExtensionByType` and `Category` turn a MIME type into `.png` and `image`.
`PublicURL(base, key)` joins a CDN base and a key, escaping each segment.

Check keys that come from clients with `ValidKey`:

```go
for _, key := range []string{"avatars/2025/user-42.jpg", "../etc/passwd", "/abs", "a//b"} {
	err := objstore.ValidKey(key)
	fmt.Println(key, errors.Is(err, objstore.ErrInvalidKey), errs.HTTPStatus(err))
}
```

### Readers and writers

`Put` and `ReadAll` cover most cases. Use `NewWriter` and `NewReader` to
stream large objects. Always close what you open:

```go
r, err := b.NewReader(ctx, "notes/a.txt")
if err != nil {
	panic(err)
}
defer func() { _ = r.Close() }()
data, _ := io.ReadAll(r)
```

To abort a write, cancel its `ctx`. `Close` then returns an error and
nothing is stored. `Put` does this for you when the reader fails.

### Create-only writes

Set `IfNotExists` when two writers must not overwrite each other (invoice
files, idempotent exports). The check is atomic in every backend. The
loser gets `ErrExists`:

```go
create := &objstore.WriterOptions{ContentType: "application/json", IfNotExists: true}
first := objstore.Put(ctx, b, "invoices/2025-001.json", strings.NewReader(`{"n":1}`), create)
second := objstore.Put(ctx, b, "invoices/2025-001.json", strings.NewReader(`{"n":2}`), create)
fmt.Println(first)
fmt.Println(errors.Is(second, objstore.ErrExists), errs.HTTPStatus(second))
```

### Browser uploads: prefer PostPolicy

A POST policy lets a browser upload with an HTML form, straight to the
bucket. The policy is bound to one key, a size range, and a content type
or prefix. It works the same way on GCS and S3:

```go
key := objstore.NewObjectKey(objstore.ExtensionByType("image/png"), "image")
pp, err := s.PostPolicy(ctx, key, objstore.PostPolicyOptions{
	ContentTypePrefix: "image/",
	MaxBytes:          10 << 20,
})
if err != nil {
	panic(err)
}
// Return pp as JSON; the browser POSTs pp.Fields then the "file" field to pp.URL.
```

`PostPolicyOptions` takes `MinBytes`, `MaxBytes` (default
`Config.MaxUploadBytes`), `Expires`, and `ContentType` or
`ContentTypePrefix`. One of the two content type fields is required. A
POST policy cannot forbid overwrites, so always use a fresh key from
`NewObjectKey`.

### Signed URLs

`SignedURL` signs a GET (download) or PUT (upload) URL. A PUT URL fails
closed:

- `ContentType` is required. The client must send exactly that type.
- The size is bounded by `MaxBytes` (default `Config.MaxUploadBytes`).
- It only creates. It never replaces an existing object.

```go
// A PUT URL is create-only and needs a content type.
u, err := s.SignedURL(ctx, "avatars/u42.png", objstore.SignOptions{
	Method:      http.MethodPut,
	ContentType: "image/png",
	MaxBytes:    5 << 20,
})
fmt.Println(u != "", err)

// Without ContentType the signer fails closed.
_, err = s.SignedURL(ctx, "avatars/u42.png", objstore.SignOptions{Method: http.MethodPut})
fmt.Println(errors.Is(err, objstore.ErrInvalidOptions), errs.HTTPStatus(err))
```

For downloads, `ResponseContentType` and `ResponseContentDisposition` are
part of the signature, so clients cannot change them.

Three options turn a safety rule off. Each has a reason to exist:

- **`AllowAnyContentType`** accepts any content type. **Warning:** an
  upload served from your CDN domain can then be HTML or SVG, which is
  stored XSS. Set it only for private buckets that are never served to a
  browser.
- **`AllowOverwrite`** lets a PUT URL replace an existing object.
  **Warning:** anyone with the URL can then overwrite data that someone
  else uploaded under that key. Use it only for keys owned by one user.
- **`AllowUnboundedSize`** lets S3 sign a PUT it cannot size-bound.
  **Warning:** the client can then upload any amount of data at your
  cost. Use `PostPolicy` instead.

## Backends and adapters

### memstore

In-memory `Bucket` and `Signer` in the core module. `memstore.New()`.
`SignedURL` returns a fake `mem://` URL that encodes the request, and
`PostPolicy` echoes its options, so tests can assert what would be signed.
`Keys()` lists the stored keys. Never use it in production.

### gcs

`gcs.Open(ctx, cfg, opts...)` creates a storage client from
`option.ClientOption`s and owns it, so `Close` closes it. `gcs.New(client,
cfg)` wraps a client you own. `Handle()` returns the bucket handle for
features `objstore` does not cover.

```go
put, err := b.SignedURL(ctx, "avatars/u42.png", objstore.SignOptions{
	Method:      http.MethodPut,
	ContentType: "image/png",
	MaxBytes:    5 << 20,
})
```

- URLs and policies are V4. Without `GoogleAccessID` and `PrivateKey`, the
  client signs with the attached service account (metadata server or IAM
  `signBlob`), which needs the "Service Account Token Creator" role.
- A PUT URL signs `x-goog-content-length-range` and, unless
  `AllowOverwrite`, `x-goog-if-generation-match: 0`. The client must send
  both headers exactly as signed.
- `IfNotExists` uses the `DoesNotExist` precondition.

### s3

`s3.Open(ctx, cfg, optFns...)` loads the default AWS config (environment,
shared config, IAM role). `s3.New(client, cfg)` wraps a client, for
MinIO, Cloudflare R2 or a custom endpoint:

```go
// An S3-compatible store (MinIO, Cloudflare R2) with an explicit client.
client := awss3.New(awss3.Options{
	Region:       "auto",
	BaseEndpoint: aws.String("https://minio.internal:9000"),
	UsePathStyle: true,
	Credentials:  credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
})
b, err := s3.New(client, s3.Config{Bucket: "media"})
if err != nil {
	panic(err)
}

// S3 cannot bound the size of a presigned PUT, so it is refused by default.
_, err = b.SignedURL(context.Background(), "uploads/a.png", objstore.SignOptions{
	Method: http.MethodPut, ContentType: "image/png",
})
fmt.Println(errors.Is(err, objstore.ErrInvalidOptions))
```

Limits and differences from GCS:

- A presigned PUT cannot bound the size. `SignedURL` refuses PUT unless
  `AllowUnboundedSize` is set (see the warning above). Use `PostPolicy`.
- Writes below `PartSize` are one `PutObject`; larger ones are a multipart
  upload, aborted on error or cancel. `IfNotExists` sends
  `If-None-Match: *` on both paths.
- `Delete` of a missing key costs an extra HEAD, because S3 deletes
  silently.
- `Close` is a no-op. `Client()` returns the SDK client.

## Testing

Use `memstore` in unit tests. It follows the same rules as the real
backends for key validation, writes, `IfNotExists`, errors and signing
options. Assert on
errors with `errors.Is(err, objstore.ErrNotFound)` and on status with
`errs.HTTPStatus(err)`.

For backend-level tests, `objstore/s3` uses gofakes3 and `objstore/gcs`
uses the GCS emulator (fake-gcs-server). Never test against real buckets.

## Observability

- **Spans** (from `Instrument`): `objstore.Write`, `objstore.Read`,
  `objstore.Attributes`, `objstore.Delete`, `objstore.SignedURL`,
  `objstore.PostPolicy`. Kind client. Attributes: `objstore.key`,
  `objstore.system` (from `WithSystem`), and `objstore.bytes` on writes.
  A write span lasts until `Close`.
- **Metrics:** none of its own. Use the SDK's or your HTTP metrics.
- **Errs reasons:** `OBJECT_NOT_FOUND`, `OBJECT_ALREADY_EXISTS`,
  `INVALID_OBJECT_KEY`, `INVALID_UPLOAD_OPTIONS`.

## Production checklist

- Wrap every bucket with `objstore.Instrument(b, objstore.WithSystem(...))`.
- Use `PostPolicy` for browser uploads, with a fresh `NewObjectKey` key,
  a `ContentType` or `ContentTypePrefix`, and a `MaxBytes` that fits the
  use case.
- Keep `SignExpiry` short (the 5m default is fine for most uploads).
- Do not set `AllowAnyContentType`, `AllowOverwrite` or
  `AllowUnboundedSize` on public buckets.
- Use `IfNotExists` when an object must be written once.
- Call `Close` on shutdown.
- Return objstore errors as they are, so clients get 404/409/400.

## Migrating from the old kitchen

`uploader` becomes `objstore` plus `objstore/gcs` (and `objstore/s3`). See
[data-utils.md](../docs/migration/data-utils.md#uploader--objstore--objstoregcs--objstores3).
