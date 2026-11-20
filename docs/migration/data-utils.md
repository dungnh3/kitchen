# Migration: data-utils

From the old library's `cache`, `storage`, `migrations`, `uploader`, `utils`,
`parallel` and `crawl` packages to `cache`, `redisx`, `pg`, `pg/gorm`,
`migrate`, `blob`, `blob/gcs`, `xslices`, `xmaps`, `timex` and `webmeta`.
Design and rationale: [`docs/design/data-utils.md`](../design/data-utils.md).

Module paths (D1):

| Import | Module |
|---|---|
| `github.com/dungnh3/kitchen/{cache,redisx,pg,blob,blob/memblob,xslices,xmaps,timex,webmeta}` | core `github.com/dungnh3/kitchen` |
| `github.com/dungnh3/kitchen/pg/gorm` (package `pggorm`) | submodule |
| `github.com/dungnh3/kitchen/migrate` (+ `cmd/kmigrate`) | submodule |
| `github.com/dungnh3/kitchen/blob/gcs` | submodule |

## Old bugs fixed (behaviour changes)

Every defect listed in `docs/review/06-data-utils.md` is fixed or removed with
the API that had it. None is carried over.

| Old package | Old bug | Now |
|---|---|---|
| migrations | `Down` reverted **all** migrations (same as `DownAll`) | `Down(ctx, n)` reverts exactly n |
| migrations | `Down`/`DownAll`/`Steps` closed the caller's DB pool and `log.Panicf`'d on close errors | `Close` never touches the caller's pool; errors are returned |
| migrations | `Up` never closed the migrator and pinned a pool connection forever | `Close` releases it |
| migrations | `file://` only, files had to be on disk at `/migrations` | any `fs.FS` (`embed.FS`) |
| migrations | used `lib/pq`, broken go.mod (missing/indirect deps) | pgx/v5 driver, tidy go.mod |
| migrations | `ErrNoChange` surfaced as an error, dirty state unclear | no-change is nil; dirty → `*DirtyError` |
| cache | `RemoveByPattern` used blocking `KEYS` and ignored the iterator error | `DeleteByPrefix`: SCAN + UNLINK, glob-escaped, errors returned |
| cache | only `*redis.Client` (no Cluster/Sentinel) | `redis.UniversalClient` |
| cache | no stampede protection, no local tier | singleflight `GetOrLoad`, optional L1 |
| cache | `HGetAll` re-encoded JSON lossily (numbers → float64) | removed (use go-redis hashes directly) |
| cache | `Increase` set no TTL | removed (use go-redis `Incr` + `Expire`) |
| cache | go-redis marked `// indirect`, no tests | tidy, miniredis + synctest tests |
| storage | Postgres DSN hardcoded `sslmode=disable` | `SSLMode` default `prefer`, configurable |
| storage | DSN hardcoded `TimeZone=Asia/Ho_Chi_Minh` | no default; `RuntimeParams["timezone"]` |
| storage | password not escaped (spaces/quotes broke the DSN) | built with `url.UserPassword` |
| storage | no pool settings | MaxConns, lifetimes, idle time, health check, timeouts |
| storage | package named `config` in dir `storage`; `GormLogLevel` duplicated | packages `pg`, `pggorm`, `redisx` |
| storage | MySQL `Loc *time.Location` not loadable from config; `GoogleAuthFile` ignored | MySQL removed |
| uploader | V1 `CreateFileUploadURL` ignored the per-file size limit | `PostPolicyOptions.MaxBytes` per call, default from config |
| uploader | V1 constructor called `log.Fatal` | constructors return errors |
| uploader | `DetectSourceCategory` compared prefixes backwards and depended on map order | `blob.Category` parses the MIME type |
| uploader | object keys used `filepath.Join` (backslashes on Windows) | `path.Join` |
| uploader | CDN URL built with `Sprintf`, no escaping | `blob.PublicURL` escapes each segment |
| uploader | `UploadBytes` forced `ChunkSize=0`, disabling retries | `blob.Put` keeps the client's resumable default; `ChunkSize` is opt-in |
| uploader | no `Close` for the client | `Close` closes the client created by `gcs.Open` |
| uploader | `UploadStream` recovered panics and never closed/aborted the writer | `blob.Put` cancels and closes on error; no panic swallowing |
| uploader | `pkg/errors`, no tests, no fake | stdlib errors, fake-gcs-server tests, `memblob` |
| utils | `IsContains` used reflection + `DeepEqual` | `slices.Contains` |
| utils | `Max`/`Min` panicked on empty input | removed; see table above |
| utils | `UniqueArray` returned random order | `xslices.Uniq` keeps first-seen order |
| utils | `Sum` accepted strings via `constraints.Ordered` | removed |
| utils | `ChunkArray` actually meant "first N" | removed; slice expression |
| utils | regexes recompiled on every call | removed with the VN helpers; `timex` uses no regex |
| utils | `MustParseTime` ran all patterns without early return and hid errors | `timex.Parse` returns the first match or an error |
| utils | `StartOfDay`/`StartOfMonth`/`EndOfMonth` forced UTC, dropping the location | `timex` keeps `t.Location()` |
| utils | `EndOfDay` truncated to the second | last nanosecond of the day |
| utils | `VietnamLocation` silently nil without tzdata | removed; `time.LoadLocation` returns the error |
| utils | `ParseTemplate` used `html/template` for text (escaped output), sprig v2 `+incompatible` | removed |
| parallel | dropped task errors, lost result order, deadlocked for n ≤ 0, ignored ctx, leaked goroutines | removed; `errgroup.SetLimit` |
| crawl | no `context` | `Fetch(ctx, url)` |
| crawl | no status or Content-Type check | `*StatusError`, `ErrNotHTML` |
| crawl | unbounded body | `io.LimitReader` (2 MiB default) |
| crawl | no User-Agent, no charset decoding | UA header; `x/net/html/charset` |
| crawl | SSRF: LLM-supplied URLs could reach private/metadata addresses | dial-time guard after DNS and on redirects |
| crawl | logo chosen by document order | fixed priority (og:image > twitter:image; apple-touch-icon > icon > /favicon.ico) |
| crawl | skipped extensions returned an empty result with nil error | non-HTML → `ErrNotHTML` |

## storage → pg, pg/gorm, redisx

| Old | New |
|---|---|
| `config.Postgres{...}.FormatDSN()` + `gorm.Open(postgres.Open(dsn))` | `pool, err := pg.Open(ctx, pg.Config{...})` then `pggorm.Open(pool, pggorm.Options{Logger: log})` |
| `config.Postgres.GormLogLevel` | `pggorm.Options{LogLevel: logger.Warn, SlowThreshold: ...}` (slog-based) |
| `config.Redis{...}` + `redis.NewClient` | `rdb, err := redisx.Open(ctx, redisx.Config{Addrs: ..., Password: ...})` |
| `config.MySQL` | **removed**; open MySQL in the service (`gorm.Open(mysql.Open(dsn))`). |

Behaviour changes:

- **TLS:** the old DSN forced `sslmode=disable`. `pg.Config.SSLMode` defaults
  to `prefer` (TLS when the server offers it). Set `SSLMode: "disable"` (or
  `?sslmode=disable` in `URL`) to keep the old behaviour.
- **Time zone:** the old DSN forced `TimeZone=Asia/Ho_Chi_Minh`. `pg` sets no
  time zone. To keep it: `RuntimeParams: map[string]string{"timezone": "Asia/Ho_Chi_Minh"}`.
- **Password escaping:** passwords with spaces, quotes or `@` now work.
- **Pool:** defaults are MaxConnLifetime 30m (+5m jitter), MaxConnIdleTime 5m,
  HealthCheckPeriod 30s, ConnectTimeout 5s, MaxConns = pgx default. `Open`
  pings before returning and returns errors instead of calling `log.Fatal`.
- **Behind PgBouncer (transaction mode):** set `QueryExecMode: pg.ExecModeExec`
  (or `ExecModeSimpleProtocol`); the default caches prepared statements.
- gorm and pgx share one pool, so `MaxConns` is the only connection limit.
  Tracing comes from otelpgx on the pool; drop any gorm OTel plugin.
- `Redis` with several addresses becomes a Cluster client; `MasterName`
  selects Sentinel.

## migrations → migrate

| Old | New |
|---|---|
| `migrations.Up(db *gorm.DB, &Options{...})` | `m, err := migrate.New(pool, migrate.Config{Source: embedFS, Dir: "sql"}); defer m.Close(); err = m.Up(ctx)` |
| `migrations.Down(db, opts)` | `m.Down(ctx, 1)` — **reverts one migration** |
| `migrations.DownAll(db, opts)` | `m.Down(ctx, n)` with n = number of applied migrations (deliberately no one-call "drop everything") |
| `migrations.Steps(db, opts)` with n>0 / n<0 | `m.To(ctx, version)` / `m.Down(ctx, n)` |
| migrate CLI in the `db-migrations` repo | `go run github.com/dungnh3/kitchen/migrate/cmd/kmigrate -database "$DATABASE_URL" -path ./postgresql/<svc> up` |

Behaviour changes:

- **Old `Down` reverted every migration** (it called golang-migrate's
  all-down). New `Down(ctx, n)` reverts exactly `n`.
- **Old `Down`/`DownAll`/`Steps` closed the caller's DB pool** and panicked on
  close errors. `Migrator.Close` never closes the pool passed to `New`.
- Old `Up` leaked one pinned connection; `Close` now releases it.
- History table and file format are unchanged (`schema_migrations`,
  `<version>_<name>.up.sql/.down.sql`): no state migration is needed.
- Migrations are read from any `fs.FS` (use `embed.FS`); files no longer have to
  exist on disk at `/migrations`.
- A dirty database returns `*migrate.DirtyError`; it is never forced
  automatically. Repair the schema, then `m.Force(ctx, lastGoodVersion)` or
  `kmigrate force V`.
- Driver is pgx/v5 (`lib/pq` removed). Concurrent runs wait on an advisory lock
  for `LockTimeout` (default 1m).

## cache → cache

| Old | New |
|---|---|
| `cache.NewRedisCache(rdb)` (`Cache` of `interface{}`) | `cache.New(cache.Config[T]{Redis: rdb, Prefix: "x:", TTL: d})` (typed) |
| `Get(key, &v)` / `Set(key, v, ttl)` | `c.Get(ctx, key) (T, error)` / `c.Set(ctx, key, v)` (TTL from config) |
| get, miss → load → set | `c.GetOrLoad(ctx, key, loader)` (singleflight, optional L1) |
| `Del(keys...)` | `c.Delete(ctx, keys...)` |
| `RemoveByPattern(pattern)` | `c.DeleteByPrefix(ctx, prefix)` (SCAN + UNLINK, Cluster-aware) |
| `SetNX`, `Incr`, `Expire`, `Exists`, `HGet/HSet/HGetAll`, ... | use go-redis directly (`rdb.SetNX(ctx, ...)`) |
| `cache.ErrNotFound` | `cache.ErrNotFound` (also returned by loaders to enable negative caching) |
| `MakeCacheKey(...)` | build keys in the service (`Config.Prefix` + key) |

Behaviour changes:

- **Stored format changed:** values are a one-byte tag plus codec bytes
  (JSON by default). Old JSON values are not readable; use a new `Prefix`
  (e.g. add `v2:`) so old keys expire on their own.
- Accepts `redis.UniversalClient` (Cluster and Sentinel work).
- `GetOrLoad` is fail-open: Redis errors fall back to the loader. `Get`, `Set`
  and `Delete` return Redis errors.
- TTLs get ±10 % jitter by default (`Jitter: -1` disables).

## uploader → blob + blob/gcs

| Old | New |
|---|---|
| `uploader.NewGCS(ctx, bucket, cdn, expiry, size, opts...)` / `MustNewGCS` / `MustRegisterGCS` | `gcs.Open(ctx, gcs.Config{Bucket, CDNBaseURL, SignExpiry, MaxUploadBytes}, opts...)` |
| `EngineV2` interface | `blob.Bucket` + `blob.Signer` (+ `(*gcs.Bucket).PublicURL`) |
| `CreateSignedUploadURL(ctx, f)` | `b.SignedURL(ctx, key, blob.SignOptions{Method: "PUT", ContentType: ct})` |
| `CreatePostPolicyUploadURL(ctx, f)` / V1 `CreateFileUploadURL` | `b.PostPolicy(ctx, key, blob.PostPolicyOptions{MaxBytes: n})` → `{URL, Fields}` |
| `CreateSignedDownloadURL(ctx, key, WithDownloadTTL, WithContentDisposition, WithResponseContentType)` | `b.SignedURL(ctx, key, blob.SignOptions{Expires, ResponseContentDisposition, ResponseContentType})` |
| `GetFileURL(f)` / `GetFileLink` | `b.PublicURL(key)` or `blob.PublicURL(cdnBase, key)` (escaped) |
| `UploadBytes(ctx, key, data)` / `UploadFile` | `blob.Put(ctx, b, key, bytes.NewReader(data), &blob.WriterOptions{ContentType: ct})` |
| `UploadStream(ctx, key, fn, WithContentType, WithChunkSize)` | `w, _ := b.NewWriter(ctx, key, &blob.WriterOptions{ContentType, ChunkSize})`; write; `w.Close()` (cancel ctx to abort) |
| `ObjectExists(ctx, key)` | `blob.Exists(ctx, b, key)` |
| `GenerateDefaultObjectKey(category, ext)` / `GenerateFileObjectKey(source, category, ext)` | `blob.NewObjectKey(ext, category)` / `blob.NewObjectKey(ext, source, category)` |
| `MimeTypeExtensions[mt]` | `blob.ExtensionByType(mt)` |
| `DetectMimeCategory` / `DetectSourceCategory` | `blob.Category(mt)` |
| `File` DTO, `KB`, `MB`, `DefaultMaxSizeBytes`, `DefaultSignedUrlExpiry` | removed: keep DTOs in the service; defaults live in `gcs.Config` (20 MiB, 5m) |
| V1 `Engine` / `GCloudStorage` | removed |

Behaviour changes: key layout is the same (`<prefix>/<YYYYMMDD UTC>/<xid><ext>`)
but uses `path.Join` on every OS; `image/jpeg` maps to `.jpg` (was `.jpeg`);
CDN URLs are escaped; `Close` closes the client created by `Open`; tests can use
`blob/memblob`.

## utils → stdlib, xslices, xmaps, timex

| Old | New |
|---|---|
| `FormatList(arr, fn)` | `xslices.Map(arr, fn)` (returns nil for empty input) |
| `Array2Map(arr, fn)` | `xslices.KeyBy(arr, fn)` (first wins, as before) |
| `UniqueArray(arr)` | `xslices.Uniq(arr)` — **now order-preserving** |
| `UniqueKeys(arr, fn)` | `xslices.Uniq(xslices.Map(arr, fn))` |
| `RemoveDuplicates(s)` | `xslices.Uniq(s)` |
| `DiffMaps(a, b)` | `xmaps.DiffFunc(a, b, func(x, y any) bool { return reflect.DeepEqual(x, y) })`; `xmaps.Diff` for comparable values |
| `IsContains(arr, v)` | `slices.Contains(arr, v)` |
| `Max` / `Min` (panicked on an empty slice) | builtin `max`/`min` for fixed arguments; for slices check `len(s) > 0` before `slices.Max` / `slices.Min` (they also panic on empty) |
| `Reverse` | `slices.Reverse` |
| `Filter(s, pred)` | `slices.DeleteFunc(slices.Clone(s), not(pred))` or a loop |
| `GetLastN(s, n)` | `s[max(0, len(s)-n):]` |
| `ChunkArray(s, n)` (first n) | `s[:min(n, len(s))]` |
| `Coalesce(vs...)` | `cmp.Or(vs...)` |
| `Ternary`, `IsZero`, `Sum` | write inline |
| `MustParseTime(s)` (zero on failure) | `timex.Parse(s) (time.Time, error)` or `timex.MustParse(s)` (panics) |
| `StartOfDay`, `EndOfDay`, `StartOfMonth`, `EndOfMonth`, `StartOfYear`, `EndOfYear` | same names in `timex` — **keep the input's location** (old ones forced UTC); `End*` is the last nanosecond (old `EndOfDay` was the last second) |
| `RangeDates(from, to)` | `for d := range timex.Days(from, to)` |
| `NowVN()` / `VietnamLocation` | `loc, err := time.LoadLocation("Asia/Ho_Chi_Minh")` in the service (+ `import _ "time/tzdata"` on minimal images); `time.Now().In(loc)` |
| `IsFuture`, `IsPast`, `DurationBetween` | `t.After(time.Now())`, `t.Before(time.Now())`, `t2.Sub(t1)` |
| `ParseTemplate` | `text/template` (or `html/template` for HTML) in the service |
| Phone, NID, email masking, VND currency, accent removal, URL validators | **removed** (no users); copy from the old library if needed |

Mechanical rewrites (run `goimports` afterwards):

```bash
gofmt -r 'kutils.FormatList(a, b) -> xslices.Map(a, b)' -w .
gofmt -r 'kutils.Array2Map(a, b) -> xslices.KeyBy(a, b)' -w .
gofmt -r 'kutils.UniqueArray(a) -> xslices.Uniq(a)' -w .
gofmt -r 'kutils.IsContains(a, b) -> slices.Contains(a, b)' -w .
```

`MustParseTime` callers must handle the error now (the old function silently
returned the zero time).

## parallel → errgroup

`parallel.RunWithConcurrency(ctx, tasks, n)` is removed (it dropped errors and
deadlocked for n ≤ 0). Use:

```go
g, ctx := errgroup.WithContext(ctx)
g.SetLimit(n)
out := make([]T, len(tasks))
for i, task := range tasks {
	g.Go(func() error {
		v, err := task(ctx)
		out[i] = v
		return err
	})
}
err := g.Wait()
```

## crawl → webmeta

| Old | New |
|---|---|
| `crawl.NewWorker(httpClient).Crawl(url)` | `f := webmeta.New(webmeta.Config{}); defer f.Close(); m, err := f.Fetch(ctx, url)` |
| `WebsiteMetadata{Title, Logo, Description}` | `Metadata{Title, Description, Image, Icon, SiteName, CanonicalURL, URL}`; `Logo` ≈ `Icon` (or `Image` for a preview picture) |

Behaviour changes:

- Private, loopback, link-local and metadata addresses are refused
  (`ErrForbiddenAddress`), including after redirects and DNS rebinding.
  `Config.AllowPrivate` turns this off for trusted targets and tests.
- Non-2xx responses return `*webmeta.StatusError`; non-HTML responses return
  `ErrNotHTML` (the old code returned an empty result with nil error for
  some file extensions).
- The body is capped (2 MiB default), the fetch has a 10 s timeout, a User-Agent
  is sent and charsets are decoded.
- The caller's `*http.Client` is no longer used; pass `Config.Transport` to
  customise transport (then the address guard is the caller's job).
