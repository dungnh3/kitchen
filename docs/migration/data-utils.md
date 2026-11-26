# Migration: data-utils

From the old library's `cache`, `storage`, `migrations`, `uploader`, `utils`,
`parallel` and `crawl` packages to `cache`, `redisx`, `pgx`, `pgx/gorm`,
`migrate`, `objstore`, `objstore/gcs`, `objstore/s3`, `timex` and `webmeta`; slice and map helpers move to [`github.com/samber/lo`](https://github.com/samber/lo) (owner, 2026-11-21: reuse a mature library instead of kitchen's own `xslices`/`xmaps`).
Design and rationale: [`docs/design/data-utils.md`](../design/data-utils.md).

Module paths (D1):

| Import | Module |
|---|---|
| `github.com/dungnh3/kitchen/{cache,redisx,pgx,mysqlx,objstore,objstore/memstore,timex,webmeta}` | core `github.com/dungnh3/kitchen` |
| `github.com/dungnh3/kitchen/pgx/gorm` (package `pgxgorm`) | submodule |
| `github.com/dungnh3/kitchen/mysqlx/gorm` (package `mysqlgorm`) | submodule |
| `github.com/dungnh3/kitchen/mongox` | submodule |
| `github.com/dungnh3/kitchen/migrate` (+ `cmd/kmigrate`) | submodule |
| `github.com/dungnh3/kitchen/objstore/gcs`, `github.com/dungnh3/kitchen/objstore/s3` | submodules |

## Old bugs fixed (behaviour changes)

Every defect found in the review of the old packages is fixed or removed with
the API that had it. None is carried over.

| Old package | Old bug | Now |
|---|---|---|
| migrations | `Down` reverted **all** migrations (same as `DownAll`) | `Down(ctx, n)` reverts exactly n |
| migrations | `Down`/`DownAll`/`Steps` closed the caller's DB pool and `log.Panicf`'d on close errors | `Close` never touches the caller's pool; errors are returned |
| migrations | `Up` never closed the migrator and pinned a pool connection forever | `Close` releases it |
| migrations | `file://` only, files had to be on disk at `/migrations` | any `fs.FS` (`embed.FS`) |
| migrations | used `lib/pq`, broken go.mod (missing/indirect deps); Postgres only | goose v3 on pgx/v5 or mysqlx, tidy go.mod; Postgres and MySQL |
| migrations | `ErrNoChange` surfaced as an error, dirty state unclear | nothing pending is nil; each migration in a transaction (goose), so no dirty state |
| cache | `RemoveByPattern` used blocking `KEYS` and ignored the iterator error | `DeleteByPrefix`: SCAN + UNLINK, glob-escaped, errors returned |
| cache | only `*redis.Client` (no Cluster/Sentinel) | `redis.UniversalClient` |
| cache | no stampede protection, no local tier | singleflight `GetOrLoad`, optional L1 |
| cache | `HGetAll` re-encoded JSON lossily (numbers → float64) | removed (use go-redis hashes directly) |
| cache | `Increase` set no TTL | removed (use go-redis `Incr` + `Expire`) |
| cache | go-redis marked `// indirect`, no tests | tidy, miniredis + synctest tests |
| storage | Postgres DSN hardcoded `sslmode=disable` | `SSLMode` default `require`, configurable |
| storage | DSN hardcoded `TimeZone=Asia/Ho_Chi_Minh` | no default; `RuntimeParams["timezone"]` |
| storage | password not escaped (spaces/quotes broke the DSN) | built with `url.UserPassword` |
| storage | no pool settings | MaxConns, lifetimes, idle time, health check, timeouts |
| storage | package named `config` in dir `storage`; `GormLogLevel` duplicated | packages `pgx`, `pgxgorm`, `redisx` |
| storage | MySQL `Loc *time.Location` not loadable from config; `GoogleAuthFile` ignored; plaintext by default; no pool limits | `mysqlx`: `Loc` default UTC, TLS required by default, pool defaults, otelsql tracing |
| uploader | V1 `CreateFileUploadURL` ignored the per-file size limit | `PostPolicyOptions.MaxBytes` per call, default from config |
| uploader | V1 constructor called `log.Fatal` | constructors return errors |
| uploader | `DetectSourceCategory` compared prefixes backwards and depended on map order | `objstore.Category` parses the MIME type |
| uploader | object keys used `filepath.Join` (backslashes on Windows) | `path.Join` |
| uploader | CDN URL built with `Sprintf`, no escaping | `objstore.PublicURL` escapes each segment |
| uploader | `UploadBytes` forced `ChunkSize=0`, disabling retries | `objstore.Put` keeps the client's resumable default; `ChunkSize` is opt-in |
| uploader | no `Close` for the client | `Close` closes the client created by `gcs.Open` |
| uploader | `UploadStream` recovered panics and never closed/aborted the writer | `objstore.Put` cancels and closes on error; no panic swallowing |
| uploader | `pkg/errors`, no tests, no fake | stdlib errors, fake-gcs-server tests, `memstore` |
| utils | `IsContains` used reflection + `DeepEqual` | `slices.Contains` |
| utils | `Max`/`Min` panicked on empty input | removed; see table above |
| utils | `UniqueArray` returned random order | `lo.Uniq` keeps first-seen order |
| utils | `Sum` accepted strings via `constraints.Ordered` | removed |
| utils | `ChunkArray` actually meant "first N" | removed; slice expression |
| utils | regexes recompiled on every call | removed with the VN helpers; `timex` uses no regex |
| utils | `MustParseTime` ran all patterns without early return and hid errors | `timex.Parse` returns the first match or an error |
| utils | `StartOfDay`/`StartOfMonth`/`EndOfMonth` forced UTC, dropping the location | `timex` keeps `t.Location()` |
| utils | `EndOfDay` truncated to the second | last nanosecond of the day |
| utils | `VietnamLocation` silently nil without tzdata | removed; `time.LoadLocation` returns the error |
| utils | `ParseTemplate` used `html/template` for text (escaped output), sprig v2 `+incompatible` | removed |
| parallel | dropped task errors, lost result order, deadlocked for n ≤ 0, ignored ctx, leaked goroutines | removed; `errgroup.SetLimit` recipes, `webmeta.FetchAll` for URL crawling |
| crawl | no `context` | `Fetch(ctx, url)` |
| crawl | no status or Content-Type check | errs status mapping via httpx, `ErrNotHTML` |
| crawl | unbounded body | `io.LimitReader` (2 MiB default) |
| crawl | no User-Agent, no charset decoding | UA header; `x/net/html/charset` |
| crawl | SSRF: LLM-supplied URLs could reach private/metadata addresses | dial-time guard after DNS and on redirects |
| crawl | logo chosen by document order | fixed priority (og:image > twitter:image; apple-touch-icon > icon > /favicon.ico) |
| crawl | skipped extensions returned an empty result with nil error | non-HTML → `ErrNotHTML` |

## storage → pgx, pgx/gorm, redisx

| Old | New |
|---|---|
| `config.Postgres{...}.FormatDSN()` + `gorm.Open(postgres.Open(dsn))` | `pool, err := pgx.Open(ctx, pgx.Config{...})` then `pgxgorm.Open(pool, pgxgorm.Options{Log: &gormlog.Config{Logger: log}})` |
| `config.Postgres.GormLogLevel` | `pgxgorm.Options{Log: &gormlog.Config{Level: logger.Warn, SlowThreshold: ...}}` (kitchen `log`, `log/gormlog`) |
| `config.Redis{...}` + `redis.NewClient` | `rdb, err := redisx.Open(ctx, redisx.Config{Addrs: ..., Password: ...})` |
| `config.MySQL{...}.FormatDSN()` + `gorm.Open(mysql.Open(dsn))` | `db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: dsn})` (or `Addr`/`User`/...) then `mysqlgorm.Open(db, mysqlgorm.Options{})`. `ParseTime` is always on; TLS defaults to `skip-verify` — add `?tls=false` / `TLSMode: mysqlx.TLSDisable` for a server without TLS. |
| hand-written `mongo.Connect` (driver v1) | `mongox.Open(ctx, mongox.Config{URI: uri})` on driver **v2** (see the driver's v1→v2 guide); errors via `mongox.MapError` |

Behaviour changes:

- **TLS:** the old DSN forced `sslmode=disable`. `pgx.Config.SSLMode` defaults
  to `require` (TLS always, never a plaintext fallback). A server without TLS
  (local Docker) fails to connect until you set `SSLMode: "disable"` (or
  `?sslmode=disable` in `URL`). A `URL` without `sslmode` also gets
  `require` (unless `PGSSLMODE` is set).
- **URL + fields:** with `URL`, only `User`, `Password` and `Database` may
  override it; `Host`, `Port` or `SSLMode` next to `URL` is an error.
- **Errors:** wrap repository errors with `pgx.MapError(err)` to get `errs`
  codes (`ErrNotFound`, `ErrAlreadyExists`, `ErrAborted`, ...) instead of
  hand-written `jpgx.ErrNoRows`/`23505` checks.
- **Readiness:** `server.Check{Name: "postgres", Func: pool.Ping}`.
- **Time zone:** the old DSN forced `TimeZone=Asia/Ho_Chi_Minh`. `pgx` sets no
  time zone. To keep it: `RuntimeParams: map[string]string{"timezone": "Asia/Ho_Chi_Minh"}`.
- **Password escaping:** passwords with spaces, quotes or `@` now work.
- **Pool:** defaults are MaxConnLifetime 30m (+5m jitter), MaxConnIdleTime 5m,
  HealthCheckPeriod 30s, ConnectTimeout 5s, MaxConns = pgx default. `Open`
  pings before returning and returns errors instead of calling `log.Fatal`.
- **Behind PgBouncer (transaction mode):** set `QueryExecMode: pgx.ExecModeExec`
  (or `ExecModeSimpleProtocol`); the default caches prepared statements.
- gorm and pgx share one pool, so `MaxConns` is the only connection limit.
  Tracing comes from otelpgx on the pool; drop any gorm OTel plugin.
- `Redis` with several addresses becomes a Cluster client; `MasterName`
  selects Sentinel.

## migrations → migrate

The engine changed from golang-migrate to **goose v3** (owner, 2026-11-21):
each migration runs in a transaction, so there is no dirty state any more.

| Old | New |
|---|---|
| `migrations.Up(db *gorm.DB, &Options{...})` | `m, err := migrate.NewPostgres(pool, migrate.Config{Source: embedFS, Dir: "sql"}); defer m.Close(); err = m.Up(ctx)` (MySQL: `migrate.NewMySQL(db, ...)`) |
| `migrations.Down(db, opts)` | `m.Down(ctx, 1)` — **reverts one migration** |
| `migrations.DownAll(db, opts)` | `m.To(ctx, 0)` |
| `migrations.Steps(db, opts)` with n>0 / n<0 | `m.To(ctx, version)` / `m.Down(ctx, n)` |
| golang-migrate CLI | `go run github.com/dungnh3/kitchen/migrate/cmd/kmigrate -database "$DATABASE_URL" -path ./sql up` (`-dialect mysql` for MySQL); `kmigrate create NAME` writes a template |
| `*DirtyError`, `Force` | gone: a failed migration is rolled back |

Behaviour changes:

- **Old `Down` reverted every migration** (it called golang-migrate's
  all-down). New `Down(ctx, n)` reverts exactly `n`.
- **Old `Down`/`DownAll`/`Steps` closed the caller's DB pool** and panicked on
  close errors. `Migrator.Close` never closes a pool or `*sql.DB` passed in.
- Migrations are read from any `fs.FS` (use `embed.FS`).
- Concurrent runs wait on a lock for `LockTimeout` (default 1m): a Postgres
  advisory lock, or the `goose_lock` table on MySQL.
- `Open`/`kmigrate` connect with `pgx.Open`/`mysqlx.Open`, so TLS is required
  unless the DSN disables it (`sslmode=disable`, `tls=false`).
- Each migration runs in a transaction: **remove explicit `BEGIN`/`COMMIT`**
  from files, and mark statements that cannot run in a transaction
  (`CREATE INDEX CONCURRENTLY`) with `-- +goose NO TRANSACTION`. Wrap
  multi-statement bodies such as `$$` functions in
  `-- +goose StatementBegin` / `-- +goose StatementEnd`. MySQL DDL commits
  implicitly, so a failed MySQL DDL migration is not rolled back.

### One-time conversion from golang-migrate

1. Files: merge each `.up.sql`/`.down.sql` pair into one goose file.

   ```bash
   for up in *.up.sql; do
     base=${up%.up.sql}
     { echo '-- +goose Up'; cat "$up"; echo; echo '-- +goose Down'; cat "$base.down.sql" 2>/dev/null; } > "$base.sql"
     rm "$up" "$base.down.sql"
   done
   ```

2. State: make sure `schema_migrations.dirty` is false, let goose create its
   table (`kmigrate -database "$DATABASE_URL" -path ./sql version`), then mark
   every file up to the recorded version as applied (Postgres shown; the
   `VALUES` list is the file versions):

   ```sql
   INSERT INTO goose_db_version (version_id, is_applied)
   SELECT v, true FROM (VALUES (20250304001122), (20250304100000)) AS f(v)
   WHERE v <= (SELECT version FROM schema_migrations WHERE NOT dirty)
   ORDER BY v;
   ```

3. Check with `kmigrate status`, then drop `schema_migrations`.

## cache → cache

| Old | New |
|---|---|
| `cache.NewRedisCache(rdb)` (`Cache` of `interface{}`) | `cache.New(cache.Config[T]{Redis: rdb, Prefix: "x:", TTL: d})` (typed) |
| `Get(key, &v)` / `Set(key, ttl, v)` | `c.Get(ctx, key) (T, error)` / `c.Set(ctx, key, v)` (TTL from config) or `c.Set(ctx, key, v, cache.WithTTL(ttl))` |
| get, miss → load → set | `c.GetOrLoad(ctx, key, loader)` (singleflight, optional L1) |
| `Del(keys...)` | `c.Delete(ctx, keys...)` |
| `RemoveByPattern(pattern)` | `c.DeleteByPrefix(ctx, prefix)` (SCAN + UNLINK, Cluster-aware) |
| `SetNX` used as a lock | `lease.Do(ctx, locker, name, ttl, fn)` (renewed, owner-checked, fencing token) with `lease/redis` |
| `Exists(key)` | `_, err := c.Get(ctx, key)`; `errors.Is(err, cache.ErrNotFound)` |
| `Incr` (counters, limits) | `ratelimit`, or go-redis directly |
| `Expire`, `HGet/HSet/HGetAll`, ... | go-redis directly (`rdb.Expire(ctx, ...)`) |
| `cache.ErrNotFound` | `cache.ErrNotFound`, now `errs.ErrNotFound`: any errs NotFound returned by a loader enables negative caching |
| `MakeCacheKey(...)` | build keys in the service (`Config.Prefix` + key) |
| package logger (`xlog`, zap) | `Config.Logger *log.Logger` (kitchen/log; nil = `log.Default()`); `lease.WithLogger` / `lease.DoLogger` likewise |

Behaviour changes:

- **Stored format changed:** values are a one-byte tag plus codec bytes
  (JSON by default). Old JSON values are not readable; use a new `Prefix`
  (e.g. add `v2:`) so old keys expire on their own.
- Accepts `redis.UniversalClient` (Cluster and Sentinel work).
- `GetOrLoad` is fail-open: Redis errors fall back to the loader. `Get`, `Set`
  and `Delete` return Redis errors.
- TTLs get ±10 % jitter by default (`Jitter: -1` disables).
- Loaders are bounded by `LoadTimeout` (default 30s) and a panic becomes a
  `*cache.PanicError` instead of crashing the process.
- L1 values are shared: set `Config.Clone` for pointer, slice or map values.
- `Local.RefreshAfter` refreshes hot keys in the background.

Changes from the deep review (2026-11-24), for code written against the
first kitchen `cache`:

- **`Close(ctx) error`** replaces `Close()`. It cancels running loads, waits
  for them until `ctx` ends, and is idempotent. Loading methods return
  `cache.ErrClosed` afterwards. Write `defer func() { _ = c.Close(ctx) }()`.
- **`Name` is required when `Prefix` is empty** (L1-only caches): metrics and
  logs need a name.
- **Redis format:** a key being loaded holds a lease (tag `l`). Readers treat
  it as a miss. Deploys that mix the first version with this one read the
  lease as an unreadable entry; use a new `Prefix` if both run at once.
- **Write-backs are conditional:** a load writes Redis only if the key still
  holds what the load saw, so `Set`/`Delete` during a load win on every pod.
- **`RedisTimeout`** (default 500ms) bounds each Redis command; after 5
  consecutive failures `GetOrLoad`/`GetOrLoadMany` skip Redis for 5s.
- **Log schema:** cache warnings carry `cache.key_hash` instead of `key`, and
  error strings no longer contain keys. Warnings are throttled (one per kind
  per 10s, with `suppressed`). otter's refresh errors are logged through
  `Config.Logger` ("cache: refresh failed", attribute `detail`).
- **Metrics:** `cache.result="skipped"` for `l2`, `"not_found"` for
  `loader` (before: counted as `error`), new histogram
  `kitchen.cache.load.duration`.
- **`GetMany`** now returns Redis errors and unreadable entries for every
  key (joined), not only the first, along with the values it read.
- L1 TTLs are jittered downwards (between `Local.TTL×(1−Jitter)` and
  `Local.TTL`); values promoted from Redis keep their remaining Redis TTL as
  an upper bound in L1. `LocalConfig.MaxWeight` + `Config.Weigher` bound L1
  by weight.

## uploader → objstore + objstore/gcs (+ objstore/s3)

| Old | New |
|---|---|
| `uploader.NewGCS(ctx, bucket, cdn, expiry, size, opts...)` / `MustNewGCS` / `MustRegisterGCS` | `gcs.Open(ctx, gcs.Config{Bucket, CDNBaseURL, SignExpiry, MaxUploadBytes}, opts...)` |
| `EngineV2` interface | `objstore.Bucket` + `objstore.Signer` (+ `(*gcs.Bucket).PublicURL`) |
| `CreateSignedUploadURL(ctx, f)` / V1 `CreateFileUploadURLNoLimit` | `b.SignedURL(ctx, key, objstore.SignOptions{Method: "PUT", ContentType: ct, MaxBytes: n})` — the client must send the signed `x-goog-content-length-range` and `x-goog-if-generation-match` headers; on S3 prefer `PostPolicy` |
| `CreatePostPolicyUploadURL(ctx, f)` / V1 `CreateFileUploadURL` | `b.PostPolicy(ctx, key, objstore.PostPolicyOptions{MaxBytes: n, ContentTypePrefix: "image/"})` → `{URL, Fields}` |
| `CreateSignedDownloadURL(ctx, key, WithDownloadTTL, WithContentDisposition, WithResponseContentType)` | `b.SignedURL(ctx, key, objstore.SignOptions{Expires, ResponseContentDisposition, ResponseContentType})` |
| `GetFileURL(f)` / `GetFileLink` | `b.PublicURL(key)` or `objstore.PublicURL(cdnBase, key)` (escaped) |
| `UploadBytes(ctx, key, data)` / `UploadFile` | `objstore.Put(ctx, b, key, bytes.NewReader(data), &objstore.WriterOptions{ContentType: ct})` |
| `UploadStream(ctx, key, fn, WithContentType, WithChunkSize)` | `w, _ := b.NewWriter(ctx, key, &objstore.WriterOptions{ContentType, ChunkSize})`; write; `w.Close()` (cancel ctx to abort) |
| `ObjectExists(ctx, key)` | `objstore.Exists(ctx, b, key)` |
| `GenerateDefaultObjectKey(category, ext)` / `GenerateFileObjectKey(source, category, ext)` | `objstore.NewObjectKey(ext, category)` / `objstore.NewObjectKey(ext, source, category)` |
| `MimeTypeExtensions[mt]` | `objstore.ExtensionByType(mt)` |
| `DetectMimeCategory` / `DetectSourceCategory` | `objstore.Category(mt)` |
| `File` DTO, `KB`, `MB`, `DefaultMaxSizeBytes`, `DefaultSignedUrlExpiry` | removed: keep DTOs in the service; defaults live in `gcs.Config` (20 MiB, 5m) |
| V1 `Engine` / `GCloudStorage` | removed |

Behaviour changes (2026-11-21 review):

- **Package renamed** `blob` → `objstore` (`memblob` → `memstore`).
- **Uploads are fail-closed:** a PUT URL needs `ContentType` and is bounded
  in size and create-only; a POST policy needs `ContentType` or
  `ContentTypePrefix`. Opt out with `AllowAnyContentType`,
  `AllowOverwrite`, `AllowUnboundedSize` (S3 PUT). Old URLs allowed any
  type and size (stored XSS / huge files on the CDN domain).
- Errors are `errs` errors: a missing object is NotFound (404), an
  `IfNotExists` conflict AlreadyExists (409), a bad key InvalidArgument.
- Every operation validates the key (`objstore.ValidKey`): no `..`, `//`,
  control characters or leading `/`.
- `objstore.Instrument(b)` adds tracing.
- New `objstore/s3` for AWS S3 and compatible stores.

Key layout is the same (`<prefix>/<YYYYMMDD UTC>/<xid><ext>`)
but uses `path.Join` on every OS; `image/jpeg` maps to `.jpg` (was `.jpeg`);
CDN URLs are escaped; `Close` closes the client created by `Open`; tests can use
`objstore/memstore`.

## utils → stdlib, samber/lo, timex

| Old | New |
|---|---|
| `FormatList(arr, fn)` | `lo.Map(arr, func(v V, _ int) T { return fn(v) })` (lo passes the index) |
| `Array2Map(arr, fn)` | `lo.KeyBy(lo.UniqBy(arr, fn), fn)` keeps the first element per key as before; plain `lo.KeyBy` keeps the **last** |
| `UniqueArray(arr)` | `lo.Uniq(arr)` — **now order-preserving** |
| `UniqueKeys(arr, fn)` | `lo.UniqMap(arr, func(v V, _ int) K { return fn(v) })` |
| `RemoveDuplicates(s)` | `lo.Uniq(s)` |
| `DiffMaps(a, b)` | `lo.OmitBy(a, func(k string, v any) bool { bv, ok := b[k]; return ok && reflect.DeepEqual(v, bv) })` |
| `IsContains(arr, v)` | `slices.Contains(arr, v)` |
| `Max` / `Min` (panicked on an empty slice) | builtin `max`/`min` for fixed arguments; for slices check `len(s) > 0` before `slices.Max` / `slices.Min` (they also panic on empty) |
| `Reverse` | `slices.Reverse` |
| `Filter(s, pred)` | `lo.Filter(s, func(v T, _ int) bool { return pred(v) })` (input not modified) |
| `GetLastN(s, n)` | `s[max(0, len(s)-n):]` |
| `ChunkArray(s, n)` (first n) | `s[:min(n, len(s))]` |
| `Coalesce(vs...)` | `cmp.Or(vs...)` |
| `Ternary`, `IsZero`, `Sum` | write inline |
| `MustParseTime(s)` (zero on failure) | `timex.Parse(s) (time.Time, error)` or `timex.MustParse(s)` (panics); an empty string returns `timex.ErrEmpty`, an errs `EMPTY_TIME` (InvalidArgument) |
| `StartOfDay`, `EndOfDay`, `StartOfMonth`, `EndOfMonth`, `StartOfYear`, `EndOfYear` | same names in `timex` — **keep the input's location** (old ones forced UTC); `End*` is the last nanosecond (old `EndOfDay` was the last second) |
| `RangeDates(from, to)` | `for d := range timex.Days(from, to)` |
| `NowVN()` / `VietnamLocation` | `loc, err := time.LoadLocation("Asia/Ho_Chi_Minh")` in the service (+ `import _ "time/tzdata"` on minimal images); `time.Now().In(loc)` |
| `IsFuture`, `IsPast`, `DurationBetween` | `t.After(time.Now())`, `t.Before(time.Now())`, `t2.Sub(t1)` |
| `ParseTemplate` | `text/template` in the service, parsed once at start-up (`html/template` only for HTML: the old helper used it for LLM prompts and escaped their quotes) |
| Phone, NID, email masking, VND currency, accent removal, URL validators | **removed** (no users); copy from the old library if needed |

Mechanical rewrites (run `goimports` afterwards):

```bash
gofmt -r 'kutils.UniqueArray(a) -> lo.Uniq(a)' -w .
gofmt -r 'kutils.IsContains(a, b) -> slices.Contains(a, b)' -w .
```

`MustParseTime` callers must handle the error now (the old function silently
returned the zero time).

## Logging (owner, 2026-11-22)

`pgx`, `mysqlx`, `mongox`, `migrate` and `pgxgorm`/`mysqlgorm` take a
`*log.Logger` from `github.com/dungnh3/kitchen/log` (an alias of
`*slog.Logger`); nil means `log.Default()`, the process logger `main`
installs with `log.Setup`. They no longer fall back to `slog.Default()`.
`kmigrate` logs to stderr (text unless `LOG_FORMAT` is set).

## parallel → errgroup / webmeta.FetchAll

`parallel.RunWithConcurrency(ctx, tasks, n)` is removed. It dropped failed
tasks silently, lost result order, deadlocked for n ≤ 0, kept starting tasks
after ctx was done and left goroutines running after it returned.

- **Crawling URLs** (its only use): `f.FetchAll(ctx, urls, n)` returns one
  `webmeta.Result{URL, Metadata, Err}` per URL in input order; failed URLs do
  not stop the others.
- **Best effort** (the old semantics: keep the successes):

  ```go
  var g errgroup.Group
  g.SetLimit(n)
  out := make([]T, len(tasks))
  errs := make([]error, len(tasks))
  for i, task := range tasks {
  	g.Go(func() error { out[i], errs[i] = task(ctx); return nil })
  }
  _ = g.Wait()
  // out[i] is valid where errs[i] == nil
  ```

- **Fail fast** (first error cancels the rest):

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
| `crawl.NewWorker(httpClient).Crawl(url)` | `f, err := webmeta.New(webmeta.Config{}); defer f.Close(); m, err := f.Fetch(ctx, url)` (build one Fetcher and share it) |
| `parallel.RunWithConcurrency` over `Crawl` calls | `f.FetchAll(ctx, urls, 5)` |
| `WebsiteMetadata{Title, Logo, Description}` | `Metadata{Title, Description, Image, Icon, SiteName, CanonicalURL, URL}`; `Logo` ≈ `Icon`, or `Image` for a preview picture (`ImageUrl`) |

Behaviour changes:

- Fetching goes through `httpx` with its SSRF guard: private, loopback,
  link-local and metadata addresses and ports other than 80/443 are refused
  (`httpx.ErrDestinationNotAllowed`), including after redirects and DNS
  rebinding; https is never downgraded to http on a redirect. `Config.Allow`
  and `Config.Ports` open trusted internal targets and tests.
- Errors are `errs` errors: non-2xx responses follow httpx's mapping (404 →
  `errs.ErrNotFound`; `httpx.StatusCode(err)` gives the status); non-HTML
  responses return `webmeta.ErrNotHTML` (the old code returned an empty result
  with nil error for some file extensions); bad URLs `webmeta.ErrInvalidURL`.
- The body is capped (2 MiB default), the fetch has a 10 s timeout, a User-Agent
  is sent and charsets are decoded.
- The description falls back to the first body paragraph of ≥30 characters,
  like the old crawl (now cut to 300 characters, script/style skipped).
- The caller's `*http.Client` is no longer used; pass httpx options
  (`Config.ClientOptions`) for logging, tracing or retries.
