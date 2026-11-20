# Review: cache, storage, migrations, uploader, utils, parallel, crawl

Root: `/Users/dungnh/works/sources/github.com/vulcan/kitchen`. I read every source file in scope and changed nothing.

## Downstream facts (grep of `../*/go.mod` and `.go` files)
- **Databases:** Postgres is the de-facto standard. `gorm.io/driver/postgres` appears in 61 go.mods and `jackc/pgx` in 61. MySQL is a minority: `gorm.io/driver/mysql` in 4, `go-sql-driver/mysql` in 7. No sqlc config files exist; the 13 "sqlc" hits are only `sqlclosecheck`. Nothing uses goose or atlas.
- **Concurrency libs:** `sourcegraph/conc` is in 64 go.mods and `golang.org/x/sync` in 75. `parallel` has 0 users.
- **Version pins are fragmented:** cache is pinned at 6 different pseudo-versions, uploader 11, utils 10.
- **utils usage is narrow.** By real call count: `FormatList` 88, `MustParseTime` 22, `IsContains` 21, `DiffMaps` 8, `UniqueArray` 4, `ParseTemplate` 3, `Array2Map` 3, `UniqueKeys` 1, `GetLastN` 1. Eight repos import it only as `_` from `tools.go`.
- **cache usage is minimal.** `NewRedisCache` 25, `SetNX` 5, `HGet`/`HSet`/`Increase` 1 each. `RemoveByPattern` and `HGetAll` have no users.
- **crawl** has 3 call sites, all `crawl.NewWorker(httpClient).Crawl(url)`, in ms-smith-engine and ms-smith-nexus.
- **migrations:** only `migrations.Up` is called, in ms-products, ms-payments and ms-quota-orchestrator.

## cache (`cache/`)
**API:** a `Cache` interface (`cache.go:8-23`) of `interface{}` methods: Set, Get, Incr, Expire, SetNX, Del, Exists, RemoveByPattern, and H* hash ops. It is implemented only over Redis with JSON encoding (`redis.go`). Also `ErrNotFound` and `MakeCacheKey`.

**Issues:**
- `RemoveByPattern` uses `KEYS` (`redis.go:75`), which is O(N) and blocks Redis. It also ignores `keys.Err()`.
- It takes a concrete `*redis.Client` (`:19`) instead of `redis.UniversalClient`, so Redis Cluster is not supported.
- No generics, no local L1 tier, no stampede protection (singleflight / get-or-load), no tracing.
- `HGetAll` decodes and re-encodes JSON (`:122-150`), which is lossy (numbers become float64).
- `Increase` sets no TTL.
- go.mod declares go-redis as `// indirect` (`go.mod:8`), so the file is stale and untidy. There are no tests.

**Verdict: REDESIGN.** Sketch:
`Cache[K,V]` with `Get`/`Set`/`GetOrLoad(ctx, k, loader, ttl)` (singleflight), a pluggable `Codec`, an optional L1 (otter v2) over L2 (go-redis `UniversalClient`), and `SCAN`-based invalidation. Keep the raw Redis ops out of it, since callers can use go-redis directly.

## storage (`storage/`)
**API:** config structs only, `MySQL`/`Postgres`/`Redis`, with `FormatDSN` and `GormLogLevel`. It opens no connections. The package is named `config` but lives in the `storage` directory (`mysql.go:1`).

**Issues:**
- The Postgres DSN hardcodes `sslmode=disable` and `TimeZone=Asia/Ho_Chi_Minh` (`postgres.go:19`), and does not escape the password, so spaces or quotes break it.
- No pool settings (max conns, lifetimes).
- `MySQL.Loc *time.Location` cannot come from YAML or mapstructure (`mysql.go:18`).
- `GormLogLevel` is duplicated between the two files.
- The MySQL config ignores `GoogleAuthFile` (`:33`).

**Deps:** gorm v1.25.11 vs latest v1.31.2; mysql driver v1.8.1 vs v1.10.2. Source: proxy.golang.org `@latest`.

**Verdict: REDESIGN.** Given Postgres dominance:
- `pg.Config` (URL-or-fields, TLS, pool, timeouts) → `pg.Open(ctx, cfg) (*pgxpool.Pool, error)` with OTel tracer and health check.
- An optional `pg.Gorm(pool)` adapter for legacy users.
- Drop MySQL unless the owner confirms a live MySQL user.
- pgx/v5 latest is v5.13.0; gorm postgres driver latest is v1.6.3.

## migrations (`migrations/`)
**API:** `Up`/`Down`/`DownAll`/`Steps(*gorm.DB, *Options)` over golang-migrate with the postgres driver and a `file://` source.

**Bugs:**
1. **`Down` reverts *all* migrations, not one** (`migration.go:122`). `instance.Down()` means "all down" in golang-migrate, so `Down` is identical to `DownAll`. The "Fixed bug" comment at `:121` is wrong.
2. **`Down`/`DownAll`/`Steps` close the caller's DB pool** (`defer instance.Close()` at `:115`, `:151`, `:187`). `postgres.WithInstance` sets `px.db = instance`, and `Close()` calls `p.db.Close()`. I verified this in `~/go/pkg/mod/.../migrate/v4@v4.18.3/database/postgres/postgres.go:149,220-225`. A Close error there is then turned into `log.Panicf`.
3. **`Up` never calls Close** (`:56-87`). It permanently pins one `sql.Conn` from the pool. That is harmless at startup but is a leak.
4. The `file://` source requires migration files on disk at `/migrations` by default; there is no `embed.FS` (`iofs`) support.
5. go.mod is broken or untidy: everything is marked indirect, and `lib/pq` and `pkg/errors` are missing from it (`go.mod:5-14`). It also uses `lib/pq` even though consumers use pgx.

No tests. migrate is at v4.18.3 vs latest v4.20.1.

**Verdict: REDESIGN.** Use goose v3 (v3.30.1) `goose.NewProvider(dialect, *sql.DB, fs.FS)` with `embed.FS`, run from a `*sql.DB` or pgx stdlib, never closing the caller's DB, plus an `Up(ctx)` / `Status` / `DownTo` API. Atlas is an option if declarative schemas are wanted. Keeping golang-migrate is acceptable if the history-table format must be preserved; that is an open question.

## uploader (`uploader/`)
**API:** a legacy `Engine` + `GCloudStorage`, and `EngineV2` + `GCloudStorageV2` (signed PUT URL, POST-policy URL, signed GET URL with options, `UploadBytes`, `UploadStream`, `ObjectExists`). Also `File` DTO, a MIME map, and object-key generators using xid.

**Issues:**
- V1 `CreateFileUploadURL` applies the engine limit only when `file.MaxSizeBytes != 0` and never uses the per-file value (`gcloud_storage.go:91-92`). V2 fixes this.
- V1 calls `log.Fatal` in its constructor (`:37,40,52`).
- `DetectSourceCategory` has reversed `HasPrefix(key, mimetype)` arguments and depends on map iteration order (`structz.go:76-83`), so `""` matches anything at random.
- Object keys use `filepath.Join` instead of `path.Join` (`structz.go:106,115`), which is OS-dependent.
- The CDN URL is built by `Sprintf` without escaping (`gcloud_storage_v2.go:151`).
- `ChunkSize=0` disables retries (`:172`).
- There is no `Close()` for the client.
- `UploadStream` recovers panics but never closes the writer; it relies on context cancellation (`:249-272`).
- V1 uses pkg/errors (`gcloud_storage.go:12`).
- No tests. It is hard-wired to GCS with no fake.

**Deps:**
| Module | Used | Latest |
|---|---|---|
| cloud.google.com/go/storage | v1.61.3 | v1.71.0 |
| google.golang.org/api | v0.273.0 | v0.316.0 |
| rs/xid | v1.6.0 | v1.6.0 (current) |
| pkg/errors | v0.9.1 | archived |

**Verdict: UPDATE → REDESIGN-lite.** Keep only the V2 surface. Split it into:
- a backend-neutral `blob.Bucket`, via gocloud.dev/blob (latest v0.49.0) or a thin interface with a GCS impl and an in-memory fake;
- a separate `Signer` (signed PUT/GET, POST policy).

Drop V1 and pkg/errors, add `Close`, and use `path.Join`.

## utils (`utils/`)
**Helpers:** generics (`generic.go`), VN phone/NID/email masking, VND currency, Vietnamese accent removal, time helpers, URL validators, `DiffMaps`, `ParseTemplate` (sprig).

Now in the standard library:
| Helper | Stdlib replacement |
|---|---|
| `IsContains` | `slices.Contains` |
| `Max`/`Min` | `slices.Max`/`Min`, builtin `max`/`min` (1.21) |
| `Reverse` | `slices.Reverse` |
| `RemoveDuplicates` | `slices.Compact` after sort |
| `Filter` | `slices.DeleteFunc` / iterator form |
| `GetLastN` | slicing |
| `Coalesce` | `cmp.Or` (1.22) |
| `Ternary` | none |
| constraints | `cmp.Ordered` replaces `golang.org/x/exp/constraints` |

**Bugs:**
- `IsContains` uses reflection and `DeepEqual` (`generic.go:9-17`); it is slow and needlessly reflective.
- `Max`/`Min` panic on an empty slice (`:68,78`).
- `UniqueArray` returns results in random order (`:55-65`).
- `Sum[constraints.Ordered]` accepts strings (`:110`).
- `ChunkArray` actually means "take first N" (`:102`).
- Regexes are recompiled on every call (`email.go:11`, `identity.go:20-30`, `phone.go:61`, `text.go:53`).
- `MustParseTime` runs all 6 patterns with no early return (`time.go:30-47`).
- `StartOfDay`/`StartOfMonth`/`EndOfMonth` force UTC and drop the input's location (`time.go:68-82`), which is a bug for `NowVN()` inputs. `EndOfDay` truncates to the second (`:72`).
- `VietnamLocation` silently becomes nil if tzdata is missing (`time.go:8`); fix with `import _ "time/tzdata"`.
- `ParseTemplate` uses `html/template` (`template.go:6`), so it HTML-escapes non-HTML text.
- It uses sprig v2.22 `+incompatible`; latest is v3.3.0.
- go.mod is untidy (all indirect, golang.org/x/text v0.19.0).
- Tests: `TestReverse` asserts nothing (`generic_test.go:62-63`).

**Verdict: SPLIT + DROP most of it.** Drop everything the standard library covers. Keep a small `xslices` (`Map` as FormatList's replacement, `KeyBy` as Array2Map's, `UniqueBy`) built on `iter.Seq`. Move the VN-specific helpers (phone, NID, VND, accents, `NowVN`) into a `vn` package with precompiled regexes. Fix or rename the time helpers into a `timex` package with a location parameter. Drop `ParseTemplate`, or move it to `text/template` + sprig v3.

## parallel (`parallel/tasks.go`, root module)
**API:** `RunWithConcurrency[T](ctx, []TaskFunc[T], n) ([]T, error)`.

**Bugs:**
- Task errors are silently dropped (`:24-27`).
- Result order is non-deterministic, and the unused `i` means index information is lost (`:20`).
- `concurrency<=0` creates an unbuffered semaphore and deadlocks (`:11,18`).
- The semaphore acquire ignores `ctx` (`:18`).
- On cancel it returns partial results while the goroutines keep running.

No tests and 0 users.

**Verdict: DROP.** Use `errgroup.Group` + `SetLimit`, `conc/pool` (already in 64 repos; latest v0.3.0 dated 2023, so stagnant), or Go 1.25 `sync.WaitGroup.Go`. If kept, the shape would be `parallel.Map(ctx, in, n, fn) ([]Out, error)`, order-preserving, using errgroup internally.

## crawl (`crawl/crawler.go`)
**API:** `NewWorker(*http.Client).Crawl(url) (*WebsiteMetadata, error)`, returning title, description and logo via goquery.

**Issues:**
- No `context` (`:65`), no status-code or Content-Type check, no body size limit (`io.LimitReader`), no User-Agent, and no charset decoding.
- SSRF exposure: the target URL comes from LLM/deep-research flows with no private-IP guard.
- Logo selection follows document order rather than priority between og:image and icon (`:118-126`).
- Skipped extensions return an empty result with a nil error (`:67-69`).
- `NewWorker` is a misleading name. No tests.

goquery is at v1.10.3 vs latest v1.14.0; golang.org/x/net is at v0.39.0.

**Verdict: UPDATE / MERGE** into an `httpx`/`webmeta` package. Sketch: `webmeta.Fetch(ctx, client, url, opts...)` with size limit, SSRF-safe dialer, priority-ordered selectors and tests using `httptest`.

## Dependency summary (verified via `https://proxy.golang.org/<mod>/@latest`, 2026-11-20)
| Module | Used | Latest |
|---|---|---|
| gorm.io/gorm | 1.25.11 (storage), 1.30.0 (migrations) | 1.31.2 |
| go-sql-driver/mysql | 1.8.1 | 1.10.2 |
| cloud.google.com/go/storage | 1.61.3 | 1.71.0 |
| google.golang.org/api | 0.273.0 | 0.316.0 |
| rs/xid | 1.6.0 | 1.6.0 |
| PuerkitoBio/goquery | 1.10.3 | 1.14.0 |
| golang-migrate/migrate/v4 | 4.18.3 | 4.20.1 |
| redis/go-redis/v9 | 9.11.0 (cache) | 9.23.0 |
| pressly/goose/v3 | — | 3.30.1 |
| jackc/pgx/v5 | — | 5.13.0 |
| maypok86/otter/v2 | — | 2.4.0 |
| gocloud.dev | — | 0.49.0 |

## Open questions for the owner
1. Is any live service still on MySQL (4 repos have the gorm mysql driver)? That decides whether the new storage package is Postgres-only.
2. Should the new library standardize on pgx + sqlc, or keep gorm as a first-class API for the ~61 gorm-postgres repos?
3. Migrations: keep golang-migrate's `schema_migrations` table format for in-place adoption, or accept switching to goose's table? Also, does `db-migrations/` (a sibling repo) run migrations centrally instead?
4. Has anyone called `migrations.Down`, `DownAll` or `Steps` in ops tooling, given that `Down` drops everything and closes the pool?
5. Is multi-cloud blob storage (S3/R2) a real requirement, or is GCS-only acceptable (which affects the gocloud.dev vs thin-interface choice)?
6. Should the cache include a local L1 tier and stampede protection given the 30K CCU target, and is Redis Cluster or Sentinel used in production?
7. Is the `Asia/Ho_Chi_Minh` default (Postgres DSN, `NowVN`) intended to stay library-level, or move to per-service config?
8. Should the 8 `_ "kitchen/utils"` imports in `tools.go` files be removed?
