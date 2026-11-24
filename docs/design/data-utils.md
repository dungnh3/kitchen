# Design: data-utils

Replaces the old library's packages `cache`, `storage`, `migrations`, `uploader`, `utils`,
`parallel` and `crawl`.
Cross-cutting rules: [`docs/decisions.md`](../decisions.md) (D1–D6).

All decisions below were made by the owner on 2026-11-20 (question rounds in
the data-utils session). Dependency versions verified the same day via
`proxy.golang.org/<module>/@latest`.

## Decisions (DU1–DU14)

| # | Topic | Decision |
|---|---|---|
| DU1 | MySQL | ~~Dropped~~ → **reinstated (owner, 2026-11-21)** as core `mysqlx` (go-sql-driver v1.10.2 + XSAM/otelsql v0.44.0 → `*sql.DB`) and submodule `mysqlx/gorm`, mirroring `pg`/`pg/gorm`. TLS required by default (`skip-verify`, encrypted like pg `require`). |
| DU1b | MongoDB | **Added (owner, 2026-11-21)**: submodule `mongox` on driver v2 (v2.10.1). No official otelmongo for v2 exists (contrib only ships v1), so kitchen has a small CommandMonitor/PoolMonitor → OTel bridge (DB semconv, no command bodies); replace it when an official v2 integration ships. TLS required unless the URI chooses or all hosts are loopback. No repository wrapper: use the driver API. |
| DU2 | DB API | **pgxpool is the core** (`pg.Open` → `*pgxpool.Pool`). gorm is an adapter submodule `pg/gorm` that opens `*gorm.DB` **on the same pool**. |
| DU3 | Migration tool | **goose v3.30.1** (owner, 2026-11-21; replaces golang-migrate v4.20.1). The old reason for golang-migrate — no state migration for existing service databases — no longer applies (kitchen is a personal library). goose runs each migration in a transaction (no dirty state), has a context/slog Provider API, Go migrations, and Postgres + MySQL lockers. Files: `<version>_<name>.sql` with `-- +goose Up/Down`; history table `goose_db_version`. |
| DU4 | Who runs migrations | **Library + CLI sharing one code path**: `migrate` package (embed.FS, at service start) and `migrate/cmd/kmigrate` binary (for a separate migrations repo or job). |
| DU5 | Migrate locking / failures | Postgres: goose session advisory lock; MySQL: goose lock table (`goose_lock`); both bounded by `LockTimeout` (default 1m). A failed migration rolls back its transaction, so there is no dirty state, `DirtyError` or `Force`. Nothing pending → nil. Dialects: Postgres and MySQL (owner, 2026-11-21). `Open` connects via `pg.Open` / `mysqlx.Open` (same TLS defaults). |
| DU6 | Object storage | Thin interface in core (`objstore.Bucket`, `objstore.Signer`, `objstore/memstore` fake); GCS in submodule `objstore/gcs`, S3 in `objstore/s3` (owner, 2026-11-21). No gocloud.dev. Renamed from `blob` (owner, 2026-11-21). |
| DU7 | objstore extras | Keep `objstore.NewObjectKey` (same `category/20060102/<xid>.ext` layout, `path.Join`) and `PublicURL` (CDN base, escaped). Drop `uploader.File` DTO, `KB/MB`, V1 `Engine`. MIME→ext via stdlib `mime` + small table. |
| DU8 | Cache tiers | Generic `cache.Cache[V]`: optional L1 (otter v2, short TTL, size-bounded) + optional L2 (Redis `UniversalClient`), `GetOrLoad` with singleflight. L1 cross-pod invalidation = TTL only (no pub/sub). |
| DU9 | Cache failure | **Fail-open**: Redis error → log + metric, call loader, skip L2 write. Optional `NegativeTTL` (default 0 = off) for `ErrNotFound`. TTL jitter (default ±10%). |
| DU10 | Cache API | `Get`, `Set`, `Delete(keys...)`, `GetOrLoad`, `GetMany`, `GetOrLoadMany` (MGET + batch loader), `DeleteByPrefix` (SCAN + UNLINK on every shard via `redisx.ForEachShard`: Cluster masters, Ring shards). `Codec[V]`, default `encoding/json`. Raw ops (SetNX/H*/Incr) → use go-redis directly. |
| DU11 | Redis open | New package `redisx`: `Config` → `redis.NewUniversalClient` (standalone/Cluster/Sentinel) + OTel (redisotel) + Ping. |
| DU12 | pg defaults | pgx defaults + explicit overrides; `SSLMode` default `require` (owner, 2026-11-21: never plaintext unless `disable` is explicit); no default `TimeZone`; password escaped; ping before return; `QueryExecMode` selectable (PgBouncer → `simple_protocol`/`exec`). |
| DU13 | utils | Drop `vn` entirely (0 call sites). `timex`: `Parse`/`MustParse` + start/end helpers that keep the input location; no VN constant, no embedded tzdata. Slice/map helpers: **use `github.com/samber/lo` directly** (owner, 2026-11-21, reuse rule: zero-dependency, widely used, already in 5 services); kitchen's `xslices`/`xmaps` were removed. `ParseTemplate` stays dropped: services use `text/template` (the old `html/template` escaped quotes in LLM prompts). Drop everything stdlib covers. |
| DU14 | parallel / crawl | `parallel` **dropped** (use `errgroup.SetLimit`); its only real use, best-effort crawling of citation URLs, became `webmeta.FetchAll` (owner, 2026-11-21; conc v0.3.0 is stale, pond v2 adds a dependency errgroup makes unnecessary). `crawl` → `webmeta` with SSRF-safe defaults, x/net/html parser (goquery dropped; owner re-confirmed 2026-11-21 after comparing otiai10/opengraph v2.2.0, dyatlov/go-opengraph v1.0.1 (stale) and goquery v1.14.0). Fetching goes through **`httpx` with `WithSSRFGuard`** (owner, 2026-11-21: one SSRF guard in kitchen; webmeta's own guard allowed any port and missed Teredo/fec0). |

## Module layout (D1)

| Dir | Module | Heavy deps |
|---|---|---|
| `cache/` | core | go-redis v9.23.0, otter v2.4.0, x/sync (singleflight) |
| `redisx/` | core | go-redis v9.23.0, redisotel v9.23.0 |
| `pg/` | core | pgx v5.13.0, otelpgx v0.13.0, pgerrcode |
| `objstore/`, `objstore/memstore/` | core | rs/xid v1.6.0 |
| `timex/` | core | none |
| `webmeta/` | core | golang.org/x/net (html, html/charset), `httpx` |
| `mysqlx/` | core | go-sql-driver/mysql v1.10.2, XSAM/otelsql v0.44.0 |
| `mysqlx/gorm/` | **submodule** `github.com/dungnh3/kitchen/mysqlx/gorm` | gorm v1.31.2, gorm mysql v1.6.0, core, `log/gormlog` |
| `mongox/` | **submodule** `github.com/dungnh3/kitchen/mongox` | mongo-driver v2.10.1, core (errs) |
| `pg/gorm/` | **submodule** `github.com/dungnh3/kitchen/pg/gorm` | gorm v1.31.2, gorm postgres v1.6.3, pgx, `log/gormlog` (tag it first) |
| `migrate/` (+ `cmd/kmigrate`) | **submodule** `github.com/dungnh3/kitchen/migrate` | goose v3.30.1, pgx, core (`pg`, `mysqlx`) |
| `objstore/gcs/` | **submodule** `github.com/dungnh3/kitchen/objstore/gcs` | cloud.google.com/go/storage v1.71.0 |
| `objstore/s3/` | **submodule** `github.com/dungnh3/kitchen/objstore/s3` | aws-sdk-go-v2 service/s3 v1.116.0 |

Submodules depend on core only where they need its types (`objstore/gcs`, `objstore/s3` →
`objstore`). `pg/gorm` and `migrate` take a `*pgxpool.Pool` and do not import
`pg`, so they can be versioned independently.

## Package APIs

### `pg`

```go
type Config struct {
    URL string // postgres://... ; with URL only User/Password/Database may override it
    Host string; Port int; User, Password, Database string // Host/Port/SSLMode + URL → error
    SSLMode string            // default "require"
    MaxConns, MinConns int32  // 0 → pgx default (max(4, NumCPU)) / 0
    MaxConnLifetime time.Duration       // default 30m
    MaxConnLifetimeJitter time.Duration // default 5m
    MaxConnIdleTime time.Duration       // default 5m
    HealthCheckPeriod time.Duration     // default 30s
    ConnectTimeout time.Duration        // default 5s
    StatementTimeout time.Duration      // 0 = server default
    RuntimeParams map[string]string     // e.g. {"timezone": "Asia/Ho_Chi_Minh"}
    QueryExecMode QueryExecMode         // default ExecModeCacheStatement
}
func Open(ctx context.Context, cfg Config, opts ...Option) (*pgxpool.Pool, error)
func ParseConfig(cfg Config) (*pgxpool.Config, error) // for callers that need hooks
func OpenConfig(ctx context.Context, pc *pgxpool.Config, opts ...Option) (*pgxpool.Pool, error)
func WithLogger(*log.Logger) Option // kitchen log; nil -> log.Default()
func WithTracerProvider(trace.TracerProvider) Option
func WithMeterProvider(metric.MeterProvider) Option   // pool stats via otelpgx
func WithoutPoolStats() Option                        // otelpgx never unregisters its callbacks
func MapError(err error) error // pgx/Postgres error → errs sentinel (cause kept for logs)
```
Readiness uses the pool's own `Ping`: `server.Check{Name: "postgres", Func: pool.Ping}`
(`pg.Ping` was removed as a duplicate, owner 2026-11-21). One pool per database
per process: otelpgx's stats callbacks live as long as the MeterProvider.

`MapError` (owner, 2026-11-21) uses `jackc/pgerrcode` constants:
`pgx.ErrNoRows` → `ErrNotFound`; 23505/23P01 → `ErrAlreadyExists`; other class 23 →
`ErrFailedPrecondition`; class 22 → `ErrInvalidArgument`; 40001/40P01 → `ErrAborted`;
57014 → `ErrDeadlineExceeded`; class 08, 53300, 57P01–57P03 → `ErrUnavailable`;
anything else unchanged. Clients see only the sentinel message, so constraint
names do not leak.

### `pg/gorm` (package `pggorm`)

```go
type Options struct { Log *gormlog.Config; Config *gorm.Config }
func Open(pool *pgxpool.Pool, opts Options) (*gorm.DB, error)
```
Uses `stdlib.OpenDBFromPool(pool)` (closing it never closes the pool) and
`gorm.io/driver/postgres` with `Conn:`. `pggorm.MapError` = `pg.MapError` +
`gorm.ErrRecordNotFound` → `ErrNotFound` (so pg/gorm now requires core). Logging via `log/gormlog` (owner, 2026-11-21:
one gorm logger; gorm's built-in `NewSlogLogger` formats SQL for every query and
ignores the level for slow queries). `Log == nil` → gormlog defaults +
`IgnoreRecordNotFound`.
Tracing comes from the pool's otelpgx tracer, so no gorm OTel plugin.

### `mysqlx` and `mysqlx/gorm` (package `mysqlgorm`)

```go
type Config struct {
    DSN string                 // go-sql-driver DSN; with DSN only User/Password/Database may override; no tls param → TLSRequire
    Addr string; User, Password, Database string
    TLSMode string             // TLSRequire "skip-verify" (default) | TLSVerify "true" | TLSDisable "false" | registered name
    TLS *tls.Config            // own CA (managed databases); copied; not with TLSMode or a DSN tls param
    MaxOpenConns, MaxIdleConns int           // default 25 / = MaxOpenConns
    ConnMaxLifetime, ConnMaxIdleTime time.Duration // default 30m / 5m
    ConnectTimeout time.Duration             // default 5s, bounds the ping
    ReadTimeout, WriteTimeout time.Duration
    Loc *time.Location                       // default UTC; parseTime always on
    Params map[string]string                 // session variables
}
func Open(ctx, Config, ...Option) (*sql.DB, error)
func ParseConfig(Config) (*mysql.Config, error); func OpenConfig(ctx, *mysql.Config, ...Option) (*sql.DB, error)
func WithLogger / WithTracerProvider / WithMeterProvider / WithoutPoolStats
func MapError(error) error   // 1062→AlreadyExists, FK/null/check→FailedPrecondition, 1213/1205→Aborted, ...
// mysqlgorm
type Options struct { Log *gormlog.Config; Config *gorm.Config }
func Open(db *sql.DB, Options) (*gorm.DB, error) // gorm shares the caller's *sql.DB
func MapError(error) error                         // + gorm.ErrRecordNotFound
```
otelsql spans omit SQL text, rows and reset-session spans. Multi statements,
client-side interpolation and LOAD DATA LOCAL stay off unless the DSN enables them.

### `mongox`

```go
type Config struct {
    URI string; AppName string
    MaxPoolSize, MinPoolSize uint64          // default 100 / 0
    MaxConnIdleTime time.Duration            // default 5m
    ConnectTimeout, ServerSelectionTimeout time.Duration // default 5s / 5s (driver 30s)
    Timeout time.Duration                    // CSOT, 0 = none
}
func Open(ctx, Config, ...Option) (*mongo.Client, error)   // pings the primary
func ClientOptions(Config) (*options.ClientOptions, error); func Connect(ctx, *options.ClientOptions, ...Option) (*mongo.Client, error)
func WithLogger / WithTracerProvider / WithMeterProvider
func MapError(error) error // ErrNoDocuments→NotFound, dup key→AlreadyExists, 112/TransientTransactionError→Aborted,
                           // 50/timeout→DeadlineExceeded, network/not primary/server selection→Unavailable
```
Spans are named `{command} {collection}` with `db.system.name=mongodb`,
`db.operation.name`, `db.namespace`, `db.collection.name`, `server.address/port`,
and `error.type`/`db.response.status_code` on failure. Metrics:
`db.client.operation.duration`, `db.client.connection.count{state=idle|used}`.

### `migrate`

```go
type Dialect string // Postgres, MySQL
type Config struct {
    Source fs.FS; Dir string          // e.g. embed.FS + "sql"; goose-format *.sql
    GoMigrations []*GoMigration      // optional: NewGoMigration / NewGoMigrationNoTx (no goose import, D7)
    Table string                      // default "goose_db_version"
    LockTimeout time.Duration         // default 1m
    AllowOutOfOrder bool
    Logger *log.Logger                // kitchen log; nil -> log.Default()
}
func NewPostgres(pool *pgxpool.Pool, cfg Config) (*Migrator, error) // never closes pool
func NewMySQL(db *sql.DB, cfg Config) (*Migrator, error)           // never closes db
func Open(ctx, d Dialect, dsn string, cfg Config) (*Migrator, error) // via pg.Open/mysqlx.Open, owns the connection (CLI)
func (m *Migrator) Up(ctx) error
func (m *Migrator) Down(ctx, n int) error        // exactly n steps, n>=1; stops when nothing is left
func (m *Migrator) To(ctx, version int64) error  // up or down; 0 reverts everything
func (m *Migrator) Version(ctx) (int64, error)   // 0 when nothing applied
func (m *Migrator) Status(ctx) ([]*goose.MigrationStatus, error)
func (m *Migrator) HasPending(ctx) (bool, error)
func (m *Migrator) Close() error
```
goose's global registry is disabled (`WithDisableGlobalRegistry`): no package state.
`ctx` cancellation aborts the running migration's transaction.
`kmigrate [-dialect postgres|mysql] -database DSN -path DIR [-table T] up | down N | to V | version | status | create NAME`.

### `redisx`

```go
type Config struct {
    Addrs []string; MasterName string // Sentinel when set; Cluster when len(Addrs)>1 and no MasterName
    Username, Password string; DB int
    TLS *tls.Config
    PoolSize, MinIdleConns int
    DialTimeout, ReadTimeout, WriteTimeout time.Duration
}
func Open(ctx context.Context, cfg Config, opts ...Option) (redis.UniversalClient, error)
```

### `cache`

Revised 2026-11-21 (group-3 review, owner): keep kitchen's cache (no
maintained library has Redis L2 + OTel + negative caching; go-redis/cache is
unmaintained since 2023, eko/gocache is `any`-typed with Prometheus
metrics, sturdyc is in-memory first), but use otter v2's loading API for
L1 instead of hand-rolled code.

Revised 2026-11-24 (deep review, owner): writes win over loads (Redis lease,
L1 write guard), bounded Redis latency with a fail-fast window, `Close(ctx)
error`, L1 weight bound and downward L1 jitter, `Name` required without
`Prefix`, hashed keys in logs.

```go
var ErrNotFound = errs.ErrNotFound // any errs NotFound from a loader = negative answer
var ErrClosed error                // loading methods after Close
type LocalConfig struct { MaxSize int; MaxWeight uint64; TTL, RefreshAfter time.Duration }
type Config[V any] struct {
    Redis redis.UniversalClient; Local *LocalConfig; Prefix string; TTL, NegativeTTL time.Duration
    Jitter float64; LoadTimeout /* 30s */, RedisTimeout /* 500ms */ time.Duration
    Clone func(V) V; Weigher func(key string, v V) uint32
    Codec Codec[V]; Logger *log.Logger; MeterProvider metric.MeterProvider; Name string
}
func (c *Cache[V]) Set(ctx, key string, v V, opts ...SetOption) error // WithTTL(d)
func (c *Cache[V]) Close(ctx context.Context) error
type PanicError struct{ Value any } // a panicking loader or codec is an error, not a crash
// Get, GetMany, Delete, GetOrLoad, GetOrLoadMany, DeleteByPrefix
```

- `GetOrLoad` with L1: `otter.Get` with a loader that runs one detached
  flight per key (L2, then the source, bounded by `LoadTimeout`, cancelled by
  `Close`, panics recovered). Callers return when their ctx ends; the flight
  still fills both tiers. Without L1: the same flight directly.
- **Writes win over loads.**
  - Redis (all pods): a flight's L2 lookup is a Lua script that returns the
    payload or, on a miss, stores a lease (`'l'` + random token, TTL
    `LoadTimeout + RedisTimeout`). The write-back is a compare-and-set on
    what the lookup saw (the lease, or a payload that did not decode).
    `Set` overwrites and `Delete` removes the lease, so a load that read the
    source before them cannot write back (Facebook's "Scaling Memcache"
    leases). Readers treat a lease as a miss.
  - L1 (this pod): `Set`, `Delete` and `DeleteByPrefix` bump a per-key-stripe
    generation before touching L1; a fill (flight result, L2 promotion) is
    `SetIfAbsent` between two generation reads and undoes itself if they
    differ. otter's own write-cancels-load covers its commit. A value whose
    write-back lost the lease is returned to callers but not kept in L1.
  - L1 on other pods is bounded only by `Local.TTL` (and, for promoted
    entries, by the remaining Redis TTL).
- **Redis failures.** Every command is bounded by `RedisTimeout`. After 5
  consecutive failures the read-through paths skip Redis for 5s (then one
  probe). `Get`, `Set` and `Delete` always try Redis (an invalidation is never
  skipped). Warnings are throttled to one per kind per 10s with a
  `suppressed` count.
- `LocalConfig.RefreshAfter`: otter refresh-ahead; a hot entry is reloaded
  in the background (from L2, else the source), detached from the request
  that found it stale, while callers keep the current value. A refresh that
  finds the key gone or changed drops the L1 entry.
- `GetOrLoadMany` with L1: `otter.BulkGet`, so a key being loaded by
  another call is waited for, not loaded twice. Without L1 concurrent batch
  calls are not merged. L2 lookups and write-backs are pipelined scripts.
- `Clone` copies values on Set and on every read; without it L1 values are
  shared between callers.
- Metrics: `kitchen.cache.requests{cache.name,cache.tier,cache.result}`
  (`l2` adds `skipped`, `loader` adds `not_found`) and the histogram
  `kitchen.cache.load.duration` (s). Attribute sets are precomputed: L1 hits
  allocate nothing. No cache-level spans: Redis commands are traced by
  redisotel (via `redisx`).
- Logs never carry keys, only `cache.key_hash` (first 8 bytes of SHA-256).
  otter's own logs go to `Config.Logger`.

### `lease`

Kept (owner, 2026-11-21): redsync and bsm/redislock have no fencing tokens.
`ErrNotHeld` and `ErrLeaseLost` are errs errors (Aborted). New
`lease.Do(ctx, locker, name, ttl, fn, opts...)`: a cross-process mutex that
fails fast with `ErrNotHeld`, renews every ttl/3, cancels `fn`'s ctx (cause
`ErrLeaseLost`) before the lease can expire, recovers panics and releases
on return. It shares its hold/renew loop with `Elector`.

### `objstore` (was `blob`)

Revised 2026-11-21 (group-3 review, owner): renamed `blob` → `objstore`
(`objstore/memstore`, `objstore/gcs`), added `objstore/s3`, made errors
`errs` errors, and made client-direct uploads safe by default.

```go
var ErrNotFound       = errs.New(codes.NotFound, "OBJECT_NOT_FOUND", ...)
var ErrExists         = errs.New(codes.AlreadyExists, "OBJECT_ALREADY_EXISTS", ...)
var ErrInvalidKey     = errs.New(codes.InvalidArgument, "INVALID_OBJECT_KEY", ...)
var ErrInvalidOptions = errs.New(codes.InvalidArgument, "INVALID_UPLOAD_OPTIONS", ...)
func ValidKey(key string) error // non-empty, ≤1024 B UTF-8, no leading "/", no ""/"."/".." segment, no control chars
type WriterOptions struct { ContentType, CacheControl, ContentDisposition string; Metadata map[string]string; ChunkSize *int; IfNotExists bool }
type Bucket interface { NewWriter; NewReader; Attributes; Delete; Close } // every op checks ValidKey
type SignOptions struct { Method string; Expires time.Duration; ContentType string; MaxBytes int64;
    AllowAnyContentType, AllowOverwrite, AllowUnboundedSize bool; ResponseContentType, ResponseContentDisposition string }
type PostPolicyOptions struct { Expires time.Duration; MinBytes, MaxBytes int64; ContentType, ContentTypePrefix string; AllowAnyContentType bool }
func (SignOptions) Check(key string) error; func (PostPolicyOptions) Check(key string, maxBytes int64) error // shared by backends
type Signer interface { SignedURL(...); PostPolicy(...) }
func Instrument(b Bucket, opts ...InstrumentOption) Instrumented // otelx spans "objstore.<Op>", key, bytes, errors
func Put, Exists, ReadAll, NewObjectKey, Category, PublicURL, ExtensionByType
```

Upload rules (owner, fail-closed):

| Rule | GCS | S3 |
|---|---|---|
| PUT URL needs `ContentType` (else `AllowAnyContentType`) | signed `Content-Type` | signed `Content-Type` |
| PUT URL size bound | signed `x-goog-content-length-range: 0,MaxBytes` (default `MaxUploadBytes`) | not possible: refused unless `AllowUnboundedSize`; use `PostPolicy` |
| PUT URL no overwrite (else `AllowOverwrite`) | signed `x-goog-if-generation-match: 0` | signed `If-None-Match: *` |
| POST policy needs `ContentType` or `ContentTypePrefix` | `eq` / `starts-with $Content-Type` | same |
| POST policy size | `content-length-range` | same |
| POST policy overwrite | not expressible: bound to the exact key; use a fresh `NewObjectKey` | same |
| `WriterOptions.IfNotExists` | `DoesNotExist` precondition (also makes retries safe) | `If-None-Match: *` on PutObject and CompleteMultipartUpload |

Backends:

- `objstore/gcs`: `gcs.Open(ctx, gcs.Config{Bucket, CDNBaseURL, SignExpiry, MaxUploadBytes, GoogleAccessID, PrivateKey}, opts ...option.ClientOption)`;
  412 → `ErrExists`. The GCS client already emits OTel spans; wrap with
  `objstore.Instrument` for kitchen-level spans.
- `objstore/s3` (aws-sdk-go-v2 `service/s3` v1.116.0):
  `s3.Open(ctx, s3.Config{Bucket, CDNBaseURL, SignExpiry, MaxUploadBytes, PartSize}, optFns ...func(*config.LoadOptions) error)` or `s3.New(client, cfg)`.
  The writer buffers one part (default 8 MiB): small objects are one
  PutObject, larger ones a sequential multipart upload aborted on error or
  cancel (the SDK upload manager does not pass If-None-Match to
  CompleteMultipartUpload). `Delete` HEADs first so a missing key is
  `ErrNotFound`. Works with S3-compatible stores (MinIO, R2) via
  `BaseEndpoint`/`UsePathStyle` on the client. Tests use gofakes3.
- `objstore/memstore`: in-memory, same rules, for tests.

### `timex` (slices/maps: `samber/lo`)

```go
timex.Parse(s string) (time.Time, error) // DateTime, RFC3339, DateOnly, 2006/01/02, TimeOnly, 2006-01, 2006/01
timex.MustParse(s string) time.Time
timex.StartOfDay/EndOfDay/StartOfMonth/EndOfMonth/StartOfYear/EndOfYear(t) // keep t.Location(); End* = next start − 1ns
timex.Days(from, to time.Time) iter.Seq[time.Time]
```

### `webmeta`

```go
type Config struct {
    Timeout time.Duration    // default 10s
    MaxBytes int64           // default 2 MiB
    UserAgent string         // default "kitchen-webmeta/1 (+...)"
    MaxRedirects int         // default 5
    Ports []int              // default 80, 443
    Allow []netip.Prefix     // non-public prefixes that may be fetched (tests, known internal sites)
    ClientOptions []httpx.Option // logger, tracing, retry; WithTransport is rejected
}
type Metadata struct { URL, CanonicalURL, Title, Description, SiteName, Image, Icon string }
var ErrNotHTML, ErrInvalidURL *errs.Error // InvalidArgument
func New(cfg Config) (*Fetcher, error)
func (f *Fetcher) Close()
func (f *Fetcher) Fetch(ctx context.Context, rawURL string) (*Metadata, error)
type Result struct { URL string; Metadata *Metadata; Err error }
func (f *Fetcher) FetchAll(ctx context.Context, urls []string, concurrency int) []Result // input order, best effort
func Parse(base *url.URL, r io.Reader) (*Metadata, error) // pure parser, UTF-8 input
```
Fetches through an `httpx.Client` with `WithSSRFGuard` (public addresses
only, ports 80/443, checked after DNS and on every redirect, no proxy),
`SafeRedirectPolicy` (no https→http), timeout and body cap. Errors are `errs`:
`httpx.ErrDestinationNotAllowed`, httpx's status mapping (404 →
`errs.ErrNotFound`, `httpx.StatusCode(err)`), `ErrNotHTML`, `ErrInvalidURL`.
The parser reads `<head>`; when it has no description it reads on to the
first body `<p>` of ≥30 characters (cut to 300; script/style skipped) — the
old crawl fallback. `Metadata.URL` is the final URL with query values redacted.

## Submodule versions

`objstore/gcs` and `objstore/s3` import core (`objstore`) and requires `github.com/dungnh3/kitchen v0.1.0`.
Until core is tagged `v0.1.0`, develop it with an uncommitted `go.work` that
also contains `replace github.com/dungnh3/kitchen v0.1.0 => ./` (the go
command reads the required version's go.mod even in workspace mode).
`pg/gorm`, `mysqlx/gorm`, `mongox` and `migrate` require core `v0.1.0` (and `log/gormlog v0.1.0` for the gorm adapters).

## Testing

- `-race` everywhere, `goleak.VerifyTestMain` in packages with goroutines.
- `cache`, `redisx`: miniredis v2.40.0 (+ L1 TTL with `testing/synctest` where possible).
- `pg`, `pg/gorm`, `migrate`, `mysqlx`, `mysqlx/gorm`, `mongox`: testcontainers-go v0.45.0 (postgres:18-alpine, mysql:8.4, mongo:8); skipped
  with `t.Skip` when Docker is unavailable or `-short`.
- `objstore/s3`: gofakes3 in-process (multipart, abort, If-None-Match, presign).
- `objstore/gcs`: fake-gcs-server in-process for read/write; signing tested with
  a generated RSA key (no network).
- `webmeta`: `httptest` servers allowed through `Config.Allow`/`Ports`;
  the SSRF classifier itself is tested in `httpx`.
- `Example*` tests in every package.

## Decisions taken by default (to review)

DU1–DU14 above were chosen explicitly by the owner. The choices below were
made during implementation under the owner's standing instruction to take the
recommended / best system-and-security option without asking.

| # | Decision | Alternatives | Pros | Cons |
|---|---|---|---|---|
| DD1 | `migrate.NewPostgres` takes `*pgxpool.Pool` (wrapped with `stdlib.OpenDBFromPool`); `NewMySQL` takes `*sql.DB` and never calls goose's `Provider.Close` | pass the caller's `*sql.DB` to goose and close it | goose's `Provider.Close` closes the DB it was given; the caller's pool must survive | gorm-only Postgres callers pass the pool (they have it via `pg.Open`) |
| DD2 | No `DownAll`; reverting everything is the explicit `To(ctx, 0)` | add `DownAll` | an accidental full drop needs an explicit target | — |
| DD3 | `cache` emits a metric counter but no spans | span per lookup | Redis spans already come from redisotel; no span noise on hot paths | L1 hits are not visible in traces |
| DD4 | Cache Redis payload = 1-byte tag + codec bytes | plain JSON + separate negative marker key | one GET resolves value vs negative entry | not readable by old clients (new prefix needed) |
| DD5 | `cache.Get` returns Redis errors (fail-closed); only `GetOrLoad`/`GetOrLoadMany` fail open | fail-open everywhere | caller of `Get` decides; no silent misses | callers must handle errors |
| DD6 | Cache loader runs with `context.WithoutCancel(ctx)` | caller ctx | one cancelled caller cannot fail the shared flight | a hung loader is bounded by `LoadTimeout` and cancelled by `Close` |
| DD6a | Cache write-back uses a Redis lease (Lua compare-and-set) (owner, 2026-11-24) | tombstone + `SET NX`; document the race | `Set`/`Delete` during a load win on every pod; no extra round trip | `EVALSHA` instead of `GET`/`SET` on misses; payload tag `'l'` added |
| DD6b | Cache read-through skips Redis after 5 consecutive failures for 5s, internal (owner, 2026-11-24) | timeout only; a canonical breaker package | bounded latency and no log flood during an outage | a breaker inside `cache`; a canonical `breaker` package may replace it |
| DD7 | `webmeta` parses a truncated body instead of rejecting large pages | `ErrTooLarge` | metadata lives in `<head>`; big pages still work | none significant |
| DD8 | `webmeta` uses `httpx`'s SSRF guard (non-public ranges incl. CGNAT, NAT64, 6to4, Teredo; ports 80/443 only); forbids URL credentials; ignores proxies | own guard (removed 2026-11-21) | one guard to maintain; closes port-scan and Teredo bypasses | internal targets need `Config.Allow` (+ `Ports`) |
| DD9 | `webmeta` has no custom-transport escape hatch (httpx rejects `WithTransport` with the guard) | `Config.Transport` (removed 2026-11-21) | the guard cannot be bypassed by configuration | customise via `ClientOptions` only |
| DD10 | `pg.Config.SSLMode`, `Host` or `Port` is an error when `URL` is also set | rewrite the URL / patch pgx fallbacks | pgx derives TLS ServerName and fallback hosts from the URL; a late Host override sent the `prefer` plaintext fallback (and `verify-full` name check) to the URL host | set host, port, sslmode inside URL |
| DD11 | otelpgx with SQL text excluded from span attributes | include SQL | avoids leaking literals/PII into traces | less detail in traces |
| DD12 | `pggorm` logs with parameterized queries by default | log full SQL with values | no PII in logs | `Log: &gormlog.Config{LogParams: true}` to debug |
| DD13 | `objstore/gcs` and `objstore/s3` require core `v0.1.0` (not a pseudo-version) | depend on an unpublished pseudo-version | matches D1 tagging | CI tidy for them fails until core `v0.1.0` is tagged |
| DD14 | `objstore.ExtensionByType("image/jpeg")` = `.jpg` | `.jpeg` (old map) | conventional extension | object names differ from old keys |
| DD15 | Integration tests use testcontainers `postgres:18-alpine`, skipped with `-short` or `KITCHEN_SKIP_DOCKER` | env-provided DSN | hermetic | needs Docker locally/CI |

## Out of scope

VN helpers, `parallel`, `ParseTemplate`, `uploader.File` DTO, V1
uploader `Engine`, raw Redis helpers (SetNX/H*/Incr), gocloud.dev.
