# Design: data-utils

Replaces the old library's packages `cache`, `storage`, `migrations`, `uploader`, `utils`,
`parallel` and `crawl`.
Review with file:line findings: [`docs/review/06-data-utils.md`](../review/06-data-utils.md).
Cross-cutting rules: [`docs/decisions.md`](../decisions.md) (D1–D6).

All decisions below were made by the owner on 2026-11-20 (question rounds in
the data-utils session). Dependency versions verified the same day via
`proxy.golang.org/<module>/@latest`.

## Decisions (DU1–DU14)

| # | Topic | Decision |
|---|---|---|
| DU1 | MySQL | **Dropped.** Only `ms-template-service` calls `mysql.Open`; it keeps its own 5 lines. |
| DU2 | DB API | **pgxpool is the core** (`pg.Open` → `*pgxpool.Pool`). gorm is an adapter submodule `pg/gorm` that opens `*gorm.DB` **on the same pool**. |
| DU3 | Migration tool | **golang-migrate v4.20.1** kept: same `schema_migrations(version bigint, dirty bool)` table and `NNN_name.up.sql/.down.sql` files, so the 35 DBs and the `db-migrations` repo need no state migration. |
| DU4 | Who runs migrations | **Library + CLI sharing one code path**: `migrate` package (embed.FS, at service start) and `migrate/cmd/kmigrate` binary (for `db-migrations`). |
| DU5 | Migrate locking / dirty | golang-migrate's `pg_advisory_lock` with `LockTimeout` (default 1m). Dirty → `*migrate.DirtyError{Version}`, never auto-forced; only explicit `Force`. `ErrNoChange` → nil. |
| DU6 | Blob | Thin interface in core (`blob.Bucket`, `blob.Signer`, `blob/memblob` fake, no deps); GCS impl in submodule `blob/gcs`. No gocloud.dev. Other clouds = future submodules. |
| DU7 | Blob extras | Keep `blob.NewObjectKey` (same `category/20060102/<xid>.ext` layout, `path.Join`) and `PublicURL` (CDN base, escaped). Drop `uploader.File` DTO, `KB/MB`, V1 `Engine`. MIME→ext via stdlib `mime` + small table. |
| DU8 | Cache tiers | Generic `cache.Cache[V]`: optional L1 (otter v2, short TTL, size-bounded) + optional L2 (Redis `UniversalClient`), `GetOrLoad` with singleflight. L1 cross-pod invalidation = TTL only (no pub/sub). |
| DU9 | Cache failure | **Fail-open**: Redis error → log + metric, call loader, skip L2 write. Optional `NegativeTTL` (default 0 = off) for `ErrNotFound`. TTL jitter (default ±10%). |
| DU10 | Cache API | `Get`, `Set`, `Delete(keys...)`, `GetOrLoad`, `GetMany`, `GetOrLoadMany` (MGET + batch loader), `DeleteByPrefix` (SCAN + UNLINK; Cluster: every master). `Codec[V]`, default `encoding/json`. Raw ops (SetNX/H*/Incr) → use go-redis directly. |
| DU11 | Redis open | New package `redisx`: `Config` → `redis.NewUniversalClient` (standalone/Cluster/Sentinel) + OTel (redisotel) + Ping. |
| DU12 | pg defaults | pgx defaults + explicit overrides; `SSLMode` default `prefer`; no default `TimeZone`; password escaped; ping before return; `QueryExecMode` selectable (PgBouncer → `simple_protocol`/`exec`). |
| DU13 | utils | Drop `vn` entirely (0 call sites). `timex`: `Parse`/`MustParse` + start/end helpers that keep the input location; no VN constant, no embedded tzdata. `xslices`: `Map`, `KeyBy`, `Uniq`, `UniqBy`. `xmaps`: `Diff`. Drop everything stdlib covers and `ParseTemplate`. |
| DU14 | parallel / crawl | `parallel` **dropped** (use `errgroup.SetLimit`). `crawl` → `webmeta` with SSRF-safe defaults, x/net/html parser (goquery dropped). |

## Module layout (D1)

| Dir | Module | Heavy deps |
|---|---|---|
| `cache/` | core | go-redis v9.23.0, otter v2.4.0, x/sync (singleflight) |
| `redisx/` | core | go-redis v9.23.0, redisotel v9.23.0 |
| `pg/` | core | pgx v5.13.0, otelpgx v0.13.0 |
| `blob/`, `blob/memblob/` | core | rs/xid v1.6.0 |
| `xslices/`, `xmaps/`, `timex/` | core | none |
| `webmeta/` | core | golang.org/x/net (html, html/charset) |
| `pg/gorm/` | **submodule** `github.com/dungnh3/kitchen/pg/gorm` | gorm v1.31.2, gorm postgres v1.6.3, pgx |
| `migrate/` (+ `cmd/kmigrate`) | **submodule** `github.com/dungnh3/kitchen/migrate` | golang-migrate v4.20.1, pgx |
| `blob/gcs/` | **submodule** `github.com/dungnh3/kitchen/blob/gcs` | cloud.google.com/go/storage v1.71.0 |

Submodules depend on core only where they need its types (`blob/gcs` →
`blob`). `pg/gorm` and `migrate` take a `*pgxpool.Pool` and do not import
`pg`, so they can be versioned independently.

## Package APIs

### `pg`

```go
type Config struct {
    URL string // postgres://... ; if set, the fields below override it when non-zero
    Host string; Port int; User, Password, Database string
    SSLMode string            // default "prefer"
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
func WithLogger(*slog.Logger) Option
func WithTracerProvider(trace.TracerProvider) Option
func WithMeterProvider(metric.MeterProvider) Option   // pool stats via otelpgx
func Ping(ctx context.Context, pool *pgxpool.Pool) error // health check helper
```

### `pg/gorm` (package `pggorm`)

```go
type Options struct { Logger *slog.Logger; SlowThreshold time.Duration; Config *gorm.Config }
func Open(pool *pgxpool.Pool, opts Options) (*gorm.DB, error)
```
Uses `stdlib.OpenDBFromPool(pool)` (closing it never closes the pool) and
`gorm.io/driver/postgres` with `Conn:`. Logging via gorm's `logger.NewSlogLogger`.
Tracing comes from the pool's otelpgx tracer, so no gorm OTel plugin.

### `migrate`

```go
type Config struct {
    Source fs.FS; Dir string          // e.g. embed.FS + "sql"
    Table string                      // default "schema_migrations"
    LockTimeout time.Duration         // default 1m
    StatementTimeout time.Duration    // 0 = none
    Logger *slog.Logger
}
func New(pool *pgxpool.Pool, cfg Config) (*Migrator, error)
func Open(ctx context.Context, databaseURL string, cfg Config) (*Migrator, error) // owns its pool (CLI)
func (m *Migrator) Up(ctx) error
func (m *Migrator) Down(ctx, n int) error        // exactly n steps, n>=1
func (m *Migrator) To(ctx, version uint) error
func (m *Migrator) Version(ctx) (version uint, dirty bool, err error) // ErrNilVersion when empty
func (m *Migrator) Force(ctx, version int) error
func (m *Migrator) Close() error                 // never closes a caller pool
type DirtyError struct{ Version uint }
```
`ctx` cancellation is wired to golang-migrate's `GracefulStop`.
`kmigrate -database URL -path DIR [-table T] up | down N | to V | version | force V`.

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

```go
var ErrNotFound = errors.New("cache: not found")
type Codec[V any] interface { Marshal(V) ([]byte, error); Unmarshal([]byte, *V) error }
type LocalConfig struct { MaxSize int; TTL time.Duration }
type Config[V any] struct {
    Redis redis.UniversalClient // nil → L1 only
    Local *LocalConfig          // nil → no L1
    Prefix string               // required when Redis != nil
    TTL time.Duration           // L2 TTL, required when Redis != nil
    NegativeTTL time.Duration   // 0 → negative caching off
    Jitter float64              // default 0.1, negative → off
    Codec Codec[V]              // default JSON
    Logger *slog.Logger; MeterProvider metric.MeterProvider; Name string
}
func New[V any](cfg Config[V]) (*Cache[V], error)
func (c *Cache[V]) Get(ctx, key string) (V, error)          // ErrNotFound on miss
func (c *Cache[V]) Set(ctx, key string, v V) error
func (c *Cache[V]) Delete(ctx, keys ...string) error
func (c *Cache[V]) GetOrLoad(ctx, key string, load func(ctx context.Context, key string) (V, error)) (V, error)
func (c *Cache[V]) GetMany(ctx, keys []string) (map[string]V, error)
func (c *Cache[V]) GetOrLoadMany(ctx, keys []string, load func(ctx context.Context, missing []string) (map[string]V, error)) (map[string]V, error)
func (c *Cache[V]) DeleteByPrefix(ctx, prefix string) (int, error)
func (c *Cache[V]) Close()  // stops otter
```
No cache-level spans: Redis commands are traced by redisotel (via `redisx`);
the cache exports the `kitchen.cache.requests{cache.name,cache.tier,cache.result}` counter.
Singleflight is per `Cache` and keyed by key; the loader runs with a context
detached from a single caller's cancellation (`context.WithoutCancel`) so one
cancelled caller does not fail the others. Metrics: `cache.requests{tier,result}`.

### `blob`

```go
var ErrNotFound = errors.New("blob: not found")
type Attrs struct { Size int64; ContentType, ETag string; Updated time.Time; Metadata map[string]string }
type WriterOptions struct { ContentType, CacheControl, ContentDisposition string; Metadata map[string]string; ChunkSize *int }
type Bucket interface {
    NewWriter(ctx context.Context, key string, o *WriterOptions) (io.WriteCloser, error) // cancel ctx to abort
    NewReader(ctx context.Context, key string) (io.ReadCloser, error)
    Attributes(ctx context.Context, key string) (*Attrs, error)
    Delete(ctx context.Context, key string) error
    Close() error
}
type SignOptions struct { Method string; Expires time.Duration; ContentType, ResponseContentType, ResponseContentDisposition string }
type PostPolicyOptions struct { Expires time.Duration; MinBytes, MaxBytes int64; ContentType string }
type PostPolicy struct { URL string; Fields map[string]string }
type Signer interface {
    SignedURL(ctx context.Context, key string, o SignOptions) (string, error)
    PostPolicy(ctx context.Context, key string, o PostPolicyOptions) (*PostPolicy, error)
}
func Put(ctx, b Bucket, key string, r io.Reader, o *WriterOptions) error
func Exists(ctx, b Bucket, key string) (bool, error)
func ReadAll(ctx, b Bucket, key string) ([]byte, error)
func NewObjectKey(ext string, prefix ...string) string
func Category(mimeType string) string // "image" for image/png, else "file"
func PublicURL(base, key string) (string, error)
func ExtensionByType(mimeType string) string
```
`blob/gcs`: `gcs.Open(ctx, gcs.Config{Bucket, CDNBaseURL, SignExpiry, ...}, opts ...option.ClientOption) (*gcs.Bucket, error)`;
`*gcs.Bucket` implements `Bucket` + `Signer` + `PublicURL(key)`.

### `timex`, `xslices`, `xmaps`

```go
timex.Parse(s string) (time.Time, error) // DateTime, RFC3339, DateOnly, 2006/01/02, TimeOnly, 2006-01, 2006/01
timex.MustParse(s string) time.Time
timex.StartOfDay/EndOfDay/StartOfMonth/EndOfMonth/StartOfYear/EndOfYear(t) // keep t.Location(); End* = next start − 1ns
timex.Days(from, to time.Time) iter.Seq[time.Time]

xslices.Map(s []T, fn func(T) U) []U
xslices.KeyBy(s []T, key func(T) K) map[K]T // first wins
xslices.Uniq(s []T) []T                     // order-preserving
xslices.UniqBy(s []T, key func(T) K) []T    // order-preserving, first wins

xmaps.Diff(a, b M) M                          // V comparable
xmaps.DiffFunc(a, b M, eq func(V, V) bool) M // for map[string]any use reflect.DeepEqual
```

### `webmeta`

```go
type Config struct {
    Timeout time.Duration   // default 10s
    MaxBytes int64          // default 2 MiB
    UserAgent string        // default "kitchen-webmeta/1"
    MaxRedirects int        // default 5
    AllowPrivate bool       // tests only: disables the SSRF guard
    Transport http.RoundTripper // advanced; the guard is applied on its dialer when nil
}
type Metadata struct { URL, CanonicalURL, Title, Description, SiteName, Image, Icon string }
var ErrForbiddenAddress, ErrNotHTML error
type StatusError struct{ Code int }
func New(cfg Config) *Fetcher
func (f *Fetcher) Close()
func (f *Fetcher) Fetch(ctx context.Context, rawURL string) (*Metadata, error)
func Parse(base *url.URL, r io.Reader) (*Metadata, error) // pure parser, UTF-8 input
```
SSRF guard: only http/https; `net.Dialer.Control` rejects loopback,
private, link-local (incl. 169.254.169.254), CGNAT, multicast, unspecified,
and IPv4-mapped forms **after DNS resolution** (covers rebinding and
redirects). Body capped with `io.LimitReader` (a longer page is parsed up to
the cap, not rejected); non-2xx → `*StatusError`, non-HTML → `ErrNotHTML`;
charset decoded via `x/net/html/charset`. Image priority:
`og:image` > `twitter:image`; icon: `apple-touch-icon` > `icon` > `/favicon.ico`.

## Submodule versions

`blob/gcs` imports core (`blob`) and requires `github.com/dungnh3/kitchen v0.1.0`.
Until core is tagged `v0.1.0`, develop it with an uncommitted `go.work` that
also contains `replace github.com/dungnh3/kitchen v0.1.0 => ./` (the go
command reads the required version's go.mod even in workspace mode).
`pg/gorm` and `migrate` depend only on pgx, not on core.

## Testing

- `-race` everywhere, `goleak.VerifyTestMain` in packages with goroutines.
- `cache`, `redisx`: miniredis v2.40.0 (+ L1 TTL with `testing/synctest` where possible).
- `pg`, `pg/gorm`, `migrate`: testcontainers-go postgres v0.45.0; skipped
  with `t.Skip` when Docker is unavailable or `-short`.
- `blob/gcs`: fake-gcs-server in-process for read/write; signing tested with
  a generated RSA key (no network).
- `webmeta`: `httptest` servers with `AllowPrivate` for parsing, plus guard
  unit tests on the address classifier.
- `Example*` tests in every package.

## Decisions taken by default (to review)

DU1–DU14 above were chosen explicitly by the owner. The choices below were
made during implementation under the owner's standing instruction to take the
recommended / best system-and-security option without asking.

| # | Decision | Alternatives | Pros | Cons |
|---|---|---|---|---|
| DD1 | `migrate.New` takes `*pgxpool.Pool` (wrapped with `stdlib.OpenDBFromPool`), not `*sql.DB` | accept `*sql.DB` | golang-migrate's pgx/v5 driver closes the `*sql.DB` it gets; wrapping guarantees the caller's pool survives | gorm-only callers must pass the pool (they have it via `pg.Open`) |
| DD2 | No single-call "down all" in `migrate` | add `DownAll` | an accidental full drop needs an explicit count | dev resets need `Down(ctx, n)` |
| DD3 | `cache` emits a metric counter but no spans | span per lookup | Redis spans already come from redisotel; no span noise on hot paths | L1 hits are not visible in traces |
| DD4 | Cache Redis payload = 1-byte tag + codec bytes | plain JSON + separate negative marker key | one GET resolves value vs negative entry | not readable by old clients (new prefix needed) |
| DD5 | `cache.Get` returns Redis errors (fail-closed); only `GetOrLoad`/`GetOrLoadMany` fail open | fail-open everywhere | caller of `Get` decides; no silent misses | callers must handle errors |
| DD6 | Cache loader runs with `context.WithoutCancel(ctx)` | caller ctx | one cancelled caller cannot fail the shared flight | a hung loader is bounded only by its own timeouts |
| DD7 | `webmeta` parses a truncated body instead of rejecting large pages | `ErrTooLarge` | metadata lives in `<head>`; big pages still work | none significant |
| DD8 | `webmeta` blocks CGNAT, NAT64, 6to4, TEST-NETs and reserved ranges in addition to RFC 1918/loopback/link-local; forbids URL credentials; ignores proxies | only RFC 1918 + loopback | closes common SSRF bypasses | internal targets need `AllowPrivate` |
| DD9 | `webmeta` with a custom `Transport` does not apply the guard | wrap the transport | no hidden dialer surprises | caller owns safety (documented) |
| DD10 | `pg.Config.SSLMode` is an error when `URL` is also set | rewrite the URL | no ambiguous double source of truth | set `sslmode` inside URL |
| DD11 | otelpgx with SQL text excluded from span attributes | include SQL | avoids leaking literals/PII into traces | less detail in traces |
| DD12 | `pggorm` logs with parameterized queries by default | log full SQL with values | no PII in logs | `LogParameters: true` to debug |
| DD13 | `blob/gcs` requires core `v0.1.0` (not a pseudo-version) | depend on an unpublished pseudo-version | matches D1 tagging | CI tidy for `blob/gcs` fails until core `v0.1.0` is tagged |
| DD14 | `blob.ExtensionByType("image/jpeg")` = `.jpg` | `.jpeg` (old map) | conventional extension | object names differ from old keys |
| DD15 | Integration tests use testcontainers `postgres:18-alpine`, skipped with `-short` or `KITCHEN_SKIP_DOCKER` | env-provided DSN | hermetic | needs Docker locally/CI |

## Out of scope

MySQL, VN helpers, `parallel`, `ParseTemplate`, `uploader.File` DTO, V1
uploader `Engine`, raw Redis helpers (SetNX/H*/Incr), gocloud.dev.
