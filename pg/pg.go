// Package pg opens an instrumented pgx connection pool for PostgreSQL.
//
// [Open] returns a plain *pgxpool.Pool: use it directly, with sqlc, or wrap it
// in gorm with the github.com/dungnh3/kitchen/pg/gorm submodule (same pool,
// same tracing). Queries are traced and measured with otelpgx; pool
// statistics are exported as OTel metrics.
//
// The package sets no time zone; set Config.RuntimeParams["timezone"] if a
// service needs one.
package pg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// QueryExecMode selects how pgx sends queries. See pgx.QueryExecMode.
type QueryExecMode string

const (
	// ExecModeCacheStatement prepares and caches statements per connection
	// (pgx default, fastest). Not compatible with PgBouncer transaction mode.
	ExecModeCacheStatement QueryExecMode = "cache_statement"
	// ExecModeCacheDescribe caches statement descriptions only.
	ExecModeCacheDescribe QueryExecMode = "cache_describe"
	// ExecModeDescribeExec describes then executes, without caching.
	ExecModeDescribeExec QueryExecMode = "describe_exec"
	// ExecModeExec uses the extended protocol without preparing; works with
	// PgBouncer transaction mode.
	ExecModeExec QueryExecMode = "exec"
	// ExecModeSimpleProtocol uses the simple protocol with client-side
	// parameter interpolation; works with any pooler.
	ExecModeSimpleProtocol QueryExecMode = "simple_protocol"
)

// Config describes the database and pool. Either URL or Host+Database must be
// given. Non-zero fields override the corresponding URL parts.
type Config struct {
	// URL is a postgres:// URL or key=value DSN.
	URL string

	Host     string
	Port     int // default 5432
	User     string
	Password string
	Database string
	// SSLMode is a libpq sslmode (disable, allow, prefer, require,
	// verify-ca, verify-full). Default "prefer" unless set in URL.
	SSLMode string

	// MaxConns default: pgx (max(4, runtime.NumCPU())).
	MaxConns int32
	// MinConns default 0.
	MinConns int32
	// MaxConnLifetime default 30m; MaxConnLifetimeJitter default 5m.
	MaxConnLifetime       time.Duration
	MaxConnLifetimeJitter time.Duration
	// MaxConnIdleTime default 5m.
	MaxConnIdleTime time.Duration
	// HealthCheckPeriod default 30s.
	HealthCheckPeriod time.Duration
	// ConnectTimeout default 5s; also bounds the ping in Open.
	ConnectTimeout time.Duration
	// StatementTimeout sets statement_timeout on every connection; 0 keeps
	// the server default.
	StatementTimeout time.Duration
	// ApplicationName sets application_name (shown in pg_stat_activity).
	ApplicationName string
	// RuntimeParams are extra session parameters, e.g. {"timezone": "UTC"}.
	RuntimeParams map[string]string
	// QueryExecMode default ExecModeCacheStatement.
	QueryExecMode QueryExecMode
}

type options struct {
	log *slog.Logger
	tp  trace.TracerProvider
	mp  metric.MeterProvider
}

// Option configures Open.
type Option func(*options)

// WithLogger sets the logger used for connection lifecycle messages
// (default: slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.log = l } }

// WithTracerProvider sets the tracer provider (default: global).
func WithTracerProvider(tp trace.TracerProvider) Option { return func(o *options) { o.tp = tp } }

// WithMeterProvider sets the meter provider (default: global).
func WithMeterProvider(mp metric.MeterProvider) Option { return func(o *options) { o.mp = mp } }

func (cfg Config) connString() (string, error) {
	if cfg.URL != "" {
		return cfg.URL, nil
	}
	if cfg.Host == "" || cfg.Database == "" {
		return "", errors.New("pg: URL or Host and Database are required")
	}
	port := cfg.Port
	if port == 0 {
		port = 5432
	}
	u := url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(port)),
		Path:   "/" + cfg.Database,
	}
	if cfg.User != "" {
		u.User = url.UserPassword(cfg.User, cfg.Password) // escapes special characters
	}
	q := url.Values{}
	q.Set("sslmode", cmpOr(cfg.SSLMode, "prefer"))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// ParseConfig builds a pgxpool.Config from cfg without instrumentation or
// connecting. Use it when you need pgx hooks (AfterConnect, type
// registration) and then call OpenConfig.
func ParseConfig(cfg Config) (*pgxpool.Config, error) {
	cs, err := cfg.connString()
	if err != nil {
		return nil, err
	}
	pc, err := pgxpool.ParseConfig(cs)
	if err != nil {
		return nil, fmt.Errorf("pg: parse config: %w", err)
	}
	cc := pc.ConnConfig
	if cfg.URL != "" { // field overrides on top of a URL
		if cfg.Host != "" {
			cc.Host = cfg.Host
		}
		if cfg.Port != 0 {
			cc.Port = uint16(cfg.Port) //nolint:gosec // port range validated by pgx on connect.
		}
		if cfg.User != "" {
			cc.User = cfg.User
		}
		if cfg.Password != "" {
			cc.Password = cfg.Password
		}
		if cfg.Database != "" {
			cc.Database = cfg.Database
		}
		if cfg.SSLMode != "" {
			return nil, errors.New("pg: set sslmode inside URL when URL is used")
		}
	}

	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		pc.MinConns = cfg.MinConns
	}
	if pc.MinConns > pc.MaxConns {
		return nil, fmt.Errorf("pg: MinConns %d > MaxConns %d", pc.MinConns, pc.MaxConns)
	}
	pc.MaxConnLifetime = durOr(cfg.MaxConnLifetime, 30*time.Minute)
	pc.MaxConnLifetimeJitter = durOr(cfg.MaxConnLifetimeJitter, 5*time.Minute)
	pc.MaxConnIdleTime = durOr(cfg.MaxConnIdleTime, 5*time.Minute)
	pc.HealthCheckPeriod = durOr(cfg.HealthCheckPeriod, 30*time.Second)
	cc.ConnectTimeout = durOr(cfg.ConnectTimeout, 5*time.Second)

	if cfg.StatementTimeout > 0 {
		cc.RuntimeParams["statement_timeout"] = strconv.FormatInt(cfg.StatementTimeout.Milliseconds(), 10)
	}
	if cfg.ApplicationName != "" {
		cc.RuntimeParams["application_name"] = cfg.ApplicationName
	}
	for k, v := range cfg.RuntimeParams {
		cc.RuntimeParams[k] = v
	}

	switch cfg.QueryExecMode {
	case "", ExecModeCacheStatement:
		cc.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	case ExecModeCacheDescribe:
		cc.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	case ExecModeDescribeExec:
		cc.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	case ExecModeExec:
		cc.DefaultQueryExecMode = pgx.QueryExecModeExec
	case ExecModeSimpleProtocol:
		cc.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	default:
		return nil, fmt.Errorf("pg: unknown QueryExecMode %q", cfg.QueryExecMode)
	}
	return pc, nil
}

func durOr(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// Open parses cfg, instruments the pool, connects and pings. On error no pool
// is left open. The caller must Close the pool.
func Open(ctx context.Context, cfg Config, opts ...Option) (*pgxpool.Pool, error) {
	pc, err := ParseConfig(cfg)
	if err != nil {
		return nil, err
	}
	return OpenConfig(ctx, pc, opts...)
}

// OpenConfig is Open for a pgxpool.Config built by ParseConfig (or by hand).
// It sets pc.ConnConfig.Tracer unless one is already set.
func OpenConfig(ctx context.Context, pc *pgxpool.Config, opts ...Option) (*pgxpool.Pool, error) {
	o := options{log: slog.Default(), tp: otel.GetTracerProvider(), mp: otel.GetMeterProvider()}
	for _, opt := range opts {
		opt(&o)
	}
	if pc.ConnConfig.Tracer == nil {
		pc.ConnConfig.Tracer = otelpgx.NewTracer(
			otelpgx.WithTracerProvider(o.tp),
			otelpgx.WithMeterProvider(o.mp),
			otelpgx.WithDisableSQLStatementInAttributes(),
		)
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("pg: new pool: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, durOr(pc.ConnConfig.ConnectTimeout, 5*time.Second))
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping %s:%d/%s: %w", pc.ConnConfig.Host, pc.ConnConfig.Port, pc.ConnConfig.Database, err)
	}
	if err := otelpgx.RecordStats(pool, otelpgx.WithStatsMeterProvider(o.mp)); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: record pool stats: %w", err)
	}
	o.log.InfoContext(ctx, "pg: connected",
		"host", pc.ConnConfig.Host, "database", pc.ConnConfig.Database, "max_conns", pc.MaxConns)
	return pool, nil
}

// Ping runs "SELECT 1" on a pooled connection; use it in readiness checks.
func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	var one int
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("pg: ping: %w", err)
	}
	return nil
}
