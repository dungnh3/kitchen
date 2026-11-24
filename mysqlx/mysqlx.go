// Package mysqlx opens an instrumented *sql.DB for MySQL.
//
// [Open] returns a plain *sql.DB using github.com/go-sql-driver/mysql: use it
// directly, with sqlc, or wrap it in gorm with the
// github.com/dungnh3/kitchen/mysqlx/gorm submodule (same pool, same tracing).
// Queries are traced and measured with otelsql (SQL text is not recorded);
// pool statistics are exported as OTel metrics.
//
// Defaults favour safety: TLS is required ([TLSRequire], encrypted but not
// verified, like Postgres sslmode=require), times are parsed into time.Time
// in UTC, and the driver's unsafe options (multi statements, client-side
// interpolation, LOAD DATA LOCAL of any file) stay off unless the DSN turns
// them on.
//
// Open one *sql.DB per database per process: pool statistics are registered
// as OTel callbacks that live as long as the MeterProvider (see
// [WithoutPoolStats]). For readiness use db.PingContext, e.g.
// server.Check{Name: "mysql", Func: db.PingContext}. [MapError] turns MySQL
// errors into errs sentinels.
package mysqlx

import (
	"cmp"
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"net"
	"strconv"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/dungnh3/kitchen/log"
)

// TLS modes for Config.TLSMode. Any other value names a config registered
// with mysql.RegisterTLSConfig.
const (
	// TLSRequire encrypts without verifying the server certificate (MySQL
	// 8 generates a self-signed one). Default.
	TLSRequire = "skip-verify"
	// TLSVerify encrypts and verifies the certificate and host name against
	// the system roots.
	TLSVerify = "true"
	// TLSDisable sends everything, including the password exchange, in
	// plaintext. Use only for a local server.
	TLSDisable = "false"
)

// Config describes the database and pool. Either DSN or Addr+Database must be
// given. With DSN, set the address and TLS mode inside the DSN (Config.TLS is
// allowed when the DSN has no tls parameter); User, Password, Database and
// the other fields apply on top of the DSN when set.
type Config struct {
	// DSN is a go-sql-driver DSN:
	// user:pass@tcp(host:3306)/db?tls=skip-verify. A DSN without a tls
	// parameter gets TLSRequire.
	DSN string

	// Addr is "host:port" (port defaults to 3306).
	Addr     string
	User     string
	Password string
	Database string
	// TLSMode is TLSRequire (default), TLSVerify, TLSDisable or the name
	// of a registered TLS config. Must not be set with DSN.
	TLSMode string
	// TLS verifies the server with your own settings, typically a managed
	// database's CA in RootCAs. ServerName defaults to the host. It is
	// copied, never modified. Must not be set with TLSMode, or with a DSN
	// that has a tls parameter.
	TLS *tls.Config

	// MaxOpenConns default 25; MaxIdleConns default MaxOpenConns (avoids
	// connection churn under load; database/sql's own default is 2).
	MaxOpenConns int
	MaxIdleConns int
	// ConnMaxLifetime default 30m (keep below the server's wait_timeout).
	ConnMaxLifetime time.Duration
	// ConnMaxIdleTime default 5m.
	ConnMaxIdleTime time.Duration
	// ConnectTimeout default: the DSN's timeout, else 5s; also bounds the
	// ping in Open.
	ConnectTimeout time.Duration
	// ReadTimeout and WriteTimeout are I/O timeouts per packet (0 keeps the
	// DSN's readTimeout/writeTimeout; none by default).
	ReadTimeout, WriteTimeout time.Duration
	// Loc is the time zone for DATETIME values (default: the DSN's loc,
	// else UTC).
	Loc *time.Location
	// Params are session variables sent on connect, e.g.
	// {"sql_mode": "'STRICT_ALL_TABLES'"}.
	Params map[string]string
}

type options struct {
	logger  *log.Logger
	tp      trace.TracerProvider
	mp      metric.MeterProvider
	noStats bool
}

// Option configures Open.
type Option func(*options)

// WithLogger sets the logger used for connection lifecycle messages
// (nil or default: log.Default()).
func WithLogger(l *log.Logger) Option { return func(o *options) { o.logger = l } }

// WithTracerProvider sets the tracer provider (default: global).
func WithTracerProvider(tp trace.TracerProvider) Option { return func(o *options) { o.tp = tp } }

// WithMeterProvider sets the meter provider (default: global).
func WithMeterProvider(mp metric.MeterProvider) Option { return func(o *options) { o.mp = mp } }

// WithoutPoolStats skips the database/sql pool metrics. They are registered
// as MeterProvider callbacks that are never unregistered, so a process that
// opens and closes databases repeatedly (tests, per-tenant pools) should use
// it or a dedicated MeterProvider.
func WithoutPoolStats() Option { return func(o *options) { o.noStats = true } }

// ParseConfig builds a mysql.Config from cfg without connecting. Use it when
// you need driver hooks (BeforeConnect, custom Dial) and then call
// OpenConfig.
func ParseConfig(cfg Config) (*mysql.Config, error) {
	var mc *mysql.Config
	if cfg.DSN != "" {
		if cfg.Addr != "" || cfg.TLSMode != "" {
			return nil, errors.New("mysqlx: set address and tls inside DSN when DSN is used")
		}
		var err error
		if mc, err = mysql.ParseDSN(cfg.DSN); err != nil {
			return nil, fmt.Errorf("mysqlx: parse DSN: %w", err)
		}
		switch {
		case cfg.TLS != nil && (mc.TLSConfig != "" || mc.TLS != nil):
			return nil, errors.New("mysqlx: Config.TLS must not be set with a DSN tls parameter")
		case cfg.TLS != nil:
			mc.TLS = cfg.TLS.Clone()
		case mc.TLSConfig == "" && mc.TLS == nil:
			mc.TLSConfig = TLSRequire
		}
		if cfg.User != "" {
			mc.User = cfg.User
		}
		if cfg.Password != "" {
			mc.Passwd = cfg.Password
		}
		if cfg.Database != "" {
			mc.DBName = cfg.Database
		}
	} else {
		if cfg.Addr == "" || cfg.Database == "" {
			return nil, errors.New("mysqlx: DSN or Addr and Database are required")
		}
		addr := cfg.Addr
		if _, _, err := net.SplitHostPort(addr); err != nil {
			addr = net.JoinHostPort(addr, "3306")
		}
		mc = mysql.NewConfig()
		mc.Net, mc.Addr = "tcp", addr
		mc.User, mc.Passwd, mc.DBName = cfg.User, cfg.Password, cfg.Database
		switch {
		case cfg.TLS != nil && cfg.TLSMode != "":
			return nil, errors.New("mysqlx: set TLS or TLSMode, not both")
		case cfg.TLS != nil:
			mc.TLS = cfg.TLS.Clone()
		default:
			mc.TLSConfig = cmp.Or(cfg.TLSMode, TLSRequire)
		}
		mc.Loc = time.UTC
	}
	// Verify the host name of a custom TLS config (the driver does the same
	// on its own copy; doing it here makes ParseConfig's result explicit).
	if mc.TLS != nil && mc.TLS.ServerName == "" && !mc.TLS.InsecureSkipVerify {
		if host, _, err := net.SplitHostPort(mc.Addr); err == nil {
			mc.TLS.ServerName = host
		}
	}
	if cfg.Loc != nil {
		mc.Loc = cfg.Loc
	}
	mc.ParseTime = true
	mc.Timeout = durOr(cfg.ConnectTimeout, durOr(mc.Timeout, 5*time.Second))
	if cfg.ReadTimeout > 0 {
		mc.ReadTimeout = cfg.ReadTimeout
	}
	if cfg.WriteTimeout > 0 {
		mc.WriteTimeout = cfg.WriteTimeout
	}
	if len(cfg.Params) > 0 {
		if mc.Params == nil {
			mc.Params = map[string]string{}
		}
		maps.Copy(mc.Params, cfg.Params)
	}
	// Resolve the TLS mode now so a typo fails here, not on first query.
	if _, err := mysql.NewConnector(mc); err != nil {
		return nil, fmt.Errorf("mysqlx: %w", err)
	}
	return mc, nil
}

func durOr(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// Open parses cfg, instruments the driver, connects and pings. On error no
// database is left open. The caller must Close the returned *sql.DB.
func Open(ctx context.Context, cfg Config, opts ...Option) (*sql.DB, error) {
	mc, err := ParseConfig(cfg)
	if err != nil {
		return nil, err
	}
	db, err := OpenConfig(ctx, mc, opts...)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cmp.Or(cfg.MaxOpenConns, 25))
	db.SetMaxIdleConns(cmp.Or(cfg.MaxIdleConns, cfg.MaxOpenConns, 25))
	db.SetConnMaxLifetime(durOr(cfg.ConnMaxLifetime, 30*time.Minute))
	db.SetConnMaxIdleTime(durOr(cfg.ConnMaxIdleTime, 5*time.Minute))
	return db, nil
}

// OpenConfig is Open for a mysql.Config built by ParseConfig (or by hand). It
// leaves database/sql pool limits at their defaults; set them on the result.
func OpenConfig(ctx context.Context, mc *mysql.Config, opts ...Option) (*sql.DB, error) {
	o := options{logger: log.Default(), tp: otel.GetTracerProvider(), mp: otel.GetMeterProvider()}
	for _, opt := range opts {
		opt(&o)
	}
	o.logger = log.OrDefault(o.logger)
	conn, err := mysql.NewConnector(mc)
	if err != nil {
		return nil, fmt.Errorf("mysqlx: %w", err)
	}
	host, port := splitAddr(mc.Addr)
	attrs := []attribute.KeyValue{semconv.DBSystemNameMySQL, semconv.DBNamespace(mc.DBName), semconv.ServerAddress(host)}
	if port > 0 {
		attrs = append(attrs, semconv.ServerPort(port))
	}
	otelOpts := []otelsql.Option{
		otelsql.WithTracerProvider(o.tp),
		otelsql.WithMeterProvider(o.mp),
		otelsql.WithAttributes(attrs...),
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			DisableQuery:         true, // literals may hold personal data
			DisableErrSkip:       true,
			OmitConnResetSession: true,
			OmitRows:             true,
		}),
	}
	db := otelsql.OpenDB(conn, otelOpts...)
	pctx, cancel := context.WithTimeout(ctx, durOr(mc.Timeout, 5*time.Second))
	defer cancel()
	if err := db.PingContext(pctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysqlx: ping %s/%s: %w", mc.Addr, mc.DBName, err)
	}
	if !o.noStats {
		if _, err := otelsql.RegisterDBStatsMetrics(db, otelOpts...); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("mysqlx: record pool stats: %w", err)
		}
	}
	o.logger.InfoContext(ctx, "mysqlx: connected", "addr", mc.Addr, "database", mc.DBName, "tls", tlsName(mc))
	return db, nil
}

func splitAddr(addr string) (string, int) {
	host, p, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 0
	}
	port, _ := strconv.Atoi(p)
	return host, port
}

func tlsName(mc *mysql.Config) string {
	if mc.TLS != nil {
		return "custom"
	}
	return cmp.Or(mc.TLSConfig, TLSDisable)
}
