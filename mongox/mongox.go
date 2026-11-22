// Package mongox opens an instrumented MongoDB client (driver v2).
//
// [Open] returns a plain *mongo.Client: use the official driver API
// directly. Commands are traced and measured with OTel following the
// database semantic conventions (operation, database, collection, server;
// command bodies are never recorded) and pool connections are counted, until
// the driver ships an official OTel integration for v2.
//
// TLS is required unless the URI chooses (tls=true/false, or mongodb+srv,
// which implies TLS) or every host is a loopback address. The certificate is
// verified. [MapError] turns driver errors into errs sentinels.
//
// For readiness: server.Check{Name: "mongo", Func: func(ctx context.Context)
// error { return client.Ping(ctx, nil) }}.
package mongox

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/dungnh3/kitchen/log"
)

// Config describes the deployment and pool. Zero values use the defaults
// stated per field.
type Config struct {
	// URI is a mongodb:// or mongodb+srv:// connection string. Required.
	URI string
	// AppName is sent to the server (visible in logs and currentOp).
	AppName string
	// MaxPoolSize per server; default 100 (driver default).
	MaxPoolSize uint64
	// MinPoolSize per server; default 0.
	MinPoolSize uint64
	// MaxConnIdleTime default 5m (the driver keeps idle connections forever).
	MaxConnIdleTime time.Duration
	// ConnectTimeout default 5s.
	ConnectTimeout time.Duration
	// ServerSelectionTimeout default 5s (driver default 30s); also bounds
	// the ping in Open.
	ServerSelectionTimeout time.Duration
	// Timeout is the client-side operation timeout applied when a call's ctx
	// has no deadline; 0 means none.
	Timeout time.Duration
}

type clientOpts struct {
	logger *log.Logger
	tp     trace.TracerProvider
	mp     metric.MeterProvider
}

// Option configures Open.
type Option func(*clientOpts)

// WithLogger sets the logger used for connection lifecycle messages
// (nil or default: log.Default()).
func WithLogger(l *log.Logger) Option { return func(o *clientOpts) { o.logger = l } }

// WithTracerProvider sets the tracer provider (default: global).
func WithTracerProvider(tp trace.TracerProvider) Option { return func(o *clientOpts) { o.tp = tp } }

// WithMeterProvider sets the meter provider (default: global).
func WithMeterProvider(mp metric.MeterProvider) Option { return func(o *clientOpts) { o.mp = mp } }

// ClientOptions builds driver options from cfg without connecting and
// without instrumentation. Adjust them if needed, then call Connect.
func ClientOptions(cfg Config) (*options.ClientOptions, error) {
	if cfg.URI == "" {
		return nil, errors.New("mongox: URI is required")
	}
	co := options.Client().ApplyURI(cfg.URI)
	if err := co.Validate(); err != nil {
		return nil, fmt.Errorf("mongox: %w", err)
	}
	if co.TLSConfig == nil && !tlsChosen(cfg.URI) && !allLoopback(co.Hosts) {
		co.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	if cfg.AppName != "" {
		co.SetAppName(cfg.AppName)
	}
	if cfg.MaxPoolSize > 0 {
		co.SetMaxPoolSize(cfg.MaxPoolSize)
	}
	if cfg.MinPoolSize > 0 {
		co.SetMinPoolSize(cfg.MinPoolSize)
	}
	if cfg.MinPoolSize > 0 && co.MaxPoolSize != nil && *co.MaxPoolSize > 0 && cfg.MinPoolSize > *co.MaxPoolSize {
		return nil, fmt.Errorf("mongox: MinPoolSize %d > MaxPoolSize %d", cfg.MinPoolSize, *co.MaxPoolSize)
	}
	co.SetMaxConnIdleTime(cmp.Or(cfg.MaxConnIdleTime, 5*time.Minute))
	co.SetConnectTimeout(cmp.Or(cfg.ConnectTimeout, 5*time.Second))
	co.SetServerSelectionTimeout(cmp.Or(cfg.ServerSelectionTimeout, 5*time.Second))
	if cfg.Timeout > 0 {
		co.SetTimeout(cfg.Timeout)
	}
	return co, nil
}

// tlsChosen reports whether the URI sets tls or ssl explicitly.
func tlsChosen(uri string) bool {
	if strings.HasPrefix(uri, "mongodb+srv://") {
		return true // SRV implies tls=true unless the URI says otherwise
	}
	i := strings.IndexByte(uri, '?')
	if i < 0 {
		return false
	}
	q, err := url.ParseQuery(uri[i+1:])
	if err != nil {
		return false
	}
	for k := range q {
		if k = strings.ToLower(k); k == "tls" || k == "ssl" {
			return true
		}
	}
	return false
}

func allLoopback(hosts []string) bool {
	if len(hosts) == 0 {
		return false
	}
	for _, h := range hosts {
		host, _, err := net.SplitHostPort(h)
		if err != nil {
			host = h
		}
		if host == "localhost" {
			continue
		}
		if ip := net.ParseIP(strings.Trim(host, "[]")); ip == nil || !ip.IsLoopback() {
			return false
		}
	}
	return true
}

// Open builds options from cfg, instruments the client, connects and pings
// the primary. On error no client is left connected. The caller must
// Disconnect the client.
func Open(ctx context.Context, cfg Config, opts ...Option) (*mongo.Client, error) {
	co, err := ClientOptions(cfg)
	if err != nil {
		return nil, err
	}
	return Connect(ctx, co, opts...)
}

// Connect is Open for options built by ClientOptions (or by hand). It sets
// the command and pool monitors unless co already has them.
func Connect(ctx context.Context, co *options.ClientOptions, opts ...Option) (*mongo.Client, error) {
	o := clientOpts{logger: log.Default(), tp: otel.GetTracerProvider(), mp: otel.GetMeterProvider()}
	for _, opt := range opts {
		opt(&o)
	}
	o.logger = log.OrDefault(o.logger)
	in, err := newInstrumentation(o.tp, o.mp)
	if err != nil {
		return nil, fmt.Errorf("mongox: instruments: %w", err)
	}
	if co.Monitor == nil {
		co.SetMonitor(in.commandMonitor())
	}
	if co.PoolMonitor == nil {
		co.SetPoolMonitor(in.poolMonitor()) //nolint:contextcheck // driver pool events carry no context.
	}
	client, err := mongo.Connect(co)
	if err != nil {
		return nil, fmt.Errorf("mongox: connect: %w", err)
	}
	pctx := ctx
	if co.ServerSelectionTimeout != nil {
		var cancel context.CancelFunc
		pctx, cancel = context.WithTimeout(ctx, *co.ServerSelectionTimeout)
		defer cancel()
	}
	if err := client.Ping(pctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("mongox: ping %v: %w", co.Hosts, err)
	}
	o.logger.InfoContext(ctx, "mongox: connected", "hosts", co.Hosts, "tls", co.TLSConfig != nil)
	return client, nil
}
