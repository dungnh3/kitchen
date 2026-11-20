// Package redisx opens an instrumented go-redis client from a plain config.
//
// The returned [redis.UniversalClient] is a *redis.Client, *redis.ClusterClient
// or *redis.Client backed by Sentinel, chosen from [Config]:
//
//   - MasterName set           → Sentinel (Addrs are sentinel addresses)
//   - Cluster or len(Addrs) > 1 → Redis Cluster
//   - otherwise                → single node
//
// Tracing and metrics use redisotel with the given (or global) providers.
package redisx

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Config describes how to reach Redis. Zero values use go-redis defaults
// unless stated otherwise.
type Config struct {
	// Addrs are "host:port" addresses. Required.
	Addrs []string
	// MasterName selects Sentinel mode.
	MasterName string
	// Cluster forces Cluster mode even with a single seed address.
	Cluster bool
	// Username and Password authenticate to Redis (ACL or requirepass).
	Username, Password string
	// SentinelUsername and SentinelPassword authenticate to Sentinel.
	SentinelUsername, SentinelPassword string
	// DB is the database number (single node and Sentinel only).
	DB int
	// TLS enables TLS when non-nil.
	TLS *tls.Config
	// PoolSize is the max connections per node (default 10 × GOMAXPROCS).
	PoolSize int
	// MinIdleConns keeps this many idle connections per node.
	MinIdleConns int
	// DialTimeout default 5s; ReadTimeout default 3s; WriteTimeout defaults
	// to ReadTimeout.
	DialTimeout, ReadTimeout, WriteTimeout time.Duration
	// PingTimeout bounds the connectivity check in Open (default 5s).
	PingTimeout time.Duration
}

type options struct {
	tp      trace.TracerProvider
	mp      metric.MeterProvider
	noTrace bool
}

// Option configures Open.
type Option func(*options)

// WithTracerProvider sets the tracer provider (default: global).
func WithTracerProvider(tp trace.TracerProvider) Option { return func(o *options) { o.tp = tp } }

// WithMeterProvider sets the meter provider (default: global).
func WithMeterProvider(mp metric.MeterProvider) Option { return func(o *options) { o.mp = mp } }

// WithoutTracing disables per-command spans (metrics stay on).
func WithoutTracing() Option { return func(o *options) { o.noTrace = true } }

// UniversalOptions converts cfg to go-redis options, for callers who need to
// tweak fields Config does not expose before calling redis.NewUniversalClient.
func (cfg Config) UniversalOptions() (*redis.UniversalOptions, error) {
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("redisx: at least one address is required")
	}
	return &redis.UniversalOptions{
		Addrs:            cfg.Addrs,
		MasterName:       cfg.MasterName,
		IsClusterMode:    cfg.Cluster,
		Username:         cfg.Username,
		Password:         cfg.Password,
		SentinelUsername: cfg.SentinelUsername,
		SentinelPassword: cfg.SentinelPassword,
		DB:               cfg.DB,
		TLSConfig:        cfg.TLS,
		PoolSize:         cfg.PoolSize,
		MinIdleConns:     cfg.MinIdleConns,
		DialTimeout:      cfg.DialTimeout,
		ReadTimeout:      cfg.ReadTimeout,
		WriteTimeout:     cfg.WriteTimeout,
	}, nil
}

// Open creates the client, instruments it and pings it. On error nothing is
// left open. The caller owns the client and must Close it.
func Open(ctx context.Context, cfg Config, opts ...Option) (redis.UniversalClient, error) {
	o := options{tp: otel.GetTracerProvider(), mp: otel.GetMeterProvider()}
	for _, opt := range opts {
		opt(&o)
	}
	uo, err := cfg.UniversalOptions()
	if err != nil {
		return nil, err
	}
	rdb := redis.NewUniversalClient(uo)
	fail := func(err error) (redis.UniversalClient, error) {
		_ = rdb.Close()
		return nil, err
	}
	if !o.noTrace {
		if err := redisotel.InstrumentTracing(rdb, redisotel.WithTracerProvider(o.tp)); err != nil {
			return fail(fmt.Errorf("redisx: tracing: %w", err))
		}
	}
	if err := redisotel.InstrumentMetrics(rdb, redisotel.WithMeterProvider(o.mp)); err != nil {
		return fail(fmt.Errorf("redisx: metrics: %w", err))
	}
	timeout := cfg.PingTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := rdb.Ping(pctx).Err(); err != nil {
		return fail(fmt.Errorf("redisx: ping %v: %w", cfg.Addrs, err))
	}
	return rdb, nil
}
