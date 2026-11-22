package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/dungnh3/kitchen/log"
)

// AdminOption configures the admin port.
type AdminOption func(*adminConfig)

type adminConfig struct {
	pprof    bool
	handlers []adminHandler
	checks   []Check
	logger   *log.Logger
	version  string
	lis      net.Listener
}

type adminHandler struct {
	pattern string
	h       http.Handler
}

// WithPprof serves net/http/pprof under /debug/pprof/ on the admin port.
func WithPprof() AdminOption { return func(c *adminConfig) { c.pprof = true } }

// WithAdminHandler serves h at pattern on the admin port, for example an
// OpenTelemetry Prometheus exporter at "GET /metrics".
func WithAdminHandler(pattern string, h http.Handler) AdminOption {
	return func(c *adminConfig) { c.handlers = append(c.handlers, adminHandler{pattern, h}) }
}

// WithAdminChecks adds readiness checks to a standalone Admin.
func WithAdminChecks(checks ...Check) AdminOption {
	return func(c *adminConfig) { c.checks = append(c.checks, checks...) }
}

// WithAdminLogger sets the logger of a standalone Admin.
func WithAdminLogger(l *log.Logger) AdminOption { return func(c *adminConfig) { c.logger = l } }

// WithAdminVersion sets the version of a standalone Admin.
func WithAdminVersion(v string) AdminOption { return func(c *adminConfig) { c.version = v } }

// WithAdminServeListener serves a standalone Admin on l.
func WithAdminServeListener(l net.Listener) AdminOption { return func(c *adminConfig) { c.lis = l } }

// Admin serves /livez, /readyz, /version and the optional pprof
// and extra handlers. Use it standalone in worker binaries that have no
// gRPC server; a Server creates its own with [WithAdmin].
type Admin struct {
	addr    string
	cfg     adminConfig
	health  *probes
	srv     *http.Server
	started atomic.Bool
}

// NewAdmin returns an Admin that will listen on addr.
func NewAdmin(addr string, opts ...AdminOption) *Admin {
	var cfg adminConfig
	for _, o := range opts {
		o(&cfg)
	}
	return newAdmin(addr, cfg, newProbes(cfg.checks, cfg.version))
}

func newAdmin(addr string, cfg adminConfig, h *probes) *Admin {
	if cfg.logger == nil {
		cfg.logger = log.Default()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", h.livez)
	mux.HandleFunc("GET /readyz", h.readyz)
	mux.HandleFunc("GET /version", h.versionz)
	if cfg.pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	for _, ah := range cfg.handlers {
		mux.Handle(ah.pattern, ah.h)
	}
	return &Admin{
		addr:   addr,
		cfg:    cfg,
		health: h,
		srv: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       120 * time.Second,
			ErrorLog:          log.StdLogger(cfg.logger, log.LevelWarn),
		},
	}
}

func (a *Admin) listen(ctx context.Context) (net.Listener, error) {
	if a.cfg.lis != nil {
		return a.cfg.lis, nil
	}
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", a.addr)
	if err != nil {
		return nil, fmt.Errorf("server: admin listen %s: %w", a.addr, err)
	}
	return l, nil
}

// Run serves until ctx is cancelled, then shuts down within 5s. A standalone
// Admin reports ready while it runs.
func (a *Admin) Run(ctx context.Context) error {
	if !a.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	l, err := a.listen(ctx)
	if err != nil {
		return err
	}
	a.health.ready.Store(true)
	errCh := make(chan error, 1)
	go func() { errCh <- a.serve(l) }()
	select {
	case <-ctx.Done():
		a.health.ready.Store(false)
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		a.shutdown(sctx)
		return <-errCh
	case err := <-errCh:
		return err
	}
}

func (a *Admin) serve(l net.Listener) error {
	a.cfg.logger.Info("admin server listening", log.String("addr", l.Addr().String()))
	if err := a.srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server: admin: %w", err)
	}
	return nil
}

func (a *Admin) shutdown(ctx context.Context) {
	if err := a.srv.Shutdown(ctx); err != nil {
		_ = a.srv.Close()
	}
}
