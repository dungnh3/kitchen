package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"buf.build/go/protovalidate"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/stats"
)

// ErrAlreadyStarted is returned by Run when it is called more than once.
var ErrAlreadyStarted = errors.New("server: already started")

// Server is a gRPC server with a REST gateway, health checks and an optional
// admin port. Create it with New and run it with Run.
type Server struct {
	cfg        *config
	log        *slog.Logger
	propagator propagation.TextMapPropagator
	validator  protovalidate.Validator

	public   *grpc.Server // gRPC port
	internal *grpc.Server // in-process, backs the REST gateway
	inproc   *inprocListener
	conn     *grpc.ClientConn
	grpcHC   *health.Server
	health   *probes
	httpSrv  *http.Server // nil when the HTTP port is disabled
	admin    *Admin       // nil when the admin port is disabled

	drain        chan struct{}
	started      atomic.Bool
	shuttingDown atomic.Bool
}

// New builds a Server and registers every service. It does not listen; Run
// does.
func New(opts ...Option) (*Server, error) {
	cfg := newConfig(opts)
	s := &Server{
		cfg:        cfg,
		log:        cfg.logger,
		propagator: cfg.propagator,
		drain:      make(chan struct{}),
		inproc:     newInprocListener(),
		grpcHC:     health.NewServer(),
		health:     newProbes(cfg.checks, cfg.version),
	}
	if s.propagator == nil {
		s.propagator = otel.GetTextMapPropagator()
	}
	if cfg.validation {
		v, err := protovalidate.New()
		if err != nil {
			return nil, fmt.Errorf("server: protovalidate: %w", err)
		}
		s.validator = v
	}
	s.grpcHC.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	s.public = grpc.NewServer(s.grpcServerOptions(true)...)
	s.internal = grpc.NewServer(s.grpcServerOptions(false)...)
	healthpb.RegisterHealthServer(s.public, s.grpcHC)
	healthpb.RegisterHealthServer(s.internal, s.grpcHC)
	reg := fanoutRegistrar{s.public, s.internal}
	for _, svc := range cfg.services {
		svc.RegisterGRPC(reg)
	}
	if cfg.reflection {
		reflection.Register(s.public)
	}

	conn, err := grpc.NewClient("passthrough:///"+inprocNetwork,
		grpc.WithContextDialer(s.inproc.dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(cfg.maxMessageSize),
			grpc.MaxCallSendMsgSize(cfg.maxMessageSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("server: gateway connection: %w", err)
	}
	s.conn = conn

	if cfg.httpAddr != "" || cfg.httpLis != nil {
		h, err := s.httpHandler()
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		s.httpSrv = &http.Server{
			Handler:           h,
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       650 * time.Second,
			MaxHeaderBytes:    1 << 20,
			ErrorLog:          slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
			Protocols:         new(http.Protocols),
		}
		s.httpSrv.Protocols.SetHTTP1(true)
		s.httpSrv.Protocols.SetUnencryptedHTTP2(true)
		if cfg.httpServerConfig != nil {
			cfg.httpServerConfig(s.httpSrv)
		}
	}
	if cfg.adminAddr != "" || cfg.adminLis != nil {
		var ac adminConfig
		for _, o := range cfg.adminOpts {
			o(&ac)
		}
		ac.logger = s.log
		if cfg.adminLis != nil {
			ac.lis = cfg.adminLis
		}
		s.admin = newAdmin(cfg.adminAddr, ac, s.health)
	}
	return s, nil
}

func (s *Server) grpcServerOptions(public bool) []grpc.ServerOption {
	unary, stream := s.chain()
	otelOpts := []otelgrpc.Option{
		otelgrpc.WithFilter(func(i *stats.RPCTagInfo) bool { return !isHealthMethod(i.FullMethodName) }),
		otelgrpc.WithPropagators(s.propagator),
	}
	if s.cfg.tracerProvider != nil {
		otelOpts = append(otelOpts, otelgrpc.WithTracerProvider(s.cfg.tracerProvider))
	}
	if s.cfg.meterProvider != nil {
		otelOpts = append(otelOpts, otelgrpc.WithMeterProvider(s.cfg.meterProvider))
	}
	opts := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelOpts...)),
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
		grpc.MaxRecvMsgSize(s.cfg.maxMessageSize),
		grpc.MaxSendMsgSize(s.cfg.maxMessageSize),
	}
	if public {
		opts = append(opts,
			grpc.MaxConcurrentStreams(s.cfg.maxConcurrentStreams),
			grpc.KeepaliveParams(s.cfg.keepaliveParams),
			grpc.KeepaliveEnforcementPolicy(s.cfg.keepalivePolicy),
		)
	}
	return append(opts, s.cfg.grpcOpts...)
}

// httpHandler composes the HTTP port, outermost first:
//
//	legacy probe paths (bypass everything)
//	otelhttp → recovery → drain → sanitize → access log → CORS → user middlewares
//	  → ServeMux: HTTPRegistrar patterns, "/" → body limit → gateway
func (s *Server) httpHandler() (http.Handler, error) {
	gw := s.newGatewayMux()
	for _, svc := range s.cfg.services {
		if g, ok := svc.(GatewayRegistrar); ok {
			if err := g.RegisterGateway(context.Background(), gw, s.conn); err != nil {
				return nil, fmt.Errorf("server: register gateway for %T: %w", svc, err)
			}
		}
	}
	mux := http.NewServeMux()
	for _, svc := range s.cfg.services {
		if h, ok := svc.(HTTPRegistrar); ok {
			h.RegisterHTTP(mux)
		}
	}
	mux.Handle("/", limitBody(int64(s.cfg.maxMessageSize), gw))

	var h http.Handler = mux
	for i := len(s.cfg.httpMiddlewares) - 1; i >= 0; i-- {
		h = s.cfg.httpMiddlewares[i](h)
	}
	if s.cfg.cors != nil {
		h = corsHandler(*s.cfg.cors, h)
	}
	h = s.accessLog(h)
	h = s.sanitize(h)
	h = drainHTTP(s.drain, h)
	h = s.recoverHTTP(h)
	otelOpts := []otelhttp.Option{
		otelhttp.WithPropagators(s.propagator),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method }),
	}
	if s.cfg.tracerProvider != nil {
		otelOpts = append(otelOpts, otelhttp.WithTracerProvider(s.cfg.tracerProvider))
	}
	if s.cfg.meterProvider != nil {
		otelOpts = append(otelOpts, otelhttp.WithMeterProvider(s.cfg.meterProvider))
	}
	h = otelhttp.NewHandler(h, "http.server", otelOpts...)

	if !s.cfg.legacyHealthPaths {
		return h, nil
	}
	legacy := s.health.legacyPaths()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			if lh, ok := legacy[r.URL.Path]; ok {
				lh(w, r)
				return
			}
		}
		h.ServeHTTP(w, r)
	}), nil
}

// ShuttingDown reports whether Run has begun shutting down.
func (s *Server) ShuttingDown() bool { return s.shuttingDown.Load() }

// Run listens on the configured ports and serves until ctx is cancelled or a
// listener fails, then shuts down (see the package doc) and returns. It
// returns nil after a shutdown caused by ctx, and the serve error otherwise.
// Run may be called once.
func (s *Server) Run(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	defer s.conn.Close()

	lis, err := s.listen()
	if err != nil {
		return err
	}

	errCh := make(chan error, 4)
	var wg sync.WaitGroup
	serve := func(name string, f func() error) {
		wg.Go(func() {
			if err := f(); err != nil {
				errCh <- fmt.Errorf("server: %s: %w", name, err)
			}
		})
	}
	s.log.Info("grpc server listening", slog.String("addr", lis.grpc.Addr().String()))
	serve("grpc", func() error { return s.public.Serve(lis.grpc) })
	serve("grpc in-process", func() error { return s.internal.Serve(s.inproc) })
	if lis.http != nil {
		s.log.Info("http server listening", slog.String("addr", lis.http.Addr().String()))
		serve("http", func() error { return ignoreClosed(s.httpSrv.Serve(lis.http)) })
	}
	if lis.admin != nil {
		serve("admin", func() error { return s.admin.serve(lis.admin) })
	}
	s.grpcHC.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	s.health.ready.Store(true)

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errCh:
		s.log.Error("listener failed, shutting down", slog.String("error", runErr.Error()))
	}
	s.shutdown(context.WithoutCancel(ctx))
	wg.Wait()
	close(errCh)
	for err := range errCh {
		runErr = errors.Join(runErr, err)
	}
	return runErr
}

type listeners struct{ grpc, http, admin net.Listener }

func (s *Server) listen() (l listeners, err error) {
	defer func() {
		if err != nil {
			for _, c := range []net.Listener{l.grpc, l.http, l.admin} {
				if c != nil {
					_ = c.Close()
				}
			}
		}
	}()
	open := func(given net.Listener, addr, name string) (net.Listener, error) {
		if given != nil {
			return given, nil
		}
		var lc net.ListenConfig
		ln, err := lc.Listen(context.Background(), "tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("server: %s listen %s: %w", name, addr, err)
		}
		return ln, nil
	}
	if l.grpc, err = open(s.cfg.grpcLis, s.cfg.grpcAddr, "grpc"); err != nil {
		return l, err
	}
	if s.httpSrv != nil {
		if l.http, err = open(s.cfg.httpLis, s.cfg.httpAddr, "http"); err != nil {
			return l, err
		}
	}
	if s.admin != nil {
		if l.admin, err = s.admin.listen(); err != nil {
			return l, err
		}
	}
	return l, nil
}

func ignoreClosed(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// shutdown runs the ordered shutdown within the shutdown timeout:
//
//  1. mark not ready (readiness probes, grpc.health.v1 NOT_SERVING)
//  2. wait the shutdown delay
//  3. close the Draining channel
//  4. drain HTTP then the in-process gRPC server, and the public gRPC server
//     concurrently; force-close what is left at the deadline
//  5. Close services with the remaining budget
//  6. stop the admin port
func (s *Server) shutdown(base context.Context) {
	start := time.Now()
	s.log.Info("server shutting down")
	ctx, cancel := context.WithTimeout(base, s.cfg.shutdownTimeout)
	defer cancel()

	s.shuttingDown.Store(true)
	s.health.ready.Store(false)
	s.grpcHC.Shutdown()

	if d := s.cfg.shutdownDelay; d > 0 {
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
		}
	}
	close(s.drain)

	var wg sync.WaitGroup
	wg.Go(func() {
		if s.httpSrv != nil {
			if err := s.httpSrv.Shutdown(ctx); err != nil {
				s.log.Warn("http drain timed out, closing connections", slog.String("error", err.Error()))
				_ = s.httpSrv.Close()
			}
		}
		// The gateway uses the in-process server until HTTP has drained.
		stopGRPC(ctx, s.internal)
	})
	wg.Go(func() {
		if !stopGRPC(ctx, s.public) {
			s.log.Warn("grpc drain timed out, closing connections")
		}
	})
	wg.Wait()

	for _, svc := range s.cfg.services {
		if c, ok := svc.(Closer); ok {
			if err := c.Close(ctx); err != nil {
				s.log.Error("service close failed", slog.String("service", fmt.Sprintf("%T", svc)), slog.String("error", err.Error()))
			}
		}
	}
	if s.admin != nil {
		s.admin.shutdown(ctx)
	}
	s.log.Info("server stopped", slog.Duration("duration", time.Since(start)))
}

// stopGRPC drains gs until ctx is done, then stops it. It reports whether the
// drain finished in time.
func stopGRPC(ctx context.Context, gs *grpc.Server) bool {
	done := make(chan struct{})
	go func() {
		gs.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		gs.Stop()
		<-done
		return false
	}
}
