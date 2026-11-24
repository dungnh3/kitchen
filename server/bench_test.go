package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"runtime"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// chainHandler composes the server's unary interceptor chain around h, the
// way grpc.ChainUnaryInterceptor does.
func chainHandler(s *Server, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) grpc.UnaryHandler {
	unary, _ := s.chain()
	for i := len(unary) - 1; i >= 0; i-- {
		next, ic := h, unary[i]
		h = func(ctx context.Context, req any) (any, error) { return ic(ctx, req, info, next) }
	}
	return h
}

// BenchmarkUnaryChain measures the interceptor chain alone (recovery,
// request info, drain, logging, errors, validation) for one call from an
// untrusted TCP peer carrying typical client metadata.
func BenchmarkUnaryChain(b *testing.B) {
	srv, err := New(WithLogger(quietLogger()), WithGRPCAddr(""), WithHTTPListener(listenB(b)))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = srv.conn.Close() })
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Svc/Get"}
	resp := &healthpb.HealthCheckResponse{}
	h := chainHandler(srv, info, func(context.Context, any) (any, error) { return resp, nil })
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 1, 2, 3), Port: 4567}})
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(
		"x-request-id", "req-123", "user-agent", "grpc-go/1.0", "x-platform", "ios", "authorization", "Bearer x"))
	req := &healthpb.HealthCheckRequest{Service: "svc"}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := h(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGRPCUnary is one unary call over loopback TCP through the full
// public server (stats handler, interceptors, health service).
func BenchmarkGRPCUnary(b *testing.B) {
	r := startB(b)
	c := healthpb.NewHealthClient(r.conn)
	ctx := context.Background()
	req := &healthpb.HealthCheckRequest{}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := c.Check(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRESTUnary is one REST call through the HTTP middleware, the
// gateway, the in-process connection and the interceptors.
func BenchmarkRESTUnary(b *testing.B) {
	r := startB(b, WithServices(healthGateway{}))
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}}
	b.Cleanup(client.CloseIdleConnections)
	url := r.httpURL + "/v1/health"
	b.ReportAllocs()
	for b.Loop() {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		resp, err := client.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b.Fatal(resp.Status)
		}
	}
}

func listenB(b *testing.B) net.Listener {
	b.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	return l
}

// startB is start for benchmarks.
func startB(b *testing.B, opts ...Option) *running {
	b.Helper()
	gl, hl := listenB(b), listenB(b)
	srv, err := New(append([]Option{WithLogger(quietLogger()), WithGRPCListener(gl), WithHTTPListener(hl)}, opts...)...)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	conn, err := grpc.NewClient(gl.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatal(err)
	}
	for !srv.health.ready.Load() {
		runtime.Gosched()
	}
	b.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			b.Error(err)
		}
		_ = conn.Close()
	})
	return &running{srv: srv, conn: conn, httpURL: "http://" + hl.Addr().String()}
}
