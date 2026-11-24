package server

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/log"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestRunCancelledBeforeServe: Run returns nil when its ctx is cancelled
// before the serve goroutines reach Serve.
func TestRunCancelledBeforeServe(t *testing.T) {
	for i := range 20 {
		srv, err := New(WithLogger(quietLogger()), WithGRPCListener(listen(t)), WithHTTPListener(listen(t)), WithAdminListener(listen(t)))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := srv.Run(ctx); err != nil {
			t.Fatalf("iteration %d: Run = %v, want nil", i, err)
		}
	}
}

// TestGatewayMethodNotAllowed: a known gateway path with the wrong method is
// 405 UNIMPLEMENTED and an unknown path 404 NOT_FOUND, neither logged at
// ERROR.
func TestGatewayMethodNotAllowed(t *testing.T) {
	var logs syncBuffer
	logger := log.New(&logs, log.Options{Level: log.LevelWarn})
	r := start(t, WithLogger(logger), WithServices(healthGateway{}))
	tests := []struct {
		method, path string
		code         int
		status       string
	}{
		{http.MethodDelete, "/v1/health", http.StatusMethodNotAllowed, "UNIMPLEMENTED"},
		{http.MethodGet, "/v1/nope", http.StatusNotFound, "NOT_FOUND"},
	}
	for _, tt := range tests {
		req, _ := http.NewRequestWithContext(context.Background(), tt.method, r.httpURL+tt.path, nil)
		resp, err := noKeepAlive.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		e := decodeEnvelope(t, string(b))
		if resp.StatusCode != tt.code || e.Error.Code != tt.code || e.Error.Status != tt.status {
			t.Errorf("%s %s = %d %s", tt.method, tt.path, resp.StatusCode, b)
		}
	}
	if out := logs.String(); strings.Contains(out, `"level":"ERROR"`) {
		t.Errorf("routing errors logged at ERROR: %s", out)
	}
}

// TestStreamErrorEnvelope: the error that ends a REST stream is the AIP-193
// envelope in SSE (event: error), NDJSON and JSON streams.
func TestStreamErrorEnvelope(t *testing.T) {
	for _, accept := range []string{"text/event-stream", "application/x-ndjson", "application/json"} {
		t.Run(accept, func(t *testing.T) {
			r := start(t, WithServices(healthGateway{}), WithStreamingMarshalers(), WithShutdownTimeout(2*time.Second))
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, r.httpURL+"/v1/health/watch", nil)
			req.Header.Set("Accept", accept)
			resp, err := noKeepAlive.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			br := bufio.NewReader(resp.Body)
			if _, err := br.ReadString('\n'); err != nil { // first message
				t.Fatal(err)
			}
			stopped := make(chan error, 1)
			go func() { stopped <- r.stop() }() // the Watch ends with SERVICE_DRAINING
			rest, _ := io.ReadAll(br)
			if err := <-stopped; err != nil {
				t.Fatal(err)
			}
			var last string
			for _, l := range strings.Split(strings.TrimSpace(string(rest)), "\n") {
				if l = strings.TrimSpace(l); l != "" {
					last = l
				}
			}
			if accept == "text/event-stream" {
				if !strings.Contains(string(rest), "event: error\ndata: {\"error\":") {
					t.Fatalf("SSE error event missing: %q", rest)
				}
				last = strings.TrimPrefix(last, "data: ")
			}
			e := decodeEnvelope(t, last)
			if e.Error.Code != http.StatusServiceUnavailable || e.Error.Status != "UNAVAILABLE" ||
				detail(e, "ErrorInfo")["reason"] != "SERVICE_DRAINING" {
				t.Fatalf("stream error = %q", last)
			}
		})
	}
}

// TestStreamErrorSanitized: a stream error without an ErrorInfo is
// sanitized; one the errs interceptor converted is kept.
func TestStreamErrorSanitized(t *testing.T) {
	srv, err := New(WithLogger(quietLogger()), WithGRPCAddr(""), WithHTTPListener(listen(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	st := srv.streamError(context.Background(), errors.New("dial tcp 10.0.0.1:5432: secret"))
	if st.Code() != codes.Internal || strings.Contains(st.Message(), "secret") {
		t.Errorf("raw error = %v", st)
	}
	kept := srv.errs.Status(context.Background(), status.Error(codes.NotFound, "gone"))
	if st := srv.streamError(context.Background(), kept.Err()); st.Code() != codes.NotFound || st.Message() != "gone" {
		t.Errorf("converted error = %v", st)
	}
}

// TestGRPCServerOptionsPublicOnly: WithGRPCServerOptions configures the
// public port only, so grpc.Creds there leaves REST working.
func TestGRPCServerOptionsPublicOnly(t *testing.T) {
	cert, pool := selfSigned(t)
	gl, hl := listen(t), listen(t)
	srv, err := New(WithLogger(quietLogger()), WithGRPCListener(gl), WithHTTPListener(hl), WithServices(healthGateway{}),
		WithGRPCServerOptions(grpc.Creds(credentials.NewServerTLSFromCert(&cert))), WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	stop := runServer(t, srv)
	defer stop()
	resp, body := get(t, "http://"+hl.Addr().String()+"/v1/health", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("REST with TLS gRPC options = %d %s", resp.StatusCode, body)
	}
	conn, err := grpc.NewClient(gl.Addr().String(), grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, "localhost")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("TLS gRPC = %v", err)
	}
}

// TestHTTPTLS: a TLSConfig set with WithHTTPServerConfig makes the HTTP port
// serve TLS (HTTP/1.1 and HTTP/2) and refuse plaintext; one without a
// certificate fails in New.
func TestHTTPTLS(t *testing.T) {
	cert, pool := selfSigned(t)
	hl := listen(t)
	srv, err := New(WithLogger(quietLogger()), WithGRPCAddr(""), WithHTTPListener(hl), WithShutdownTimeout(time.Second),
		WithHTTPServerConfig(func(s *http.Server) {
			s.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		}))
	if err != nil {
		t.Fatal(err)
	}
	stop := runServer(t, srv)
	defer stop()
	for _, h2 := range []bool{false, true} {
		tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}, DisableKeepAlives: true, ForceAttemptHTTP2: h2}
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+hl.Addr().String()+"/livez", nil)
		resp, err := (&http.Client{Transport: tr}).Do(req)
		if err != nil {
			t.Fatalf("https (h2=%v): %v", h2, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || (h2 && resp.ProtoMajor != 2) {
			t.Errorf("https (h2=%v) = %d %s", h2, resp.StatusCode, resp.Proto)
		}
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+hl.Addr().String()+"/livez", nil)
	if resp, err := noKeepAlive.Do(req); err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Error("plaintext request served on a TLS port")
		}
	}

	// A TLSConfig without a certificate fails in New, not at Run.
	_, err = New(WithLogger(quietLogger()), WithGRPCAddr(""), WithHTTPListener(listen(t)),
		WithHTTPServerConfig(func(s *http.Server) { s.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12} }))
	if err == nil {
		t.Error("New accepted a TLSConfig without certificates")
	}
}

// slowService is a unary RPC over a POST gateway route that takes a while.
type slowService struct{ delay time.Duration }

func (s slowService) RegisterGRPC(r grpc.ServiceRegistrar) {
	r.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Slow",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Do",
			Handler: func(_ any, ctx context.Context, dec func(any) error, ic grpc.UnaryServerInterceptor) (any, error) {
				in := new(structpb.Struct)
				if err := dec(in); err != nil {
					return nil, err
				}
				h := func(ctx context.Context, _ any) (any, error) {
					select {
					case <-time.After(s.delay):
						return &emptypb.Empty{}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return ic(ctx, in, &grpc.UnaryServerInfo{FullMethod: "/test.Slow/Do"}, h)
			},
		}},
	}, s)
}

func (slowService) RegisterGateway(_ context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error {
	return mux.HandlePath(http.MethodPost, "/v1/slow", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		inbound, outbound := runtime.MarshalerForRequest(mux, r)
		ctx, err := runtime.AnnotateContext(r.Context(), mux, r, "/test.Slow/Do", runtime.WithHTTPPathPattern("/v1/slow"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outbound, w, r, err)
			return
		}
		in := new(structpb.Struct)
		if err := inbound.NewDecoder(r.Body).Decode(in); err != nil && !errors.Is(err, io.EOF) {
			runtime.HTTPError(ctx, mux, outbound, w, r, status.Error(codes.InvalidArgument, err.Error()))
			return
		}
		out := new(emptypb.Empty)
		if err := conn.Invoke(ctx, "/test.Slow/Do", in, out); err != nil {
			runtime.HTTPError(ctx, mux, outbound, w, r, err)
			return
		}
		runtime.ForwardResponseMessage(ctx, mux, outbound, w, r, out)
	})
}

// TestGatewayBodyTimeout: a REST body sent too slowly is cut off with 408;
// a request whose body arrived in time may run longer than the timeout.
func TestGatewayBodyTimeout(t *testing.T) {
	r := start(t, WithServices(slowService{delay: 600 * time.Millisecond}), WithGatewayBodyTimeout(300*time.Millisecond))
	addr := strings.TrimPrefix(r.httpURL, "http://")

	// Slow body: the headers promise 100 bytes, 1 arrives.
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_, _ = io.WriteString(c, "POST /v1/slow HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{")
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("slow body: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("slow body = %d %s", resp.StatusCode, b)
	}

	// Same over h2c: the stream's body read times out.
	h2 := &http.Transport{Protocols: new(http.Protocols)}
	h2.Protocols.SetUnencryptedHTTP2(true)
	defer h2.CloseIdleConnections()
	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	go func() { _, _ = io.WriteString(pw, "{") }()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, r.httpURL+"/v1/slow", pr)
	req.Header.Set("Content-Type", "application/json")
	resp, err = (&http.Client{Transport: h2}).Do(req)
	if err != nil {
		t.Fatalf("h2c slow body: %v", err)
	}
	b, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("h2c slow body = %s %d %s", resp.Proto, resp.StatusCode, b)
	}

	// Body in time, RPC longer than the timeout: still 200 (HTTP/1.1
	// keep-alive, Content-Length and chunked).
	client := &http.Client{}
	defer client.CloseIdleConnections()
	for _, chunked := range []bool{false, true} {
		var body io.Reader = strings.NewReader(`{"a":1}`)
		if chunked {
			body = io.MultiReader(strings.NewReader(`{"a":1}`)) // unknown length
		}
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, r.httpURL+"/v1/slow", body)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("long RPC (chunked=%v) = %d %s", chunked, resp.StatusCode, b)
		}
	}
}

// TestClose: a Server that is never run releases its gateway connection
// (goleak in TestMain), and cannot be run afterwards.
func TestClose(t *testing.T) {
	srv, err := New(WithLogger(quietLogger()), WithGRPCListener(listen(t)), WithHTTPListener(listen(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}
	if err := srv.Run(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Run after Close = %v, want ErrClosed", err)
	}

	// Close while running does nothing; Run still cleans up.
	r := start(t)
	if err := r.srv.Close(); err != nil {
		t.Errorf("Close while running = %v", err)
	}
	if _, err := healthpb.NewHealthClient(r.conn).Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Errorf("server stopped by Close while running: %v", err)
	}
}

type midPanicHTTP struct{}

func (midPanicHTTP) RegisterGRPC(grpc.ServiceRegistrar) {}
func (midPanicHTTP) RegisterHTTP(m *http.ServeMux) {
	m.HandleFunc("GET /mid", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 100)))
		_ = http.NewResponseController(w).Flush()
		panic("mid-response")
	})
}

// TestPanicAfterWriteAborts: a panic after the response started aborts the
// connection, so the client sees a truncated body as an error.
func TestPanicAfterWriteAborts(t *testing.T) {
	r := start(t, WithServices(midPanicHTTP{}))
	c, err := net.Dial("tcp", strings.TrimPrefix(r.httpURL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_, _ = io.WriteString(c, "GET /mid HTTP/1.1\r\nHost: x\r\n\r\n")
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated body read err = %v, want unexpected EOF", err)
	}
}

type rootHTTP struct{}

func (rootHTTP) RegisterGRPC(grpc.ServiceRegistrar) {}
func (rootHTTP) RegisterHTTP(m *http.ServeMux) {
	m.HandleFunc("/", func(http.ResponseWriter, *http.Request) {})
}

type dupHealth struct{}

func (dupHealth) RegisterGRPC(r grpc.ServiceRegistrar) {
	healthpb.RegisterHealthServer(r, healthpb.UnimplementedHealthServer{})
}

type wrongImpl struct{}

func (wrongImpl) RegisterGRPC(r grpc.ServiceRegistrar) {
	r.RegisterService(&healthpb.Health_ServiceDesc, struct{}{})
}

// TestRegistrationErrors: New returns an error for conflicting HTTP patterns,
// a gRPC service registered twice and an implementation of the wrong type.
func TestRegistrationErrors(t *testing.T) {
	tests := []struct {
		name string
		svc  Service
	}{
		{"http root conflicts with the gateway", rootHTTP{}},
		{"duplicate gRPC service", dupHealth{}},
		{"implementation of the wrong type", wrongImpl{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, err := New(WithLogger(quietLogger()), WithGRPCListener(listen(t)), WithHTTPListener(listen(t)), WithServices(tt.svc))
			if err == nil {
				_ = srv.Close()
				t.Fatal("New accepted a conflicting registration")
			}
		})
	}
}

// TestGRPCHealthWaitsForChecks: grpc.health.v1 is NOT_SERVING until the
// first readiness run passes.
func TestGRPCHealthWaitsForChecks(t *testing.T) {
	release := make(chan struct{})
	r := start(t, WithReadiness(Check{Name: "db", Func: func(ctx context.Context) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}))
	resp, err := healthpb.NewHealthClient(r.conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Errorf("status before the first check finished = %v", resp.GetStatus())
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := healthpb.NewHealthClient(r.conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("never SERVING after the check passed")
}

type ipHTTP struct{ ip *atomic.Value }

func (ipHTTP) RegisterGRPC(grpc.ServiceRegistrar) {}
func (s ipHTTP) RegisterHTTP(m *http.ServeMux) {
	m.HandleFunc("GET /ip", func(_ http.ResponseWriter, r *http.Request) {
		info, _ := xcontext.FromContext(r.Context())
		s.ip.Store(info.ClientIP.String())
	})
}

// TestTrustedHTTPClientIP: a trusted peer's X-Kitchen-Client-Ip is the
// client IP on HTTP as on gRPC; from an untrusted peer it is ignored.
func TestTrustedHTTPClientIP(t *testing.T) {
	loop, err := xcontext.NewTrustedProxies("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		opts []Option
		want string
	}{
		{"trusted", []Option{WithTrustedIdentityPeers(loop)}, "203.0.113.9"},
		{"untrusted", nil, "127.0.0.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := &atomic.Value{}
			r := start(t, append(tt.opts, WithServices(ipHTTP{ip: ip}))...)
			get(t, r.httpURL+"/ip", http.Header{xcontext.HeaderClientIP: {"203.0.113.9"}})
			if got, _ := ip.Load().(string); got != tt.want {
				t.Errorf("client IP = %q, want %q", got, tt.want)
			}
		})
	}
}

// runServer runs srv until the returned stop is called.
func runServer(t *testing.T, srv *Server) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for !srv.health.ready.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	return func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run = %v", err)
		}
	}
}

// selfSigned returns a certificate for localhost / 127.0.0.1 and a pool
// that trusts it.
func selfSigned(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}
