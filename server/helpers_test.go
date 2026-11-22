package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/log"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// healthGateway maps grpc.health.v1 to REST the way generated
// RegisterXxxHandler code does: GET /v1/health and GET /v1/health/watch.
type healthGateway struct{}

func (healthGateway) RegisterGRPC(grpc.ServiceRegistrar) {} // the server registers health itself

func (healthGateway) RegisterGateway(_ context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error {
	c := healthpb.NewHealthClient(conn)
	if err := mux.HandlePath(http.MethodGet, "/v1/health", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		_, outbound := runtime.MarshalerForRequest(mux, r)
		ctx, err := runtime.AnnotateContext(r.Context(), mux, r, "/grpc.health.v1.Health/Check", runtime.WithHTTPPathPattern("/v1/health"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outbound, w, r, err)
			return
		}
		var md runtime.ServerMetadata
		resp, err := c.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.Header(&md.HeaderMD), grpc.Trailer(&md.TrailerMD))
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outbound, w, r, err)
			return
		}
		runtime.ForwardResponseMessage(ctx, mux, outbound, w, r, resp)
	}); err != nil {
		return err
	}
	return mux.HandlePath(http.MethodGet, "/v1/health/watch", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		_, outbound := runtime.MarshalerForRequest(mux, r)
		ctx, err := runtime.AnnotateContext(r.Context(), mux, r, "/grpc.health.v1.Health/Watch", runtime.WithHTTPPathPattern("/v1/health/watch"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outbound, w, r, err)
			return
		}
		stream, err := c.Watch(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			runtime.HTTPError(ctx, mux, outbound, w, r, err)
			return
		}
		header, err := stream.Header()
		if err != nil {
			runtime.HTTPError(ctx, mux, outbound, w, r, err)
			return
		}
		ctx = runtime.NewServerMetadataContext(ctx, runtime.ServerMetadata{HeaderMD: header})
		runtime.ForwardResponseStream(ctx, mux, outbound, w, r, func() (proto.Message, error) { return stream.Recv() })
	})
}

type running struct {
	srv      *Server
	grpcAddr string
	httpURL  string
	adminURL string
	conn     *grpc.ClientConn
	cancel   context.CancelFunc
	done     chan error
	t        *testing.T
	stopped  bool
}

func quietLogger() *log.Logger { return log.Discard() }

func listen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// start runs a Server on loopback ports and stops it at test cleanup.
func start(t *testing.T, opts ...Option) *running {
	t.Helper()
	gl, hl, al := listen(t), listen(t), listen(t)
	all := append([]Option{
		WithLogger(quietLogger()),
		WithGRPCListener(gl), WithHTTPListener(hl), WithAdminListener(al),
		WithShutdownTimeout(5 * time.Second),
	}, opts...)
	srv, err := New(all...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{
		srv: srv, grpcAddr: gl.Addr().String(),
		httpURL: "http://" + hl.Addr().String(), adminURL: "http://" + al.Addr().String(),
		cancel: cancel, done: make(chan error, 1), t: t,
	}
	go func() { r.done <- srv.Run(ctx) }()
	conn, err := grpc.NewClient(r.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	r.conn = conn
	waitServing(t, r)
	t.Cleanup(func() {
		_ = r.stop()
		_ = conn.Close()
	})
	return r
}

func waitServing(t *testing.T, r *running) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		// Started: Run marked the server ready and gRPC answers. The gRPC
		// status itself may be NOT_SERVING when a readiness check fails.
		_, err := healthpb.NewHealthClient(r.conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
		if err == nil && r.srv.health.ready.Load() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not become ready")
}

func (r *running) stop() error {
	if r.stopped {
		return nil
	}
	r.stopped = true
	r.cancel()
	select {
	case err := <-r.done:
		return err
	case <-time.After(10 * time.Second):
		r.t.Fatal("Run did not return")
		return nil
	}
}

func get(t *testing.T, url string, header http.Header) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := noKeepAlive.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

// noKeepAlive does not leave idle connection goroutines behind for goleak.
var noKeepAlive = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
