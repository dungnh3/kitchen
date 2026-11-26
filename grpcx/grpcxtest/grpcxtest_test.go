package grpcxtest_test

import (
	"context"
	"testing"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/dungnh3/kitchen/grpcx"
	"github.com/dungnh3/kitchen/grpcx/grpcxtest"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type authHealth struct {
	healthpb.UnimplementedHealthServer
	got chan string
}

func (a authHealth) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	a.got <- md.Get("authorization")[0]
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func TestServers(t *testing.T) {
	tests := []struct {
		name string
		tls  bool
	}{{"plaintext", false}, {"tls", true}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			register := func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }
			srv := grpcxtest.NewServer(t, register)
			if tt.tls {
				srv = grpcxtest.NewTLSServer(t, register)
			}
			res, err := healthpb.NewHealthClient(srv.Conn(t)).Check(t.Context(), &healthpb.HealthCheckRequest{})
			if err != nil || res.GetStatus() != healthpb.HealthCheckResponse_SERVING {
				t.Fatalf("Check: %v, %v", res, err)
			}
		})
	}
}

func TestTLSServerCarriesCredentials(t *testing.T) {
	a := authHealth{got: make(chan string, 1)}
	srv := grpcxtest.NewTLSServer(t, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, a) })
	conn := srv.Conn(t, grpcx.WithTokenSource(grpcx.StaticToken("tok")))
	if _, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	if got := <-a.got; got != "Bearer tok" {
		t.Errorf("authorization %q", got)
	}
}
