package server

import (
	"context"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
)

// Service is a gRPC service mounted on a Server. RegisterGRPC is called once;
// the registrar mounts the service on both the public gRPC server and the
// in-process server that backs the REST gateway.
//
// A Service may also implement [GatewayRegistrar], [HTTPRegistrar] and
// [Closer].
type Service interface {
	RegisterGRPC(grpc.ServiceRegistrar)
}

// GatewayRegistrar registers grpc-gateway handlers, normally the generated
// RegisterXxxHandler(ctx, mux, conn). conn is the in-process connection to
// this server.
type GatewayRegistrar interface {
	RegisterGateway(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error
}

// HTTPRegistrar registers plain net/http handlers (uploads, SSE, webhooks)
// with method patterns such as "GET /v1/files/{id}". They take precedence
// over the gateway mapping and get the request info, drain channel, logging,
// tracing and recovery middleware, but not the gateway body limit.
type HTTPRegistrar interface {
	RegisterHTTP(mux *http.ServeMux)
}

// Closer is called during shutdown after in-flight requests have drained.
type Closer interface {
	Close(ctx context.Context) error
}

// fanoutRegistrar registers every service on several grpc.Servers.
type fanoutRegistrar []*grpc.Server

func (f fanoutRegistrar) RegisterService(desc *grpc.ServiceDesc, impl any) {
	for _, s := range f {
		s.RegisterService(desc, impl)
	}
}
