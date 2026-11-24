package server

import (
	"context"
	"fmt"
	"net/http"
	"reflect"

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

// fanoutRegistrar registers every service on several grpc.Servers. It
// rejects what grpc-go would answer with logger.Fatalf (an os.Exit from
// inside New): a service registered twice, or an implementation that does
// not implement the service's handler type. The first such error is kept
// for New to return.
type fanoutRegistrar struct {
	servers []*grpc.Server
	err     error
}

func (f *fanoutRegistrar) RegisterService(desc *grpc.ServiceDesc, impl any) {
	if f.err != nil {
		return
	}
	if impl != nil && desc.HandlerType != nil {
		ht := reflect.TypeOf(desc.HandlerType).Elem()
		if !reflect.TypeOf(impl).Implements(ht) {
			f.err = fmt.Errorf("server: register %s: %T does not implement %v", desc.ServiceName, impl, ht)
			return
		}
	}
	for _, s := range f.servers {
		if _, dup := s.GetServiceInfo()[desc.ServiceName]; dup {
			f.err = fmt.Errorf("server: service %s registered twice", desc.ServiceName)
			return
		}
	}
	for _, s := range f.servers {
		s.RegisterService(desc, impl)
	}
}
