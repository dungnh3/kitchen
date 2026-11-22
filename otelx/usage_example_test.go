package otelx_test

import (
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dungnh3/kitchen/otelx"
)

// kitchen/server already instruments its HTTP and gRPC ports. Wrap your own
// handlers only when you serve HTTP outside server.
func ExampleHTTPHandler() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(_ http.ResponseWriter, r *http.Request) {
		_, span := otelx.Start(r.Context(), "load order")
		defer span.End()
	})
	// Internal traffic: continue the caller's trace.
	internal := otelx.HTTPHandler(mux, "orders.http")
	// Internet-facing: start a new trace and only link the caller's, so a
	// client cannot choose your trace IDs or force sampling.
	public := otelx.PublicHTTPHandler(mux, "orders.public")
	_, _ = internal, public
}

// Outgoing calls: kitchen/httpx already traces HTTP. Use these for gRPC
// clients and for third-party SDKs that accept a RoundTripper.
func ExampleGRPCClientHandler() {
	conn, err := grpc.NewClient("billing:9090",
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TLS in production
		grpc.WithStatsHandler(otelx.GRPCClientHandler()))
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	rt := otelx.HTTPTransport(http.DefaultTransport) // for an SDK's custom transport option
	_ = rt
}
