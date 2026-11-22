package context_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/otelx"
	"github.com/dungnh3/kitchen/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func Example_server() {
	// The server builds Info for every request. Trust only the hops that
	// append to X-Forwarded-For: here a GCP load balancer and the cluster.
	proxies, err := xcontext.NewTrustedProxies("35.191.0.0/16", "130.211.0.0/22", "10.0.0.0/8")
	if err != nil {
		return
	}
	srv, err := server.New(
		server.WithTrustedProxies(proxies),
		server.WithForwardHeaders("X-Tenant-Id"), // also captured in Info.Headers
	)
	if err != nil {
		return
	}
	_ = srv
}

func ExampleFromHTTP() {
	proxies, _ := xcontext.NewTrustedProxies("10.0.0.0/8")
	r := httptest.NewRequest(http.MethodGet, "/v1/orders", nil)
	r.RemoteAddr = "10.0.0.5:51000" // the load balancer
	r.Header.Set("X-Forwarded-For", "203.0.113.7")
	r.Header.Set("X-Request-Id", "r-9")
	r.Header.Set("X-Tenant-Id", "acme")
	r.Header.Set("User-Agent", "MyApp/2.1 (iPhone; iOS 18.0)")

	info := xcontext.FromHTTP(r, xcontext.Options{TrustedProxies: proxies, Headers: []string{"X-Tenant-Id"}})
	ctx := xcontext.NewContext(r.Context(), info)

	fmt.Println(info.ClientIP, info.RequestID, info.Platform, xcontext.Header(ctx, "X-Tenant-Id"))
	// Output: 203.0.113.7 r-9 ios acme
}

func ExampleHeader() {
	// Credentials and identity headers are never returned, even when sent.
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-tenant-id", "acme", "authorization", "Bearer secret", "user-id", "admin"))
	fmt.Printf("%q %q %q\n", xcontext.Header(ctx, "X-Tenant-Id"), xcontext.Header(ctx, "Authorization"), xcontext.Header(ctx, "User-Id"))
	// Output: "acme" "" ""
}

func ExampleAppendToOutgoing() {
	ctx := xcontext.NewContext(context.Background(), xcontext.Info{RequestID: "r-1", DeviceID: "d-1"})
	ctx = xcontext.WithUserID(ctx, "u-42")

	ctx = xcontext.AppendToOutgoing(ctx)
	md, _ := metadata.FromOutgoingContext(ctx)
	fmt.Println(md.Get("x-request-id"), md.Get("user-id"), md.Get("x-device-id"))
	// Output: [r-1] [u-42] [d-1]
}

func ExampleUnaryClientInterceptor() {
	// Calls to other kitchen services carry the request id, user id and
	// client info; the callee accepts the user id only from trusted peers
	// (server.WithTrustedIdentityPeers).
	conn, err := grpc.NewClient("orders:9090",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelx.GRPCClientHandler()),
		grpc.WithChainUnaryInterceptor(xcontext.UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(xcontext.StreamClientInterceptor()),
	)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
}
