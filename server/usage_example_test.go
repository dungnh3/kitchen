package server_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/server"
	"google.golang.org/grpc"
)

// files is a Service with plain HTTP routes next to its gRPC API, and a
// Closer that runs after in-flight requests have drained.
type files struct{}

func (files) RegisterGRPC(grpc.ServiceRegistrar) {} // pb.RegisterFilesServer(r, f)

func (files) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/files/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "42" {
			errs.WriteHTTP(w, r, errs.ErrNotFound) // AIP-193 JSON, 404
			return
		}
		_, _ = io.WriteString(w, "file 42")
	})
}

func (files) Close(context.Context) error { return nil } // flush, close clients

// Example_service mounts a service, runs the server on ephemeral ports and
// calls it, as a test would.
func Example_service() {
	ctx, cancel := context.WithCancel(context.Background())
	grpcLis, _ := net.Listen("tcp", "127.0.0.1:0")
	httpLis, _ := net.Listen("tcp", "127.0.0.1:0")
	srv, err := server.New(
		server.WithServices(files{}),
		server.WithGRPCListener(grpcLis),
		server.WithHTTPListener(httpLis),
		server.WithLogger(log.Discard()),
	)
	if err != nil {
		panic(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	base := "http://" + httpLis.Addr().String()
	for _, path := range []string{"/v1/files/42", "/v1/files/7", "/readyz"} {
		resp, err := http.Get(base + path)
		if err != nil {
			panic(err)
		}
		_ = resp.Body.Close()
		fmt.Println(path, resp.StatusCode)
	}
	cancel()
	fmt.Println("run:", <-done)
	// Output:
	// /v1/files/42 200
	// /v1/files/7 404
	// /readyz 200
	// run: <nil>
}

// Example_httpOnly serves only HTTP routes (an MCP endpoint, webhooks):
// no public gRPC port, but the same probes, drain, logs and tracing.
func Example_httpOnly() {
	srv, err := server.New(
		server.WithGRPCAddr(""),
		server.WithServices(files{}),
	)
	if err != nil {
		panic(err)
	}
	_ = srv // srv.Run(ctx) in main
}

// Example_browser serves a browser front end on another origin.
func Example_browser() {
	srv, err := server.New(
		server.WithServices(files{}),
		server.WithCORS(server.CORSConfig{
			AllowedOrigins: []string{"https://app.example.com"},
			AllowedHeaders: []string{"Authorization", "Content-Type"},
			MaxAge:         10 * time.Minute,
		}),
		server.WithCompression(),         // when no CDN or ingress compresses
		server.WithStreamingMarshalers(), // server streams as SSE or NDJSON
	)
	if err != nil {
		panic(err)
	}
	_ = srv
}

// Example_tuning shows the knobs for high load and long shutdowns.
func Example_tuning() {
	srv, err := server.New(
		server.WithServices(files{}),
		server.WithMaxMessageSize(16<<20),          // default 4 MiB
		server.WithMaxConcurrentStreams(2000),      // default 1000
		server.WithShutdownTimeout(55*time.Second), // < terminationGracePeriodSeconds
		server.WithShutdownDelay(5*time.Second),    // let the LB stop routing first
		server.WithHTTPServerConfig(func(s *http.Server) {
			s.ReadTimeout = time.Minute // only when no route streams
		}),
	)
	if err != nil {
		panic(err)
	}
	_ = srv
}

// ExampleDraining ends a long-lived stream when shutdown starts, so the
// client reconnects to another instance instead of being cut at the
// shutdown timeout.
func ExampleDraining() {
	watch := func(ctx context.Context, send func(string) error) error {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-server.Draining(ctx):
				return errs.ErrServiceDraining // UNAVAILABLE: retry elsewhere
			case <-tick.C:
				if err := send("tick"); err != nil {
					return err
				}
			}
		}
	}
	_ = watch // call it from a server-streaming handler with stream.Context()
}
