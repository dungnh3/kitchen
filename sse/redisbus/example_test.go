package redisbus_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/envelope"
	"github.com/dungnh3/kitchen/sse/redisbus"
)

// Writers append with a Producer; End marks the stream finished.
func ExampleProducer() {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	p, _ := redisbus.NewProducer(rdb, redisbus.ProducerConfig{Keyspace: redisbus.Keyspace{Prefix: "sse:", Buckets: 64}})
	ctx := context.Background()
	_, _ = p.Append(ctx, "run_1", sse.Message{Type: sse.TypeToken, Data: []byte("Hello")})
	_, _ = p.End(ctx, "run_1")
	_, err := p.Append(ctx, "run_1", sse.Message{Type: sse.TypeToken, Data: []byte("late")})
	fmt.Println(err)
	// Output: redisbus: stream has ended: run_1
}

// Any consumer (gRPC, workers) can range over a stream through the hub.
func ExampleHub_Subscribe() {
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	p, _ := redisbus.NewProducer(rdb, redisbus.ProducerConfig{})
	hub, _ := redisbus.NewHub(rdb, redisbus.HubConfig{})
	defer hub.Close()

	ctx := context.Background()
	for _, tok := range []string{"Hel", "lo"} {
		_, _ = p.Append(ctx, "run_1", sse.Message{Type: sse.TypeToken, Data: []byte(tok)})
	}
	_, _ = p.End(ctx, "run_1")

	for m, err := range hub.Subscribe(ctx, "run_1", "") {
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Println(m.Type, string(m.Data))
	}
	// Output:
	// stream.token Hel
	// stream.token lo
}

// A gateway wires Hub → sse.Server → HTTP.
func Example_gateway() {
	var rdb redis.UniversalClient = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	hub, _ := redisbus.NewHub(rdb, redisbus.HubConfig{})
	srv, _ := sse.NewServer(hub, sse.ServerConfig{MaxConnections: 10_000, MaxConnectionsPerStream: 20})

	visible := sse.VisibleTypes("run.", "step.", "approval.")
	http.HandleFunc("GET /api/v1/runs/{run_id}/stream", func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("run_id")
		ticket, err := srv.Admit(r.Context(), runID)
		if err != nil {
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		err = srv.Serve(w, r, runID, sse.ServeOptions{
			Ticket:  ticket,
			Encoder: envelope.NewEncoder,
			Visible: visible,
			Transforms: []sse.TransformFactory{envelope.ContextValidator(envelope.ValidatorConfig{
				Resolve: func(context.Context) envelope.Rule {
					return envelope.Rule{SessionIDs: []string{runID}} // plus user/tenant from the JWT
				},
			})},
		})
		if errors.Is(err, sse.ErrInvalidCursor) {
			http.Error(w, "bad Last-Event-ID", http.StatusBadRequest)
		}
	})
	// go hub.Run(ctx); defer srv.Shutdown(ctx)
}
