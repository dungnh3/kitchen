package redisbus_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/alicebob/miniredis/v2"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/redisx"
	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/envelope"
	"github.com/dungnh3/kitchen/sse/redisbus"
)

// Writers append with a Producer; End marks the stream finished.
func ExampleProducer() {
	mr, _ := miniredis.Run()
	defer mr.Close()
	ctx := context.Background()
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = rdb.Close() }()

	p, _ := redisbus.NewProducer(rdb, redisbus.ProducerConfig{
		Keyspace: redisbus.Keyspace{Prefix: "sse:", Buckets: 64}, // same on every reader
	})
	e, _ := envelope.NewBuilder("run_1", "chat", "user_1").BuildText(sse.TypeToken, "Hello")
	msg, _ := e.Message()
	if _, err := p.Append(ctx, "run_1", msg); err != nil {
		fmt.Println(err)
	}
	_, _ = p.End(ctx, "run_1")
	_, err = p.Append(ctx, "run_1", msg)
	fmt.Println(errors.Is(err, redisbus.ErrStreamEnded), err)
	// Output: true SSE_STREAM_ENDED: stream has ended: run_1
}

// The outbox leg feeds relay.Relay. Without OutboxClient (mirror mode) it is
// written atomically with the stream entry. With OutboxClient (split mode) it
// goes to another Redis after the stream write, best effort.
func ExampleProducer_outbox() {
	ctx := context.Background()
	streams, _ := redisx.Open(ctx, redisx.Config{Addrs: []string{"redis-streams:6379"}})
	outbox, _ := redisx.Open(ctx, redisx.Config{Addrs: []string{"redis-outbox:6379"}})

	p, _ := redisbus.NewProducer(streams, redisbus.ProducerConfig{
		Outbox:       &redisbus.OutboxConfig{Stream: "outbox:orch"},
		OutboxClient: outbox, // omit for mirror mode
	})
	id, err := p.Append(ctx, "run_1", sse.Message{Type: "run.started", Data: []byte(`{}`)})
	switch {
	case errors.Is(err, redisbus.ErrOutboxDiverged):
		// Viewers get the event (id is valid); the projection misses it.
		log.Default().WarnContext(ctx, "outbox diverged", log.String("entry", id), log.Err(err))
	case err != nil:
		// Nothing was written; retry or fail the run.
	}
}

// Any consumer (gRPC, workers) can range over a stream through the hub.
func ExampleHub_Subscribe() {
	mr, _ := miniredis.Run()
	defer mr.Close()
	ctx := context.Background()
	rdb, _ := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	defer func() { _ = rdb.Close() }()

	p, _ := redisbus.NewProducer(rdb, redisbus.ProducerConfig{})
	hub, _ := redisbus.NewHub(rdb, redisbus.HubConfig{})
	defer hub.Close()

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

// A gateway wires Hub → sse.Server → HTTP, with the envelope encoder and the
// context validator. Mount the handler in a kitchen server service.
func Example_gateway() {
	ctx := context.Background()
	logger := log.Default()
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{"localhost:6379"}})
	if err != nil {
		logger.ErrorContext(ctx, "redis", log.Err(err))
		return
	}
	hub, _ := redisbus.NewHub(rdb, redisbus.HubConfig{Logger: logger})
	go func() { _ = hub.Run(ctx) }()
	srv, _ := sse.NewServer(hub, sse.ServerConfig{MaxConnections: 10_000, MaxConnectionsPerStream: 20, Logger: logger})
	defer func() { _ = srv.Shutdown(ctx) }()

	validator := envelope.ContextValidator(envelope.ValidatorConfig{
		Logger: logger,
		Resolve: func(ctx context.Context) envelope.Rule {
			return envelope.Rule{UserIDs: []string{xcontext.UserID(ctx)}}
		},
	})
	visible := sse.VisibleTypes("run.", "step.", "approval.")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/runs/{run_id}/stream", func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("run_id")
		ticket, err := srv.Admit(r.Context(), runID)
		if err != nil {
			errs.WriteHTTP(w, r, err)
			return
		}
		err = srv.Serve(w, r, runID, sse.ServeOptions{
			Ticket:     ticket,
			Encoder:    envelope.NewEncoder,
			Visible:    visible,
			Transforms: []sse.TransformFactory{validator},
		})
		if errors.Is(err, sse.ErrInvalidCursor) {
			errs.WriteHTTP(w, r, err)
		}
	})
}
