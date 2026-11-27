package webhook_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/alicebob/miniredis/v2"
	"google.golang.org/grpc"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/redisx"
	"github.com/dungnh3/kitchen/webhook"
)

// The sender signs the payload; the three headers go with the POST.
func ExampleSigner_Headers() {
	signer, err := webhook.NewSigner("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")
	if err != nil {
		panic(err)
	}
	h, err := signer.Headers("msg_p5jXN8AQM9LWM0D4loKWxJek", time.Unix(1614265330, 0), []byte(`{"test": 2432232314}`))
	if err != nil {
		panic(err)
	}
	fmt.Println(h.Get(webhook.HeaderID))
	fmt.Println(h.Get(webhook.HeaderTimestamp))
	fmt.Println(h.Get(webhook.HeaderSignature))
	// Output:
	// msg_p5jXN8AQM9LWM0D4loKWxJek
	// 1614265330
	// v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=
}

// A new endpoint gets a new random secret; show it to the receiver once.
func ExampleGenerateSecret() {
	secret, err := webhook.GenerateSecret()
	if err != nil {
		panic(err)
	}
	fmt.Println(strings.HasPrefix(secret, webhook.PrefixSymmetric), len(secret))
	// Output: true 50
}

// The receiver wraps its handler; only verified requests reach it.
func ExampleNewMiddleware() {
	verifier, err := webhook.NewVerifier(webhook.VerifierConfig{
		Secrets: []string{"whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"}, // from config, never from code
	})
	if err != nil {
		panic(err)
	}
	verified, err := webhook.NewMiddleware(webhook.MiddlewareConfig{
		Verifier: verifier,
		Errors:   errs.NewHandler(errs.Config{}), // the server's handler
		Logger:   log.Discard(),                  // the service's logger
	})
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle("POST /webhooks/billing", verified(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg, _ := webhook.FromContext(r.Context())
		body, _ := io.ReadAll(r.Body) // the raw body, as signed
		fmt.Printf("handling %s: %s\n", msg.ID, body)
		w.WriteHeader(http.StatusNoContent)
	})))

	send := func(sign bool) int {
		body := `{"type":"invoice.paid"}`
		r := httptest.NewRequest(http.MethodPost, "/webhooks/billing", strings.NewReader(body))
		if sign {
			signer, _ := webhook.NewSigner("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")
			h, _ := signer.Headers("msg_1", time.Now(), []byte(body))
			for k, v := range h {
				r.Header[k] = v
			}
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, r)
		return rec.Code
	}
	fmt.Println(send(true))
	fmt.Println(send(false))
	// Output:
	// handling msg_1: {"type":"invoice.paid"}
	// 204
	// 401
}

// With Redis, a message the sender delivers twice is handled once.
func ExampleNewRedisDedup() {
	ctx := context.Background()
	mr, _ := miniredis.Run() // in a service: your Redis address
	defer mr.Close()
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	if err != nil {
		panic(err)
	}
	defer func() { _ = rdb.Close() }()

	dedup, err := webhook.NewRedisDedup(rdb, webhook.RedisDedupConfig{Prefix: "webhook:billing:"})
	if err != nil {
		panic(err)
	}
	verifier, _ := webhook.NewVerifier(webhook.VerifierConfig{Secrets: []string{"whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"}})
	verified, err := webhook.NewMiddleware(webhook.MiddlewareConfig{Verifier: verifier, Dedup: dedup, Logger: log.Discard()})
	if err != nil {
		panic(err)
	}
	h := verified(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { fmt.Println("handled") }))

	signer, _ := webhook.NewSigner("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")
	for range 2 { // the sender retries: same id, new timestamp
		hdr, _ := signer.Headers("msg_1", time.Now(), []byte("{}"))
		r := httptest.NewRequest(http.MethodPost, "/webhooks/billing", strings.NewReader("{}"))
		for k, v := range hdr {
			r.Header[k] = v
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		fmt.Println(rec.Code)
	}
	// Output:
	// handled
	// 200
	// 200
}

// Billing is a service that receives webhooks on a plain HTTP route of
// kitchen's server (server.HTTPRegistrar).
type Billing struct {
	verified func(http.Handler) http.Handler
}

// RegisterGRPC implements server.Service.
func (*Billing) RegisterGRPC(grpc.ServiceRegistrar) {}

// RegisterHTTP mounts the webhook route. With server.WithAuth, list
// "POST /webhooks/billing" in the authenticator's Public methods: the
// signature authenticates the sender.
func (b *Billing) RegisterHTTP(mux *http.ServeMux) {
	mux.Handle("POST /webhooks/billing", b.verified(http.HandlerFunc(b.handle)))
}

func (*Billing) handle(w http.ResponseWriter, r *http.Request) {
	msg, _ := webhook.FromContext(r.Context())
	_ = msg // decode msg.Body and act on it; return 2xx only when done
	w.WriteHeader(http.StatusNoContent)
}

func Example_server() {
	verifier, err := webhook.NewVerifier(webhook.VerifierConfig{Secrets: []string{"whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"}})
	if err != nil {
		panic(err)
	}
	verified, err := webhook.NewMiddleware(webhook.MiddlewareConfig{Verifier: verifier})
	if err != nil {
		panic(err)
	}
	svc := &Billing{verified: verified}
	// server.New(server.WithServices(svc), ...)
	mux := http.NewServeMux()
	svc.RegisterHTTP(mux)
	fmt.Println("mounted")
	// Output: mounted
}
