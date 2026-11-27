package delivery_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"

	"github.com/dungnh3/kitchen/httpx"
	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/jobs/jobstest"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/webhook"
	"github.com/dungnh3/kitchen/webhook/delivery"
)

func Example() {
	ctx := context.Background()

	// A customer's endpoint (in production: their URL, reached over https).
	verifier, _ := webhook.NewVerifier(webhook.VerifierConfig{Secrets: []string{"whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"}})
	received := make(chan string, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		var body [256]byte
		n, _ := r.Body.Read(body[:])
		if _, err := verifier.Verify(r.Context(), r.Header, body[:n]); err == nil {
			received <- string(body[:n])
		}
	}))
	defer endpoint.Close()

	// Endpoints come from the service's own tables.
	endpoints := delivery.EndpointsFunc(func(_ context.Context, id string) (delivery.Endpoint, error) {
		if id != "ep_1" {
			return delivery.Endpoint{}, delivery.ErrEndpointNotFound
		}
		return delivery.Endpoint{URL: endpoint.URL, Secrets: []string{"whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"}}, nil
	})

	u, _ := url.Parse(endpoint.URL)
	port, _ := strconv.Atoi(u.Port())
	worker, err := delivery.NewWorker(delivery.Config{
		Endpoints: endpoints,
		Logger:    log.Discard(),
		// Only for this example: the test endpoint is plain http on
		// loopback, which the defaults refuse.
		AllowHTTP: true,
		SSRF:      httpx.SSRFConfig{Ports: []int{port}, Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}},
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = worker.Close(ctx) }()

	reg := jobs.NewRegistry()
	if err := jobs.Register(reg, worker); err != nil {
		panic(err)
	}
	client, err := jobs.NewClient(jobstest.New(), // jobspgx.New(pool) in a service
		jobs.WithRegistry(reg), jobs.WithLogger(log.Discard()),
		jobs.WithQueues(jobs.Queue{Name: "default", Workers: 10}, jobs.Queue{Name: delivery.DefaultQueue, Workers: 10}))
	if err != nil {
		panic(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- client.Run(ctx) }()

	// In the business transaction: client.InsertTx(ctx, tx, args, nil).
	args, err := delivery.NewArgs("ep_1", "invoice.paid", map[string]string{"type": "invoice.paid", "id": "inv_1"})
	if err != nil {
		panic(err)
	}
	if _, err := client.Insert(ctx, args, nil); err != nil {
		panic(err)
	}
	fmt.Println(<-received)

	if err := client.Close(ctx); err != nil {
		panic(err)
	}
	if err := <-stopped; err != nil {
		panic(err)
	}
	// Output: {"id":"inv_1","type":"invoice.paid"}
}
