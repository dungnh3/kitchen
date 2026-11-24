package httpx_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/httpx"
	"github.com/dungnh3/kitchen/log"
)

func Example() {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/items/missing" {
			http.Error(w, `{"error":"no such item"}`, http.StatusNotFound)
			return
		}
		_, _ = fmt.Fprint(w, `{"id":"42"}`)
	}))
	defer api.Close()

	// Build once per upstream, share across goroutines.
	c, err := httpx.New(httpx.WithBaseURL(api.URL+"/v1"), httpx.WithBearer("token"),
		httpx.WithRetry(httpx.RetryPolicy{}))
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	var out struct {
		ID string `json:"id"`
	}
	err = c.Post("/orders").IdempotencyKey("order-1").JSON(map[string]int{"qty": 1}).Do(ctx, &out)
	fmt.Println(out.ID, err)

	err = c.Get("/items/{id}").PathParam("id", "missing").Do(ctx, nil)
	fmt.Println(errors.Is(err, errs.ErrNotFound), httpx.StatusCode(err))
	// Output:
	// 42 <nil>
	// true 404
}

func ExampleRequest_Send() {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: hello\n\n")
	}))
	defer api.Close()

	// Streams outlive one attempt: rely on the ctx deadline, not WithTimeout.
	c, err := httpx.New(httpx.WithBaseURL(api.URL), httpx.WithTimeout(0))
	if err != nil {
		panic(err)
	}
	res, err := c.Get("/events").Header("Accept", "text/event-stream").Send(context.Background())
	if err != nil {
		panic(err) // a non-2xx status is an error here too; res is nil then
	}
	defer func() { _ = res.Body().Close() }()

	line, _ := bufio.NewReader(res.Body()).ReadString('\n')
	fmt.Print(res.StatusCode(), " ", line)
	// Output: 200 data: hello
}

func ExampleWithRetry() {
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprint(w, `{"ok":true}`)
	}))
	defer api.Close()

	c, err := httpx.New(httpx.WithBaseURL(api.URL), httpx.WithRetry(httpx.RetryPolicy{
		MaxAttempts:   3,                     // first attempt included
		MinBackoff:    10 * time.Millisecond, // timex.Backoff with jitter
		MaxBackoff:    time.Second,
		MaxRetryAfter: 10 * time.Second, // a longer Retry-After returns at once
	}))
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	// GET is idempotent: retried.
	err = c.Get("/status").Do(ctx, nil)
	fmt.Println(err, calls.Load())

	// A POST is retried only with an idempotency key the server deduplicates on.
	calls.Store(0)
	err = c.Post("/payments").IdempotencyKey("pay-7f3a").JSON(map[string]int{"amount": 100}).Do(ctx, nil)
	fmt.Println(err, calls.Load())
	// Output:
	// <nil> 3
	// <nil> 3
}

func ExampleWithCircuitBreaker() {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer api.Close()

	c, err := httpx.New(httpx.WithBaseURL(api.URL), httpx.WithCircuitBreaker(httpx.BreakerConfig{
		FailureThreshold: 2,                // 5xx responses count
		ResetTimeout:     30 * time.Second, // window, and how long it stays open
		OnStateChange: func(from, to string) {
			fmt.Println("breaker:", from, "->", to)
		},
	}))
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	for range 2 {
		_ = c.Get("/").Do(ctx, nil)
	}
	err = c.Get("/").Do(ctx, nil) // fails fast, nothing is sent
	fmt.Println(errors.Is(err, httpx.ErrCircuitOpen), errors.Is(err, errs.ErrDependencyUnavailable))
	// Output:
	// breaker: closed -> open
	// true true
}

func ExampleWithSSRFGuard() {
	internal := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer internal.Close()

	// No base URL: every request names its own URL, so guard the dialer.
	c, err := httpx.New(httpx.WithSSRFGuard(httpx.SSRFConfig{}))
	if err != nil {
		panic(err)
	}
	userURL := internal.URL + "/admin" // 127.0.0.1: refused after DNS resolution
	err = c.Get(userURL).Do(context.Background(), nil)
	fmt.Println(errors.Is(err, httpx.ErrDestinationNotAllowed), errs.Code(err))
	// Output: true InvalidArgument
}

func ExampleStatusError() {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/busy" {
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = fmt.Fprint(w, `{"code":"BAD_SKU"}`)
	}))
	defer api.Close()
	c, err := httpx.New(httpx.WithBaseURL(api.URL))
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	var apiErr struct {
		Code string `json:"code"`
	}
	err = c.Post("/orders").JSON(map[string]string{"sku": "x"}).ErrorInto(&apiErr).Do(ctx, nil)
	if se, ok := errors.AsType[*httpx.StatusError](err); ok {
		fmt.Println(se.StatusCode, string(se.Body))
	}
	fmt.Println(apiErr.Code, errs.Code(err)) // unmapped statuses are Internal

	err = c.Get("/busy").Do(ctx, nil)
	fmt.Println(errors.Is(err, errs.ErrDependencyUnavailable), httpx.StatusCode(err))
	// Output:
	// 422 {"code":"BAD_SKU"}
	// BAD_SKU Internal
	// true 429
}

func ExampleWithErrorMapper() {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer api.Close()

	c, err := httpx.New(httpx.WithBaseURL(api.URL), httpx.WithErrorMapper(func(se *httpx.StatusError) error {
		if se.StatusCode == http.StatusConflict {
			return errs.Wrap(se, errs.ErrResourceConflict) // keep se in the chain
		}
		return httpx.DefaultErrorMapper(se)
	}))
	if err != nil {
		panic(err)
	}
	err = c.Put("/items/1").JSON(map[string]int{"v": 2}).Do(context.Background(), nil)
	fmt.Println(errors.Is(err, errs.ErrResourceConflict), httpx.StatusCode(err))
	// Output: true 409
}

func ExampleWithRequestHook() {
	seen := make(chan string, 1)
	api := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("X-Signature") + " " + r.Header.Get("X-Via")
	}))
	defer api.Close()

	logger := log.Default() // main calls log.Setup once; pass the logger down
	c, err := httpx.New(httpx.WithBaseURL(api.URL),
		httpx.WithLogger(logger), // resty's warnings go through kitchen/log
		httpx.WithRequestHook(func(r *httpx.Request) error {
			r.Header("X-Signature", "sig-"+r.Method()) // runs after OriginGuard
			return nil
		}),
		httpx.WithResponseHook(func(res *httpx.Response) error {
			logger.Debug("upstream call", log.Int("status", res.StatusCode()), log.String("url", res.URL()))
			return nil
		}),
		httpx.WithTransportMiddleware(func(next http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(r *http.Request) (*http.Response, error) {
				r = r.Clone(r.Context())
				r.Header.Set("X-Via", "mw") // traced and error-mapped like the base transport
				return next.RoundTrip(r)
			})
		}),
	)
	if err != nil {
		panic(err)
	}
	err = c.Get("/").Do(context.Background(), nil)
	fmt.Println(<-seen, err)
	// Output: sig-GET mw <nil>
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// A client for one upstream, wired the kitchen way. Compile-only: it needs
// the network.
func Example_wiring() {
	logger := log.Default() // after log.Setup in main
	payments, err := httpx.New(
		httpx.WithBaseURL("https://payments.example.com/v2"),
		httpx.WithBearer(os.Getenv("PAYMENTS_API_KEY")),
		httpx.WithLogger(logger),
		httpx.WithTimeout(5*time.Second), // per attempt
		httpx.WithRetry(httpx.RetryPolicy{}),
		httpx.WithCircuitBreaker(httpx.BreakerConfig{}),
		// Traces and metrics use the global providers that otelx/sdk.Setup
		// installs; WithTracerProvider / WithMeterProvider override them.
	)
	if err != nil {
		logger.Error("payments client", log.Err(err))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // whole call, retries included
	defer cancel()

	var charge struct {
		ID string `json:"id"`
	}
	err = payments.Get("/charges/{id}").PathParam("id", "ch_1").Do(ctx, &charge)
	switch {
	case errors.Is(err, errs.ErrNotFound):
		// 404 upstream: the handler can return err as is (NotFound).
	case err != nil:
		// Returning err keeps the errs code: Unavailable, DeadlineExceeded or
		// Internal. The upstream body stays in the cause, never in the reply.
	}
}
