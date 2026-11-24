package httpx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A breaker opens when the upstream is down (connection refused), not only
// on 5xx responses.
func TestCircuitBreakerCountsTransportErrors(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens on url any more

	c := newClient(t, WithBaseURL(url), WithCircuitBreaker(BreakerConfig{
		FailureThreshold: 2, ResetTimeout: time.Minute,
	}))
	for range 2 {
		if err := c.Get("/").Do(t.Context(), nil); err == nil || errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("want a transport error, got %v", err)
		}
	}
	if err := c.Get("/").Do(t.Context(), nil); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("after 2 transport errors: err = %v, want ErrCircuitOpen", err)
	}
}

// A call cancelled by its caller says nothing about the upstream and does
// not count as a failure.
func TestCircuitBreakerIgnoresCallerCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("slow") != "" {
			<-r.Context().Done()
		}
	}))
	defer srv.Close()

	c := newClient(t, WithBaseURL(srv.URL), WithCircuitBreaker(BreakerConfig{
		FailureThreshold: 1, ResetTimeout: time.Minute,
	}))
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(20*time.Millisecond, cancel)
	if err := c.Get("/?slow=1").Do(ctx, nil); err == nil {
		t.Fatal("want an error from the cancelled call")
	}
	if err := c.Get("/").Do(t.Context(), nil); err != nil {
		t.Fatalf("after a caller cancel: err = %v, want nil", err)
	}
}
