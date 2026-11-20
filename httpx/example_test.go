package httpx_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/dungnh3/kitchen/httpx"
)

func Example() {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"id":"42"}`)
	}))
	defer api.Close()

	// Build once, share across goroutines.
	c, err := httpx.New(api.URL+"/v1", httpx.WithTimeout(10*time.Second),
		httpx.WithRetry(httpx.RetryPolicy{MaxAttempts: 3}))
	if err != nil {
		panic(err)
	}
	var out struct {
		ID string `json:"id"`
	}
	err = c.Post("/orders").Bearer("token").IdempotencyKey("order-1").
		JSON(map[string]int{"qty": 1}).Do(context.Background(), &out)
	if code := httpx.StatusCode(err); code != 0 {
		fmt.Println("api error", code)
		return
	}
	fmt.Println(out.ID, err)
	// Output: 42 <nil>
}
