package redisbus_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/redisbus"
)

// A malformed stream ID without Last-Event-ID is refused before the response
// is committed, as a 400: a stream that ends without a terminal event after
// the headers makes EventSource reconnect forever.
func TestServeInvalidStreamIDUncommitted(t *testing.T) {
	s := newStack(t, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	err := s.srv.Serve(w, r, "a{b}", sse.ServeOptions{})
	if !errors.Is(err, redisbus.ErrInvalidStreamID) {
		t.Fatalf("err = %v, want ErrInvalidStreamID", err)
	}
	if w.Header().Get("Content-Type") != "" || w.Body.Len() != 0 {
		t.Fatal("response was committed")
	}
	if got := errs.HTTPStatus(err); got != http.StatusBadRequest {
		t.Fatalf("HTTP status = %d, want 400", got)
	}
}
