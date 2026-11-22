package errs_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/log"

	"github.com/dungnh3/kitchen/errs"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nopWriter struct{ h http.Header }

func (w *nopWriter) Header() http.Header         { return w.h }
func (w *nopWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *nopWriter) WriteHeader(int)             {}

type benchLoc struct{}

func (benchLoc) Localize(_ context.Context, info *errdetails.ErrorInfo) (string, string, bool) {
	return "vi", "Không tìm thấy " + info.GetReason(), true
}

func benchHandler() *errs.Handler {
	return errs.NewHandler(errs.Config{Logger: log.Discard(), Domain: "svc", Localizer: benchLoc{}})
}

func BenchmarkWriteHTTPError(b *testing.B) {
	h := benchHandler()
	r := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/v1/x", nil)
	err := errs.New(codes.NotFound, "ITEM_NOT_FOUND", errs.Meta("id", "42"))
	b.ReportAllocs()
	for b.Loop() {
		h.WriteHTTP(&nopWriter{h: http.Header{}}, r, err)
	}
}

func BenchmarkWriteHTTPRateLimited(b *testing.B) {
	h := benchHandler()
	r := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/v1/x", nil)
	err := errs.ErrRateLimited.With(errs.RetryAfter(1500 * time.Millisecond))
	b.ReportAllocs()
	for b.Loop() {
		h.WriteHTTP(&nopWriter{h: http.Header{}}, r, err)
	}
}

func BenchmarkGatewayInprocStatus(b *testing.B) {
	h := benchHandler()
	r := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/v1/x", nil)
	// What the gateway receives from the in-process call: an already
	// completed status.
	st := h.Status(context.Background(), errs.New(codes.NotFound, "ITEM_NOT_FOUND"))
	err := status.ErrorProto(st.Proto())
	b.ReportAllocs()
	for b.Loop() {
		h.GatewayErrorHandler(r.Context(), nil, nil, &nopWriter{h: http.Header{}}, r, err)
	}
}

func BenchmarkStatusGRPC(b *testing.B) {
	h := benchHandler()
	err := errs.New(codes.NotFound, "ITEM_NOT_FOUND", errs.Meta("id", "42"))
	b.ReportAllocs()
	for b.Loop() {
		_ = h.Status(context.Background(), err)
	}
}
