package webhook_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/webhook"
)

func newRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

// signedRequest returns a POST signed now with specSecret.
func signedRequest(t *testing.T, id, body string) *http.Request {
	t.Helper()
	h, err := mustSigner(t, specSecret).Headers(id, time.Now(), []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/webhooks/test", strings.NewReader(body))
	for k, v := range h {
		r.Header[k] = v
	}
	return r
}

func newMiddleware(t *testing.T, cfg webhook.MiddlewareConfig) func(http.Handler) http.Handler {
	t.Helper()
	if cfg.Verifier == nil {
		v, err := webhook.NewVerifier(webhook.VerifierConfig{Secrets: []string{specSecret}})
		if err != nil {
			t.Fatal(err)
		}
		cfg.Verifier = v
	}
	cfg.Logger = log.Discard()
	mw, err := webhook.NewMiddleware(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return mw
}

func reason(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Details []struct {
				Reason string `json:"reason"`
			} `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || len(body.Error.Details) == 0 {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	return body.Error.Details[0].Reason
}

func TestMiddleware(t *testing.T) {
	const payload = `{"type":"invoice.paid"}`
	tests := []struct {
		name       string
		req        func(t *testing.T) *http.Request
		maxBody    int64
		wantStatus int
		wantReason string
		wantCalled bool
	}{
		{"verified", func(t *testing.T) *http.Request { return signedRequest(t, "msg_1", payload) }, 0, http.StatusNoContent, "", true},
		{"bad signature", func(t *testing.T) *http.Request {
			r := signedRequest(t, "msg_1", payload)
			r.Header.Set(webhook.HeaderSignature, "v1,AAAA")
			return r
		}, 0, http.StatusUnauthorized, "WEBHOOK_SIGNATURE_INVALID", false},
		{"unsigned", func(*testing.T) *http.Request {
			return httptest.NewRequest(http.MethodPost, "/webhooks/test", strings.NewReader(payload))
		}, 0, http.StatusUnauthorized, "WEBHOOK_SIGNATURE_INVALID", false},
		{
			"declared body too large", func(t *testing.T) *http.Request { return signedRequest(t, "msg_1", payload) },
			8, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", false,
		},
		{"streamed body too large", func(t *testing.T) *http.Request {
			r := signedRequest(t, "msg_1", payload)
			r.ContentLength = -1
			return r
		}, 8, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			h := newMiddleware(t, webhook.MiddlewareConfig{MaxBodyBytes: tc.maxBody})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				body, _ := io.ReadAll(r.Body)
				m, ok := webhook.FromContext(r.Context())
				if string(body) != payload || !ok || m.ID != "msg_1" || string(m.Body) != payload {
					t.Errorf("handler got body %q, message %+v %v", body, m, ok)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, tc.req(t))
			if rec.Code != tc.wantStatus || called != tc.wantCalled {
				t.Fatalf("status %d called %v, want %d %v (%s)", rec.Code, called, tc.wantStatus, tc.wantCalled, rec.Body)
			}
			if tc.wantReason != "" && reason(t, rec) != tc.wantReason {
				t.Fatalf("reason = %s", reason(t, rec))
			}
		})
	}
}

// failingVerifier is a custom Verifier that returns a plain error.
type failingVerifier struct{}

func (failingVerifier) Verify(context.Context, http.Header, []byte) (webhook.Message, error) {
	return webhook.Message{}, io.ErrUnexpectedEOF
}

func TestMiddlewareCustomVerifierFailsClosed(t *testing.T) {
	h := newMiddleware(t, webhook.MiddlewareConfig{Verifier: failingVerifier{}})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("handler called")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, signedRequest(t, "msg_1", "{}"))
	if rec.Code != http.StatusUnauthorized || reason(t, rec) != "WEBHOOK_SIGNATURE_INVALID" {
		t.Fatalf("status %d %s", rec.Code, rec.Body)
	}
}

func TestNewMiddlewareValidation(t *testing.T) {
	if _, err := webhook.NewMiddleware(webhook.MiddlewareConfig{}); err == nil {
		t.Error("no verifier accepted")
	}
	if _, err := webhook.NewMiddleware(webhook.MiddlewareConfig{Verifier: failingVerifier{}, MaxBodyBytes: -1}); err == nil {
		t.Error("negative limit accepted")
	}
	if _, err := webhook.NewRedisDedup(nil, webhook.RedisDedupConfig{}); err == nil {
		t.Error("nil redis accepted")
	}
}

func TestMiddlewareDedup(t *testing.T) {
	type step struct {
		id         string
		handler    int // status the handler writes; 0 writes nothing, -1 panics
		wantStatus int
		wantCalled bool
	}
	tests := []struct {
		name  string
		steps []step
	}{
		{"second delivery is acknowledged without the handler", []step{
			{"msg_1", http.StatusOK, http.StatusOK, true},
			{"msg_1", http.StatusOK, http.StatusOK, false},
			{"msg_2", http.StatusOK, http.StatusOK, true},
		}},
		{"implicit 200 counts as handled", []step{
			{"msg_1", 0, http.StatusOK, true},
			{"msg_1", 0, http.StatusOK, false},
		}},
		{"failure releases the id", []step{
			{"msg_1", http.StatusInternalServerError, http.StatusInternalServerError, true},
			{"msg_1", http.StatusAccepted, http.StatusAccepted, true},
			{"msg_1", http.StatusAccepted, http.StatusOK, false},
		}},
		{"client error releases the id", []step{
			{"msg_1", http.StatusBadRequest, http.StatusBadRequest, true},
			{"msg_1", http.StatusOK, http.StatusOK, true},
		}},
		{"panic releases the id", []step{
			{"msg_1", -1, -1, true},
			{"msg_1", http.StatusOK, http.StatusOK, true},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, rdb := newRedis(t)
			d, err := webhook.NewRedisDedup(rdb, webhook.RedisDedupConfig{})
			if err != nil {
				t.Fatal(err)
			}
			mw := newMiddleware(t, webhook.MiddlewareConfig{Dedup: d})
			for i, s := range tc.steps {
				called := false
				h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					called = true
					switch s.handler {
					case -1:
						panic("boom")
					case 0:
					default:
						w.WriteHeader(s.handler)
					}
				}))
				rec := httptest.NewRecorder()
				func() {
					defer func() {
						if p := recover(); p != nil && s.handler != -1 {
							t.Fatalf("step %d: panic %v", i, p)
						}
					}()
					h.ServeHTTP(rec, signedRequest(t, s.id, "{}"))
				}()
				if called != s.wantCalled || s.wantStatus > 0 && rec.Code != s.wantStatus {
					t.Fatalf("step %d: status %d called %v, want %d %v", i, rec.Code, called, s.wantStatus, s.wantCalled)
				}
			}
		})
	}
}

func TestMiddlewareDedupInProgress(t *testing.T) {
	mr, rdb := newRedis(t)
	d, err := webhook.NewRedisDedup(rdb, webhook.RedisDedupConfig{Prefix: "wh:", LockTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	mw := newMiddleware(t, webhook.MiddlewareConfig{Dedup: d})
	var calls atomic.Int32
	var inner http.Handler
	inner = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			// A retry arrives while the first delivery is still running.
			rec := httptest.NewRecorder()
			mw(inner).ServeHTTP(rec, signedRequest(t, "msg_1", "{}"))
			if rec.Code != http.StatusConflict || reason(t, rec) != "REQUEST_IN_PROGRESS" {
				t.Errorf("concurrent delivery: %d %s", rec.Code, rec.Body)
			}
			if v, _ := mr.Get("wh:msg_1"); !strings.HasPrefix(v, "claim:") {
				t.Errorf("claim = %q", v)
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	mw(inner).ServeHTTP(rec, signedRequest(t, "msg_1", "{}"))
	if rec.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("status %d calls %d", rec.Code, calls.Load())
	}
	if v, _ := mr.Get("wh:msg_1"); v != "done" || mr.TTL("wh:msg_1") != 72*time.Hour {
		t.Fatalf("marker = %q ttl %v", v, mr.TTL("wh:msg_1"))
	}
}

func TestRedisDedupExpiredClaim(t *testing.T) {
	mr, rdb := newRedis(t)
	d, err := webhook.NewRedisDedup(rdb, webhook.RedisDedupConfig{LockTTL: time.Second, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	slow, err := d.Begin(ctx, "msg_1")
	if err != nil {
		t.Fatal(err)
	}
	mr.FastForward(2 * time.Second) // the slow handler's claim lapses
	fast, err := d.Begin(ctx, "msg_1")
	if err != nil {
		t.Fatal(err)
	}
	// The slow handler fails late: its release must not drop the new claim.
	if err := slow.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Begin(ctx, "msg_1"); !errors.Is(err, webhook.ErrInProgress) {
		t.Fatalf("Begin = %v, want ErrInProgress", err)
	}
	if err := slow.Done(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fast.Done(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Begin(ctx, "msg_1"); !errors.Is(err, webhook.ErrDuplicate) {
		t.Fatalf("Begin = %v, want ErrDuplicate", err)
	}
	mr.FastForward(time.Hour + time.Second)
	if _, err := d.Begin(ctx, "msg_1"); err != nil {
		t.Fatalf("after TTL: %v", err)
	}
}

func TestMiddlewareDedupUnavailable(t *testing.T) {
	mr, rdb := newRedis(t)
	d, err := webhook.NewRedisDedup(rdb, webhook.RedisDedupConfig{})
	if err != nil {
		t.Fatal(err)
	}
	mr.Close()
	h := newMiddleware(t, webhook.MiddlewareConfig{Dedup: d})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("handler called without dedup")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, signedRequest(t, "msg_1", "{}"))
	if rec.Code != http.StatusServiceUnavailable || reason(t, rec) != "DEPENDENCY_UNAVAILABLE" {
		t.Fatalf("status %d %s", rec.Code, rec.Body)
	}
}
