package delivery_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/httpx"
	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/jobs/jobstest"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/webhook"
	"github.com/dungnh3/kitchen/webhook/delivery"
)

const secret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw" //nolint:gosec // G101: the spec's public test vector

// receiver is an endpoint that verifies what it receives.
type receiver struct {
	srv    *httptest.Server
	mu     sync.Mutex
	got    []webhook.Message
	status func(n int) (int, http.Header)
	calls  chan struct{}
}

func newReceiver(t *testing.T, status func(n int) (int, http.Header)) *receiver {
	t.Helper()
	v, err := webhook.NewVerifier(webhook.VerifierConfig{Secrets: []string{secret}})
	if err != nil {
		t.Fatal(err)
	}
	rc := &receiver{status: status, calls: make(chan struct{}, 100)}
	rc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m, err := v.Verify(r.Context(), r.Header, body)
		if err != nil || r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("bad delivery: %v %s %s", err, r.Method, r.Header.Get("Content-Type"))
		}
		rc.mu.Lock()
		rc.got = append(rc.got, m)
		n := len(rc.got)
		rc.mu.Unlock()
		code, h := rc.status(n)
		for k, vs := range h {
			w.Header()[k] = vs
		}
		w.WriteHeader(code)
		rc.calls <- struct{}{}
	}))
	t.Cleanup(rc.srv.Close)
	return rc
}

func (rc *receiver) messages() []webhook.Message {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]webhook.Message(nil), rc.got...)
}

// loopback lets the SSRF guard reach the test server, and only it.
func loopback(t *testing.T, rawURL string) httpx.SSRFConfig {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(u.Port())
	return httpx.SSRFConfig{Ports: []int{port}, Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}}
}

func staticEndpoint(ep delivery.Endpoint) delivery.Endpoints {
	return delivery.EndpointsFunc(func(_ context.Context, id string) (delivery.Endpoint, error) {
		if id != "ep_1" {
			return delivery.Endpoint{}, delivery.ErrEndpointNotFound
		}
		return ep, nil
	})
}

func newWorker(t *testing.T, cfg delivery.Config) *delivery.Worker {
	t.Helper()
	cfg.Logger = log.Discard()
	w, err := delivery.NewWorker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

func job(t *testing.T, endpointID string, attempt, maxAttempts int) *jobs.Job[delivery.Args] {
	t.Helper()
	a, err := delivery.NewArgs(endpointID, "invoice.paid", map[string]string{"id": "inv_1"})
	if err != nil {
		t.Fatal(err)
	}
	return &jobs.Job[delivery.Args]{Row: jobs.Row{Attempt: attempt, MaxAttempts: maxAttempts}, Args: a}
}

func TestWork(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		header      http.Header
		endpoint    func(srvURL string) delivery.Endpoint
		endpointID  string
		attempt     int
		wantOutcome delivery.Outcome
		wantJobErr  string // "", "cancel", "retry", "fail"
		wantDelay   time.Duration
		wantSent    bool
	}{
		{name: "2xx is delivered", status: http.StatusNoContent, attempt: 1, wantOutcome: delivery.Delivered, wantSent: true},
		{
			name: "5xx is retried on the schedule", status: http.StatusBadGateway, attempt: 2,
			wantOutcome: delivery.Retrying, wantJobErr: "retry", wantDelay: 5 * time.Minute, wantSent: true,
		},
		{
			name: "4xx is retried", status: http.StatusBadRequest, attempt: 1,
			wantOutcome: delivery.Retrying, wantJobErr: "retry", wantDelay: 5 * time.Second, wantSent: true,
		},
		{
			name: "Retry-After raises the delay", status: http.StatusTooManyRequests, header: http.Header{"Retry-After": {"600"}}, attempt: 1,
			wantOutcome: delivery.Retrying, wantJobErr: "retry", wantDelay: 10 * time.Minute, wantSent: true,
		},
		{
			name: "Retry-After is capped by the schedule", status: http.StatusServiceUnavailable, header: http.Header{"Retry-After": {"999999"}}, attempt: 1,
			wantOutcome: delivery.Retrying, wantJobErr: "retry", wantDelay: 10 * time.Hour, wantSent: true,
		},
		{
			name: "redirect is not followed", status: http.StatusFound, header: http.Header{"Location": {"http://169.254.169.254/"}}, attempt: 1,
			wantOutcome: delivery.Retrying, wantJobErr: "retry", wantDelay: 5 * time.Second, wantSent: true,
		},
		{
			name: "last attempt fails", status: http.StatusInternalServerError, attempt: 7,
			wantOutcome: delivery.Failed, wantJobErr: "fail", wantSent: true,
		},
		{
			name: "410 stops delivery", status: http.StatusGone, attempt: 1,
			wantOutcome: delivery.Gone, wantJobErr: "cancel", wantSent: true,
		},
		{
			name: "unknown endpoint is cancelled", endpointID: "ep_gone", attempt: 1,
			wantOutcome: delivery.Cancelled, wantJobErr: "cancel",
		},
		{name: "disabled endpoint is cancelled", attempt: 1, endpoint: func(u string) delivery.Endpoint {
			return delivery.Endpoint{URL: u, Secrets: []string{secret}, Disabled: true}
		}, wantOutcome: delivery.Cancelled, wantJobErr: "cancel"},
		{name: "invalid secret is cancelled", attempt: 1, endpoint: func(u string) delivery.Endpoint {
			return delivery.Endpoint{URL: u, Secrets: []string{"plain"}}
		}, wantOutcome: delivery.Cancelled, wantJobErr: "cancel"},
		{name: "invalid URL is cancelled", attempt: 1, endpoint: func(string) delivery.Endpoint {
			return delivery.Endpoint{URL: "ftp://example.com/hook", Secrets: []string{secret}}
		}, wantOutcome: delivery.Cancelled, wantJobErr: "cancel"},
		{name: "private address is refused by the SSRF guard", attempt: 1, endpoint: func(string) delivery.Endpoint {
			return delivery.Endpoint{URL: "http://10.0.0.1:" + "80/hook", Secrets: []string{secret}}
		}, wantOutcome: delivery.Cancelled, wantJobErr: "cancel"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rc := newReceiver(t, func(int) (int, http.Header) { return tc.status, tc.header })
			ep := delivery.Endpoint{URL: rc.srv.URL + "/hook", Secrets: []string{secret}}
			if tc.endpoint != nil {
				ep = tc.endpoint(rc.srv.URL + "/hook")
			}
			var results []delivery.Result
			w := newWorker(t, delivery.Config{
				Endpoints: staticEndpoint(ep),
				SSRF:      loopback(t, rc.srv.URL),
				AllowHTTP: true,
				OnResult:  func(_ context.Context, r delivery.Result) { results = append(results, r) },
			})
			id := tc.endpointID
			if id == "" {
				id = "ep_1"
			}
			j := job(t, id, tc.attempt, 7)
			start := time.Now()
			err := w.Work(context.Background(), j)
			if len(results) != 1 || results[0].Outcome != tc.wantOutcome {
				t.Fatalf("results = %+v", results)
			}
			r := results[0]
			if tc.wantSent != (len(rc.messages()) == 1) {
				t.Fatalf("sent %d messages", len(rc.messages()))
			}
			if tc.wantSent && (rc.messages()[0].ID != j.Args.MessageID || r.StatusCode != tc.status) {
				t.Fatalf("message %+v result %+v", rc.messages()[0], r)
			}
			switch tc.wantJobErr {
			case "":
				if err != nil || r.Err != nil {
					t.Fatalf("Work = %v, result err %v", err, r.Err)
				}
			case "retry":
				lo, hi := start.Add(tc.wantDelay*9/10), time.Now().Add(tc.wantDelay*11/10)
				if err == nil || r.NextAttempt.Before(lo) || r.NextAttempt.After(hi) {
					t.Fatalf("Work = %v, next %v, want about %v from now", err, r.NextAttempt.Sub(start), tc.wantDelay)
				}
			case "cancel", "fail":
				if err == nil || r.Err == nil || !r.Outcome.Final() {
					t.Fatalf("Work = %v result %+v", err, r)
				}
			}
		})
	}
}

func TestDefaultSSRFGuardRefusesLoopback(t *testing.T) {
	rc := newReceiver(t, func(int) (int, http.Header) { return http.StatusOK, nil })
	var got delivery.Result
	w := newWorker(t, delivery.Config{
		Endpoints: staticEndpoint(delivery.Endpoint{URL: rc.srv.URL, Secrets: []string{secret}}),
		AllowHTTP: true,
		OnResult:  func(_ context.Context, r delivery.Result) { got = r },
	})
	if err := w.Work(context.Background(), job(t, "ep_1", 1, 7)); err == nil {
		t.Fatal("delivered to loopback")
	}
	if got.Outcome != delivery.Cancelled || !errors.Is(got.Err, httpx.ErrDestinationNotAllowed) || len(rc.messages()) != 0 {
		t.Fatalf("result %+v, sent %d", got, len(rc.messages()))
	}
}

func TestHTTPSRequiredByDefault(t *testing.T) {
	var got delivery.Result
	w := newWorker(t, delivery.Config{
		Endpoints: staticEndpoint(delivery.Endpoint{URL: "http://example.com/hook", Secrets: []string{secret}}),
		OnResult:  func(_ context.Context, r delivery.Result) { got = r },
	})
	if err := w.Work(context.Background(), job(t, "ep_1", 1, 7)); err == nil || got.Outcome != delivery.Cancelled {
		t.Fatalf("Work = %v, result %+v", err, got)
	}
}

func TestLookupFailureIsRetried(t *testing.T) {
	var got delivery.Result
	w := newWorker(t, delivery.Config{
		Endpoints: delivery.EndpointsFunc(func(context.Context, string) (delivery.Endpoint, error) {
			return delivery.Endpoint{}, errors.New("db down")
		}),
		OnResult: func(_ context.Context, r delivery.Result) { got = r },
	})
	if err := w.Work(context.Background(), job(t, "ep_1", 1, 7)); err == nil || got.Outcome != delivery.Retrying {
		t.Fatalf("Work = %v, result %+v", err, got)
	}
}

func TestNewWorkerValidation(t *testing.T) {
	ep := staticEndpoint(delivery.Endpoint{})
	tests := []struct {
		name string
		cfg  delivery.Config
	}{
		{"no endpoints", delivery.Config{}},
		{"negative delay", delivery.Config{Endpoints: ep, Schedule: []time.Duration{-1}}},
		{"negative timeout", delivery.Config{Endpoints: ep, Timeout: -1}},
		{"custom transport would bypass the SSRF guard", delivery.Config{Endpoints: ep, HTTPOptions: []httpx.Option{httpx.WithTransport(http.DefaultTransport)}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := delivery.NewWorker(tc.cfg); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestArgs(t *testing.T) {
	a, err := delivery.NewArgs("ep_1", "invoice.paid", map[string]int{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	b := a.To("ep_2")
	if b.MessageID != a.MessageID || b.EndpointID != "ep_2" || string(a.Payload) != `{"n":1}` {
		t.Fatalf("args %+v %+v", a, b)
	}
	o := a.InsertOpts()
	if o.Queue != "webhooks" || o.MaxAttempts != 7 || o.UniqueKey != "webhook:ep_1:"+a.MessageID || !o.KeepUnique {
		t.Fatalf("opts %+v", o)
	}
	if _, err := delivery.NewArgs("", "x", nil); err == nil {
		t.Error("empty endpoint accepted")
	}
	if _, err := delivery.NewArgs("ep", "x", func() {}); err == nil {
		t.Error("unencodable payload accepted")
	}
}

// TestRetriesThroughJobs runs the worker in a jobs client: the first two
// attempts fail, the third is delivered, all with the same message id.
func TestRetriesThroughJobs(t *testing.T) {
	rc := newReceiver(t, func(n int) (int, http.Header) {
		if n < 3 {
			return http.StatusServiceUnavailable, nil
		}
		return http.StatusOK, nil
	})
	w := newWorker(t, delivery.Config{
		Endpoints: staticEndpoint(delivery.Endpoint{URL: rc.srv.URL, Secrets: []string{secret}}),
		Schedule:  []time.Duration{10 * time.Millisecond},
		SSRF:      loopback(t, rc.srv.URL),
		AllowHTTP: true,
	})
	reg := jobs.NewRegistry()
	if err := jobs.Register(reg, w); err != nil {
		t.Fatal(err)
	}
	drv := jobstest.New()
	client, err := jobs.NewClient(drv, jobs.WithRegistry(reg), jobs.WithLogger(log.Discard()),
		jobs.WithQueues(jobs.Queue{Name: delivery.DefaultQueue, Workers: 2}), jobs.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	stopped := make(chan error, 1)
	go func() { stopped <- client.Run(ctx) }()

	a, err := delivery.NewArgs("ep_1", "invoice.paid", map[string]string{"id": "inv_1"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Insert(ctx, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	if dup, err := client.Insert(ctx, a, nil); err != nil || !dup.Duplicate {
		t.Fatalf("second insert = %+v, %v", dup, err)
	}
	for range 3 {
		select {
		case <-rc.calls:
		case <-time.After(10 * time.Second):
			t.Fatal("delivery did not arrive")
		}
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	row, err := client.Get(ctx, res.ID)
	if err != nil {
		t.Fatal(err)
	}
	msgs := rc.messages()
	if row.State != jobs.StateCompleted || row.Attempt != 3 || len(msgs) != 3 {
		t.Fatalf("row %+v, %d messages", row, len(msgs))
	}
	for _, m := range msgs {
		if m.ID != a.MessageID {
			t.Fatalf("message id %s, want %s", m.ID, a.MessageID)
		}
	}
}
