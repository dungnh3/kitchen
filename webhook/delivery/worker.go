package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/dungnh3/kitchen/httpx"
	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/timex"
	"github.com/dungnh3/kitchen/webhook"
)

// Outcome is the result of one attempt.
type Outcome string

// The outcomes.
const (
	// Delivered: the endpoint answered 2xx.
	Delivered Outcome = "delivered"
	// Retrying: the attempt failed and the job will run again.
	Retrying Outcome = "retrying"
	// Failed: the attempt failed and it was the last one.
	Failed Outcome = "failed"
	// Gone: the endpoint answered 410 Gone; delivery to it stopped.
	Gone Outcome = "gone"
	// Cancelled: the endpoint is missing, disabled or invalid, or its URL
	// is not allowed by the SSRF guard; delivery stopped.
	Cancelled Outcome = "cancelled"
)

// Final reports whether no attempt follows.
func (o Outcome) Final() bool { return o != Retrying }

// Result describes one attempt, for [Config].OnResult.
type Result struct {
	Args    Args
	Attempt int
	Outcome Outcome
	// StatusCode is the endpoint's HTTP status, or 0 when none was
	// received.
	StatusCode int
	// Err is the failure, nil when delivered.
	Err error
	// NextAttempt is the time of the retry when Outcome is Retrying.
	NextAttempt time.Time
	Duration    time.Duration
}

// Config configures [NewWorker].
type Config struct {
	// Endpoints looks endpoints up. Required.
	Endpoints Endpoints
	// Schedule is the delay before each retry (default [DefaultSchedule]).
	// Attempts beyond it wait for its last entry. The number of attempts is
	// the job's MaxAttempts, set at insert ([Args.InsertOpts]).
	Schedule []time.Duration
	// Timeout bounds one HTTP attempt (default 15s).
	Timeout time.Duration
	// SSRF configures the SSRF guard, which is always on: by default only
	// public addresses on ports 80 and 443 are reachable.
	SSRF httpx.SSRFConfig
	// AllowHTTP accepts http:// endpoint URLs. Off by default: payloads and
	// signatures travel only over TLS.
	AllowHTTP bool
	// HTTPOptions are added to the httpx client after the defaults (for
	// example httpx.WithTransportSettings). They cannot turn the SSRF guard
	// off: httpx refuses WithTransport together with it.
	HTTPOptions []httpx.Option
	// OnResult, when set, is called after every attempt: record delivery
	// logs, or disable an endpoint on Gone or after repeated Failed
	// results. It runs inside the job and must be quick.
	OnResult func(ctx context.Context, r Result)
	// Logger (nil: log.Default()).
	Logger *log.Logger
	// MeterProvider (default: the OTel global) and TracerProvider (passed
	// to httpx; default: the OTel global).
	MeterProvider  metric.MeterProvider
	TracerProvider trace.TracerProvider
}

// Worker delivers webhooks. It implements jobs.Worker[Args]; register it
// with jobs.Register.
type Worker struct {
	cfg      Config
	http     *httpx.Client
	log      *log.Logger
	attempts metric.Int64Counter
}

const defaultTimeout = 15 * time.Second

// maxErrorBody is how much of a failed response is kept in the error.
const maxErrorBody = 1024

// NewWorker returns a delivery worker. Call Close when the jobs client has
// stopped.
func NewWorker(cfg Config) (*Worker, error) {
	if cfg.Endpoints == nil {
		return nil, errors.New("delivery: Config.Endpoints is required")
	}
	if len(cfg.Schedule) == 0 {
		cfg.Schedule = DefaultSchedule
	}
	for _, d := range cfg.Schedule {
		if d < 0 {
			return nil, errors.New("delivery: negative delay in Schedule")
		}
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("delivery: negative Timeout")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}
	l := log.OrDefault(cfg.Logger)
	opts := []httpx.Option{
		httpx.WithLogger(l),
		httpx.WithTimeout(cfg.Timeout),
		httpx.WithSSRFGuard(cfg.SSRF),
		httpx.WithRedirectPolicy(httpx.NoRedirects),
		httpx.WithMaxErrorBody(maxErrorBody),
		httpx.WithMaxResponseBody(maxErrorBody),
		httpx.WithHeader("User-Agent", "kitchen-webhook"),
	}
	if cfg.TracerProvider != nil {
		opts = append(opts, httpx.WithTracerProvider(cfg.TracerProvider))
	}
	if cfg.MeterProvider != nil {
		opts = append(opts, httpx.WithMeterProvider(cfg.MeterProvider))
	}
	client, err := httpx.New(append(opts, cfg.HTTPOptions...)...)
	if err != nil {
		return nil, fmt.Errorf("delivery: %w", err)
	}
	mp := cfg.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	attempts, err := mp.Meter("github.com/dungnh3/kitchen/webhook/delivery").Int64Counter(
		"kitchen.webhook.delivery.attempts",
		metric.WithDescription("Webhook delivery attempts, by outcome."),
		metric.WithUnit("{attempt}"))
	if err != nil {
		return nil, fmt.Errorf("delivery: %w", err)
	}
	return &Worker{cfg: cfg, http: client, log: l, attempts: attempts}, nil
}

// Timeout bounds one job attempt: the HTTP timeout plus time for the
// endpoint lookup.
func (w *Worker) Timeout() time.Duration { return w.cfg.Timeout + 10*time.Second }

// Close releases idle connections.
func (w *Worker) Close(context.Context) error {
	w.http.CloseIdleConnections()
	return nil
}

// Work delivers one attempt.
func (w *Worker) Work(ctx context.Context, job *jobs.Job[Args]) error {
	start := time.Now()
	res := Result{Args: job.Args, Attempt: job.Attempt}
	code, err := w.send(ctx, job.Args)
	res.StatusCode, res.Duration = code, time.Since(start)

	var permanent *permanentError
	switch {
	case err == nil:
		res.Outcome = Delivered
	case code == http.StatusGone:
		res.Outcome, res.Err = Gone, err
		err = jobs.Cancel(err)
	case errors.As(err, &permanent), errors.Is(err, httpx.ErrDestinationNotAllowed):
		res.Outcome, res.Err = Cancelled, err
		err = jobs.Cancel(err)
	case job.Attempt >= job.MaxAttempts:
		res.Outcome, res.Err = Failed, err
	default:
		d := w.delay(job.Attempt, err)
		res.Outcome, res.Err, res.NextAttempt = Retrying, err, time.Now().Add(d)
		err = jobs.RetryAfter(d, err)
	}
	w.record(ctx, res)
	return err
}

// delay returns the wait before the next attempt: the schedule's entry,
// raised to the endpoint's Retry-After but never above the schedule's last
// entry, with ±10% jitter so retries of one outage spread out.
func (w *Worker) delay(attempt int, err error) time.Duration {
	s := w.cfg.Schedule
	d := s[min(max(attempt, 1), len(s))-1]
	if se, ok := errors.AsType[*httpx.StatusError](err); ok {
		if secs, perr := strconv.Atoi(se.Header.Get("Retry-After")); perr == nil && secs > 0 {
			d = max(d, min(time.Duration(secs)*time.Second, s[len(s)-1]))
		}
	}
	return timex.Jitter(d, 0.1)
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func permanent(format string, a ...any) error {
	return &permanentError{err: fmt.Errorf("delivery: "+format, a...)}
}

// send makes one attempt and returns the endpoint's status code.
func (w *Worker) send(ctx context.Context, a Args) (int, error) {
	ep, err := w.cfg.Endpoints.Endpoint(ctx, a.EndpointID)
	switch {
	case errors.Is(err, ErrEndpointNotFound):
		return 0, &permanentError{err: err}
	case err != nil:
		return 0, fmt.Errorf("delivery: look up endpoint: %w", err)
	case ep.Disabled:
		return 0, permanent("endpoint %q is disabled", a.EndpointID)
	}
	u, err := url.Parse(ep.URL)
	schemeOK := u != nil && (u.Scheme == "https" || u.Scheme == "http" && w.cfg.AllowHTTP)
	if err != nil || u.Host == "" || !schemeOK {
		return 0, permanent("endpoint %q has an invalid or non-https URL", a.EndpointID)
	}
	signer, err := webhook.NewSigner(ep.Secrets...)
	if err != nil {
		return 0, permanent("endpoint %q: %w", a.EndpointID, err)
	}
	header, err := signer.Headers(a.MessageID, time.Now(), a.Payload)
	if err != nil {
		return 0, permanent("%w", err)
	}
	req := w.http.Post(u.String()).Body(bytes.NewReader(a.Payload), "application/json")
	for k := range header {
		req.Header(k, header.Get(k))
	}
	res, err := req.Send(ctx)
	if err != nil {
		return httpx.StatusCode(err), err
	}
	_ = res.Body().Close()
	if code := res.StatusCode(); !res.IsSuccess() {
		// A redirect is not followed: the endpoint must answer at its URL.
		return code, fmt.Errorf("delivery: endpoint answered %d %s", code, http.StatusText(code))
	}
	return res.StatusCode(), nil
}

func (w *Worker) record(ctx context.Context, r Result) {
	attrs := []attribute.KeyValue{attribute.String("kitchen.webhook.outcome", string(r.Outcome))}
	if r.StatusCode != 0 {
		attrs = append(attrs, attribute.Int("http.response.status_code", r.StatusCode))
	}
	w.attempts.Add(ctx, 1, metric.WithAttributes(attrs...))
	if r.Outcome != Delivered {
		lvl := log.LevelWarn
		if r.Outcome == Retrying {
			lvl = log.LevelInfo
		}
		w.log.Log(ctx, lvl, "webhook: delivery attempt failed",
			log.String("webhook.id", r.Args.MessageID), log.String("webhook.endpoint_id", r.Args.EndpointID),
			log.String("webhook.event_type", r.Args.EventType), log.String("kitchen.webhook.outcome", string(r.Outcome)),
			log.Int("attempt", r.Attempt), log.Int("http.response.status_code", r.StatusCode), log.Err(r.Err))
	}
	if w.cfg.OnResult != nil {
		w.cfg.OnResult(ctx, r)
	}
}
