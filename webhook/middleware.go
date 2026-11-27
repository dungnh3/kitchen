package webhook

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
)

// DefaultMaxBodyBytes is the default body limit of [NewMiddleware].
const DefaultMaxBodyBytes = 1 << 20

// settleTimeout bounds the Dedup call made after the handler returns.
const settleTimeout = 5 * time.Second

// MiddlewareConfig configures [NewMiddleware].
type MiddlewareConfig struct {
	// Verifier checks each request. Required.
	Verifier Verifier
	// MaxBodyBytes is the largest accepted body (default 1 MiB). A larger
	// one is refused with 413 before it is verified.
	MaxBodyBytes int64
	// Dedup, when set, runs the handler at most once per message id (see
	// [Dedup]). nil turns deduplication off.
	Dedup Dedup
	// Errors renders refusals (nil: errs.WriteHTTP). Pass the server's
	// handler so refusals are localized like every other error.
	Errors *errs.Handler
	// Logger logs refusals at debug level and Dedup failures (nil:
	// log.Default()).
	Logger *log.Logger
}

type messageKey struct{}

// FromContext returns the verified message of a request that passed the
// middleware.
func FromContext(ctx context.Context) (Message, bool) {
	m, ok := ctx.Value(messageKey{}).(Message)
	return m, ok
}

// NewMiddleware returns an HTTP middleware that lets through only verified
// webhooks. For each request it:
//
//  1. reads the body up to MaxBodyBytes (413 PAYLOAD_TOO_LARGE above it);
//  2. verifies it (401 WEBHOOK_SIGNATURE_INVALID on any failure);
//  3. with Dedup, claims the message id: a message already handled gets 200
//     without calling the handler, one being handled by another request gets
//     409 REQUEST_IN_PROGRESS, and a Dedup failure gets 503 so the sender
//     retries later;
//  4. calls the handler with r.Body replaced by the raw body and the
//     [Message] in the context.
//
// With Dedup, a handler response of 2xx marks the id as handled; any other
// response, or a panic, releases it so the sender's retry runs the handler
// again.
//
// Routes behind this middleware authenticate by signature, so list them as
// public in the server's bearer authenticator.
func NewMiddleware(cfg MiddlewareConfig) (func(http.Handler) http.Handler, error) {
	if cfg.Verifier == nil {
		return nil, errors.New("webhook: middleware needs a Verifier")
	}
	if cfg.MaxBodyBytes < 0 {
		return nil, errors.New("webhook: negative MaxBodyBytes")
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = DefaultMaxBodyBytes
	}
	m := &middleware{cfg: cfg, log: log.OrDefault(cfg.Logger)}
	return m.wrap, nil
}

type middleware struct {
	cfg MiddlewareConfig
	log *log.Logger
}

func (m *middleware) fail(w http.ResponseWriter, r *http.Request, err error) {
	if m.cfg.Errors != nil {
		m.cfg.Errors.WriteHTTP(w, r, err)
		return
	}
	errs.WriteHTTP(w, r, err)
}

func (m *middleware) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		limit := strconv.FormatInt(m.cfg.MaxBodyBytes, 10)
		if r.ContentLength > m.cfg.MaxBodyBytes {
			m.fail(w, r, errs.ErrPayloadTooLarge.With(errs.Meta("limit_bytes", limit)))
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, m.cfg.MaxBodyBytes))
		if err != nil {
			if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
				m.fail(w, r, errs.ErrPayloadTooLarge.With(errs.Meta("limit_bytes", limit)))
				return
			}
			m.fail(w, r, errs.Wrap(err, errs.ErrMalformedRequest))
			return
		}
		msg, err := m.cfg.Verifier.Verify(ctx, r.Header, body)
		if err != nil {
			m.log.DebugContext(ctx, "webhook: request refused", log.Err(err))
			if !errors.Is(err, ErrSignatureInvalid) {
				err = errs.Wrap(err, ErrSignatureInvalid) // a custom Verifier's error still fails closed as 401
			}
			m.fail(w, r, err)
			return
		}
		var ticket Ticket
		if m.cfg.Dedup != nil {
			ticket, err = m.cfg.Dedup.Begin(ctx, msg.ID)
			switch {
			case errors.Is(err, ErrDuplicate):
				w.WriteHeader(http.StatusOK)
				return
			case errors.Is(err, ErrInProgress):
				m.fail(w, r, errs.Wrap(err, errs.ErrRequestInProgress))
				return
			case err != nil:
				m.log.ErrorContext(ctx, "webhook: dedup unavailable", log.String("webhook.id", msg.ID), log.Err(err))
				m.fail(w, r, errs.Wrap(err, errs.ErrDependencyUnavailable))
				return
			}
		}
		r = r.WithContext(context.WithValue(ctx, messageKey{}, msg))
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		if ticket == nil {
			next.ServeHTTP(w, r)
			return
		}
		rec := &statusRecorder{ResponseWriter: w}
		ok := false
		defer func() {
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
			defer cancel()
			settle := ticket.Release
			if ok {
				settle = ticket.Done
			}
			if err := settle(sctx); err != nil {
				m.log.WarnContext(ctx, "webhook: dedup update failed", log.String("webhook.id", msg.ID), log.Err(err))
			}
		}()
		next.ServeHTTP(rec, r)
		ok = rec.code == 0 || rec.code >= 200 && rec.code <= 299
	})
}

// statusRecorder records the response status.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.code == 0 && code >= 200 {
		s.code = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
