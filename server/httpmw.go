package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/cors"
)

// sanitize removes client-sent gRPC metadata headers and, from untrusted
// peers, identity headers; ensures a valid X-Request-Id (echoed in the
// response); and stores the request Info (and a trusted user id) in the
// request context.
func (s *Server) sanitize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k := range r.Header {
			if len(k) >= len(runtime.MetadataHeaderPrefix) &&
				strings.EqualFold(k[:len(runtime.MetadataHeaderPrefix)], runtime.MetadataHeaderPrefix) {
				r.Header.Del(k)
			}
		}
		remote := xcontext.AddrFromHostPort(r.RemoteAddr)
		trusted := s.cfg.trustAllIdentity || s.cfg.trustedIdentityPeers.Contains(remote)
		if !trusted {
			for _, h := range xcontext.IdentityHeaders() {
				if len(r.Header.Values(h)) > 0 {
					s.warnStrippedIdentity(r.Context(), "http", remote, h)
				}
				r.Header.Del(h)
			}
		}
		info := xcontext.FromHTTP(r, s.infoOptions())
		if info.RequestID == "" {
			info.RequestID = xcontext.NewRequestID()
		}
		r.Header.Set(xcontext.HeaderRequestID, info.RequestID)
		w.Header().Set(xcontext.HeaderRequestID, info.RequestID)
		ctx := xcontext.NewContext(r.Context(), info)
		if trusted {
			if uid := strings.TrimSpace(r.Header.Get(xcontext.HeaderUserID)); uid != "" {
				ctx = xcontext.WithUserID(ctx, uid)
			}
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// recoverHTTP turns a handler panic into a 500 (when nothing was written
// yet) and logs it. http.ErrAbortHandler is re-raised, as net/http expects.
func (s *Server) recoverHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &recorder{ResponseWriter: w}
		defer func() { //nolint:contextcheck // r.Context() is passed to s.recovered.
			p := recover()
			if p == nil {
				return
			}
			if err, ok := p.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(p)
			}
			err := s.recovered(r.Context(), r.Method+" "+r.URL.Path, p)
			if !rec.wroteHeader {
				s.errs.WriteHTTP(w, r, err)
			}
		}()
		next.ServeHTTP(rec, r)
	})
}

// accessLog logs each request: 5xx at error, 4xx at info, others at debug.
// A request whose handler panics is logged as 500 before the panic goes on
// to the recovery middleware.
func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w}
		defer func() {
			if p := recover(); p != nil {
				s.logAccess(r, rec, http.StatusInternalServerError, start)
				panic(p)
			}
		}()
		next.ServeHTTP(rec, r)
		s.logAccess(r, rec, rec.status(), start)
	})
}

func (s *Server) logAccess(r *http.Request, rec *recorder, code int, start time.Time) {
	lvl := log.LevelDebug
	switch {
	case code >= 500:
		lvl = log.LevelError
	case code >= 400:
		lvl = log.LevelInfo
	}
	ctx := r.Context()
	if !s.log.Enabled(ctx, lvl) {
		return
	}
	attrs := []log.Attr{
		log.String("http.request.method", r.Method),
		log.String("url.path", r.URL.Path),
		log.Int("http.response.status_code", code),
		log.Int64("http.response.body.size", rec.bytes),
		log.Duration("duration", time.Since(start)),
	}
	if info, ok := xcontext.FromContext(ctx); ok {
		if info.ClientIP.IsValid() {
			attrs = append(attrs, log.String("client.address", info.ClientIP.String()))
		}
	}
	s.log.LogAttrs(ctx, lvl, "http request", attrs...)
}

// limitBody rejects gateway request bodies over n bytes: PAYLOAD_TOO_LARGE
// (HTTP 413, see errs.HTTPStatusFromStatus) when the declared length is too large, a read error
// otherwise.
func (s *Server) limitBody(n int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > n {
			s.errs.WriteHTTP(w, r, errs.ErrPayloadTooLarge.With(errs.Meta("limit_bytes", strconv.FormatInt(n, 10))))
			return
		}
		st := &bodyLimit{n: n}
		r = r.WithContext(context.WithValue(r.Context(), bodyLimitKey{}, st))
		r.Body = &limitedBody{ReadCloser: http.MaxBytesReader(w, r.Body, n), st: st}
		next.ServeHTTP(w, r)
	})
}

func corsHandler(cfg CORSConfig, next http.Handler) http.Handler {
	return cors.New(cors.Options{
		AllowedOrigins:   cfg.AllowedOrigins,
		AllowedMethods:   cfg.AllowedMethods,
		AllowedHeaders:   cfg.AllowedHeaders,
		ExposedHeaders:   cfg.ExposedHeaders,
		AllowCredentials: cfg.AllowCredentials,
		MaxAge:           int(cfg.MaxAge / time.Second),
	}).Handler(next)
}

// recorder captures the status and size while keeping Flush, Hijack and
// http.ResponseController working.
type recorder struct {
	http.ResponseWriter
	code        int
	bytes       int64
	wroteHeader bool
}

func (r *recorder) WriteHeader(code int) {
	if !r.wroteHeader && code >= 200 {
		r.code, r.wroteHeader = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.code, r.wroteHeader = http.StatusOK, true
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

func (r *recorder) status() int {
	if r.code == 0 {
		return http.StatusOK
	}
	return r.code
}

func (r *recorder) Flush() {
	if !r.wroteHeader {
		r.code, r.wroteHeader = http.StatusOK, true
	}
	_ = http.NewResponseController(r.ResponseWriter).Flush()
}

func (r *recorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(r.ResponseWriter).Hijack()
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// userIDHTTP stores the authenticated user id (WithUserIDFrom) after the
// user middlewares have authenticated the request.
func (s *Server) userIDHTTP(next http.Handler) http.Handler {
	if s.cfg.userID == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(s.withUserID(r.Context())))
	})
}

type bodyLimitKey struct{}

// bodyLimit records that a request body hit the limit, for gatewayError.
type bodyLimit struct {
	n        int64
	exceeded atomic.Bool
}

type limitedBody struct {
	io.ReadCloser
	st *bodyLimit
}

func (b *limitedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	var mbe *http.MaxBytesError
	if err != nil && errors.As(err, &mbe) {
		b.st.exceeded.Store(true)
	}
	return n, err
}
