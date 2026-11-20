package server

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/dungnh3/kitchen/reqctx"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/cors"
	"google.golang.org/grpc/status"
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
		remote := reqctx.AddrFromString(r.RemoteAddr)
		trusted := s.cfg.trustAllIdentity || s.cfg.trustedIdentityPeers.Contains(remote)
		if !trusted {
			for _, h := range reqctx.IdentityHeaders() {
				r.Header.Del(h)
			}
		}
		info := reqctx.FromHTTP(r, s.cfg.trustedProxies)
		if info.RequestID == "" {
			info.RequestID = reqctx.NewRequestID()
		}
		r.Header.Set(reqctx.HeaderRequestID, info.RequestID)
		w.Header().Set(reqctx.HeaderRequestID, info.RequestID)
		ctx := reqctx.NewContext(r.Context(), info)
		if trusted {
			uid := r.Header.Get(reqctx.HeaderUserID)
			if uid == "" {
				uid = r.Header.Get(reqctx.HeaderXUserID)
			}
			if uid != "" {
				ctx = reqctx.WithUserID(ctx, uid)
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
		defer func() {
			p := recover()
			if p == nil {
				return
			}
			if err, ok := p.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(p)
			}
			_ = s.recovered(r.Context(), r.Method+" "+r.URL.Path, p)
			if !rec.wroteHeader {
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(rec, r)
	})
}

// accessLog logs each request: 5xx at error, 4xx at info, others at debug.
func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		code := rec.status()
		lvl := slog.LevelDebug
		switch {
		case code >= 500:
			lvl = slog.LevelError
		case code >= 400:
			lvl = slog.LevelInfo
		}
		ctx := r.Context()
		if !s.log.Enabled(ctx, lvl) {
			return
		}
		attrs := []slog.Attr{
			slog.String("http.request.method", r.Method),
			slog.String("url.path", r.URL.Path),
			slog.Int("http.response.status_code", code),
			slog.Int64("http.response.body.size", rec.bytes),
			slog.Duration("duration", time.Since(start)),
		}
		if info, ok := reqctx.FromContext(ctx); ok {
			attrs = append(attrs, slog.String("request_id", info.RequestID))
			if info.ClientIP.IsValid() {
				attrs = append(attrs, slog.String("client.address", info.ClientIP.String()))
			}
		}
		s.log.LogAttrs(ctx, lvl, "http request", attrs...)
	})
}

// limitBody rejects gateway request bodies over n bytes: 413 when the
// declared length is too large, a read error otherwise.
func limitBody(n int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > n {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, n)
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

// statusFromError is status.FromError that also unwraps
// runtime.HTTPStatusError.
func statusFromError(err error) (*status.Status, bool) {
	var hse *runtime.HTTPStatusError
	if errors.As(err, &hse) {
		err = hse.Err
	}
	return status.FromError(err)
}
