package server

import (
	"context"
	"io"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/dungnh3/kitchen/reqctx"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// JSONMarshaler is the default REST marshaler: proto field names
// (snake_case), enums as names, zero values emitted, unknown input fields
// ignored.
func JSONMarshaler() *runtime.JSONPb {
	return &runtime.JSONPb{
		MarshalOptions:   protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true},
		UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: true},
	}
}

func (s *Server) newGatewayMux() *runtime.ServeMux {
	forward := map[string]string{}
	for _, h := range append(reqctx.ForwardHeaders(), s.cfg.forwardHeaders...) {
		forward[textproto.CanonicalMIMEHeaderKey(h)] = strings.ToLower(h)
	}
	opts := []runtime.ServeMuxOption{
		runtime.WithMarshalerOption(runtime.MIMEWildcard, JSONMarshaler()),
		runtime.WithIncomingHeaderMatcher(func(key string) (string, bool) {
			if k, ok := forward[textproto.CanonicalMIMEHeaderKey(key)]; ok {
				return k, true
			}
			return runtime.DefaultHeaderMatcher(key)
		}),
		runtime.WithMetadata(s.gatewayMetadata),
		runtime.WithErrorHandler(s.gatewayError),
		runtime.WithMiddlewares(routeSpanName),
	}
	if s.cfg.streamingMarshalers {
		opts = append(opts,
			runtime.WithMarshalerOption(contentTypeSSE, &sseMarshaler{JSONPb: *JSONMarshaler()}),
			runtime.WithMarshalerOption(contentTypeNDJSON, &ndjsonMarshaler{JSONPb: *JSONMarshaler()}),
		)
	}
	return runtime.NewServeMux(append(opts, s.cfg.gatewayOpts...)...)
}

// gatewayMetadata passes the resolved client IP and the trace context to the
// in-process gRPC call.
func (s *Server) gatewayMetadata(ctx context.Context, _ *http.Request) metadata.MD {
	md := metadata.MD{}
	if info, ok := reqctx.FromContext(ctx); ok {
		if info.ClientIP.IsValid() {
			md.Set(reqctx.HeaderClientIP, info.ClientIP.String())
		}
		md.Set(reqctx.HeaderRequestID, info.RequestID)
	}
	s.propagator.Inject(ctx, metadataCarrier(md))
	return md
}

func (s *Server) gatewayError(ctx context.Context, mux *runtime.ServeMux, m runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
	if s.cfg.httpErrorHandler != nil {
		s.cfg.httpErrorHandler(ctx, mux, m, w, r, err)
		return
	}
	if s.cfg.httpStatus != nil {
		st, _ := statusFromError(err)
		if code := s.cfg.httpStatus(st); code >= 400 && code <= 599 {
			err = &runtime.HTTPStatusError{HTTPStatus: code, Err: err}
		}
	}
	runtime.DefaultHTTPErrorHandler(ctx, mux, m, w, r, err)
}

// routeSpanName names the HTTP server span after the matched gateway route.
func routeSpanName(next runtime.HandlerFunc) runtime.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, p map[string]string) {
		if route, ok := runtime.HTTPPathPattern(r.Context()); ok {
			span := trace.SpanFromContext(r.Context())
			span.SetName(r.Method + " " + route)
			span.SetAttributes(attribute.String("http.route", route))
		}
		next(w, r, p)
	}
}

type metadataCarrier metadata.MD

var _ propagation.TextMapCarrier = metadataCarrier{}

func (c metadataCarrier) Get(k string) string {
	if v := metadata.MD(c).Get(k); len(v) > 0 {
		return v[0]
	}
	return ""
}
func (c metadataCarrier) Set(k, v string) { metadata.MD(c).Set(k, v) }
func (c metadataCarrier) Keys() []string {
	out := make([]string, 0, len(c))
	for k := range c {
		out = append(out, k)
	}
	return out
}

const (
	contentTypeSSE    = "text/event-stream"
	contentTypeNDJSON = "application/x-ndjson"
)

// unwrapChunk returns the message of a grpc-gateway stream chunk
// ({"result": msg}) or error chunk ({"error": status}), and whether it was
// an error.
func unwrapChunk(v any) (msg any, isErr bool) {
	switch c := v.(type) {
	case map[string]any:
		if r, ok := c["result"]; ok && len(c) == 1 {
			return r, false
		}
		if e, ok := c["error"]; ok && len(c) == 1 {
			return e, true
		}
	case map[string]proto.Message:
		if r, ok := c["result"]; ok && len(c) == 1 {
			return r, false
		}
		if e, ok := c["error"]; ok && len(c) == 1 {
			return e, true
		}
	}
	return v, false
}

// sseMarshaler writes each streamed message as one Server-Sent Event:
// "data: <json>\n\n"; a stream error is "event: error\ndata: <status>\n\n".
type sseMarshaler struct{ runtime.JSONPb }

func (*sseMarshaler) ContentType(any) string { return contentTypeSSE }
func (*sseMarshaler) Delimiter() []byte      { return []byte("\n\n") }

func (m *sseMarshaler) Marshal(v any) ([]byte, error) {
	msg, isErr := unwrapChunk(v)
	b, err := m.JSONPb.Marshal(msg)
	if err != nil {
		return nil, err
	}
	prefix := "data: "
	if isErr {
		prefix = "event: error\ndata: "
	}
	return append([]byte(prefix), b...), nil
}

func (m *sseMarshaler) NewEncoder(w io.Writer) runtime.Encoder {
	return runtime.EncoderFunc(func(v any) error {
		b, err := m.Marshal(v)
		if err != nil {
			return err
		}
		_, err = w.Write(append(b, m.Delimiter()...))
		return err
	})
}

// ndjsonMarshaler writes each streamed message as one JSON line.
type ndjsonMarshaler struct{ runtime.JSONPb }

func (*ndjsonMarshaler) ContentType(any) string { return contentTypeNDJSON }
func (*ndjsonMarshaler) Delimiter() []byte      { return []byte("\n") }

func (m *ndjsonMarshaler) Marshal(v any) ([]byte, error) {
	msg, _ := unwrapChunk(v)
	return m.JSONPb.Marshal(msg)
}
