package server

import (
	"context"
	"io"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/ratelimit"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	for _, h := range append(xcontext.DefaultForwardHeaders(), s.cfg.forwardHeaders...) {
		forward[textproto.CanonicalMIMEHeaderKey(h)] = strings.ToLower(h)
	}
	// Identity headers are not forwarded as sent: gatewayMetadata passes the
	// user id the HTTP side established (auth, WithUserIDFrom or a trusted
	// peer), so there is exactly one value.
	for _, h := range xcontext.IdentityHeaders() {
		delete(forward, textproto.CanonicalMIMEHeaderKey(h))
	}
	opts := []runtime.ServeMuxOption{
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &jsonMarshaler{JSONPb: *JSONMarshaler()}),
		runtime.WithIncomingHeaderMatcher(func(key string) (string, bool) {
			if k, ok := forward[textproto.CanonicalMIMEHeaderKey(key)]; ok {
				return k, true
			}
			return runtime.DefaultHeaderMatcher(key)
		}),
		runtime.WithMetadata(s.gatewayMetadata),
		runtime.WithErrorHandler(s.gatewayError),
		runtime.WithRoutingErrorHandler(s.routingError),
		runtime.WithStreamErrorHandler(s.streamError),
		runtime.WithMiddlewares(routeSpanName),
	}
	if s.cfg.rateLimit != nil {
		pass := map[string]bool{}
		for _, h := range ratelimit.ResponseHeaders() {
			pass[h] = true
		}
		opts = append(opts, runtime.WithOutgoingHeaderMatcher(func(key string) (string, bool) {
			if pass[strings.ToLower(key)] {
				return key, true
			}
			return runtime.MetadataHeaderPrefix + key, true // grpc-gateway's default
		}))
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
	if info, ok := xcontext.FromContext(ctx); ok {
		if info.ClientIP.IsValid() {
			md.Set(xcontext.HeaderClientIP, info.ClientIP.String())
		}
		md.Set(xcontext.HeaderRequestID, info.RequestID)
	}
	if uid := xcontext.UserID(ctx); uid != "" {
		md.Set(xcontext.HeaderUserID, uid)
	}
	s.propagator.Inject(ctx, metadataCarrier(md))
	return md
}

// gatewayError renders REST errors with the errs.Handler (AIP-193 envelope,
// default domain, localization), unless WithHTTPErrorHandler replaced it.
func (s *Server) gatewayError(ctx context.Context, mux *runtime.ServeMux, m runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
	if s.cfg.httpErrorHandler != nil {
		s.cfg.httpErrorHandler(ctx, mux, m, w, r, err)
		return
	}
	// A chunked body over the limit fails inside the generated decoder as
	// INVALID_ARGUMENT; report it as PAYLOAD_TOO_LARGE like a declared one.
	if st, ok := r.Context().Value(bodyLimitKey{}).(*bodyLimit); ok {
		if st.exceeded.Load() {
			s.errs.WriteHTTP(w, r, errs.ErrPayloadTooLarge.With(errs.Meta("limit_bytes", strconv.FormatInt(st.n, 10))))
			return
		}
		// The body did not arrive within WithGatewayBodyTimeout: 408.
		if st.timedOut.Load() {
			s.errs.GatewayErrorHandler(ctx, mux, m, w, r, &runtime.HTTPStatusError{
				HTTPStatus: http.StatusRequestTimeout,
				Err:        errs.ErrDeadlineExceeded.With(errs.Meta("timeout", s.cfg.gatewayBodyTimeout.String())),
			})
			return
		}
	}
	// Generated gateway code turns a body, query or path parameter that
	// cannot be parsed into a bare InvalidArgument status. Every service
	// behind the gateway goes through the errs interceptor, which always adds
	// an ErrorInfo, so a bare one is the gateway's own parse failure: report
	// it as MALFORMED_REQUEST (localized), keeping the parser's message as
	// "detail" for the client.
	if st, ok := status.FromError(err); ok && st.Code() == codes.InvalidArgument {
		if _, hasInfo := errs.Info(err); !hasInfo {
			s.errs.WriteHTTP(w, r, errs.Wrap(err, errs.ErrMalformedRequest, errs.Meta("detail", truncate(st.Message(), 200))))
			return
		}
	}
	s.errs.GatewayErrorHandler(ctx, mux, m, w, r, err)
}

// routingError renders the gateway's routing failures as kitchen errors
// through gatewayError (so WithHTTPErrorHandler still applies). They are
// client errors: none is logged at ERROR, so scanners hitting wrong methods
// do not flood the error log.
//
//   - 404: NOT_FOUND.
//   - 405 (the path exists with another method): HTTP 405, UNIMPLEMENTED.
//     No Allow header: grpc-gateway does not report the allowed methods.
//   - 400 (a path that cannot be unescaped): MALFORMED_REQUEST.
func (s *Server) routingError(ctx context.Context, mux *runtime.ServeMux, m runtime.Marshaler, w http.ResponseWriter, r *http.Request, httpStatus int) {
	var err error
	switch httpStatus {
	case http.StatusNotFound:
		err = errs.ErrNotFound
	case http.StatusMethodNotAllowed:
		err = &runtime.HTTPStatusError{HTTPStatus: http.StatusMethodNotAllowed, Err: errs.ErrUnimplemented}
	case http.StatusBadRequest:
		err = errs.ErrMalformedRequest
	default:
		err = errs.ErrInternal
	}
	s.gatewayError(ctx, mux, m, w, r, err)
}

// streamError converts an error that ends a REST stream. A status the errs
// interceptor completed (it has an ErrorInfo) is kept; anything else, such as
// a marshaling failure inside the gateway, is converted (sanitized and
// logged) by the errs.Handler, so no internal error text reaches the client.
func (s *Server) streamError(ctx context.Context, err error) *status.Status {
	if _, ok := errs.Info(err); ok {
		if st, ok := status.FromError(err); ok {
			return st
		}
	}
	return s.errs.Status(ctx, err)
}

// errorEnvelope returns the AIP-193 HTTP error envelope
// ({"error": {"code": 503, "status": "UNAVAILABLE", ...}}, one line) when v
// is the error chunk grpc-gateway writes at the end of a failed stream.
func errorEnvelope(v any) ([]byte, bool) {
	msg, isErr := unwrapChunk(v)
	if !isErr {
		return nil, false
	}
	sp, ok := msg.(*spb.Status)
	if !ok || sp.GetCode() <= 0 { // OK, or not a gRPC code
		return nil, false
	}
	b, _ := errs.MarshalHTTP(errorFromProto(sp).GRPCStatus())
	return b, true
}

// errorFromProto rebuilds, through errs, the error streamError chose, from
// the google.rpc.Status grpc-gateway hands to the marshaler (errs.FromStatus
// for a proto): the first ErrorInfo gives reason, domain and metadata, the
// other details are kept, and a status without one gets the code name as
// reason.
func errorFromProto(sp *spb.Status) *errs.Error {
	code := codes.Unknown
	if c := sp.GetCode(); c > 0 {
		code = codes.Code(uint32(c))
	}
	reason := errs.CodeName(code)
	opts := []errs.Option{errs.Message(sp.GetMessage())}
	var details []proto.Message
	haveInfo := false
	for _, a := range sp.GetDetails() {
		m, err := a.UnmarshalNew()
		if err != nil {
			continue // unknown detail type
		}
		if info, ok := m.(*errdetails.ErrorInfo); ok && !haveInfo {
			haveInfo = true
			reason = info.GetReason()
			opts = append(opts, errs.Domain(info.GetDomain()))
			for k, v := range info.GetMetadata() {
				opts = append(opts, errs.Meta(k, v))
			}
			continue
		}
		details = append(details, m)
	}
	if len(details) > 0 {
		opts = append(opts, errs.Details(details...))
	}
	return errs.New(code, reason, opts...)
}

// jsonMarshaler is JSONMarshaler with the stream error chunk rendered as the
// AIP-193 envelope; results stay {"result": msg}.
type jsonMarshaler struct{ runtime.JSONPb }

func (m *jsonMarshaler) Marshal(v any) ([]byte, error) {
	if b, ok := errorEnvelope(v); ok {
		return b, nil
	}
	return m.JSONPb.Marshal(v)
}

// truncate cuts s to at most n bytes and returns valid UTF-8 (the parser
// message can echo client input).
func truncate(s string, n int) string {
	if len(s) <= n {
		return strings.ToValidUTF8(s, "\uFFFD")
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
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
// "data: <json>\n\n"; a stream error is "event: error\ndata: <AIP-193
// envelope>\n\n".
type sseMarshaler struct{ runtime.JSONPb }

func (*sseMarshaler) ContentType(any) string { return contentTypeSSE }
func (*sseMarshaler) Delimiter() []byte      { return []byte("\n\n") }

func (m *sseMarshaler) Marshal(v any) ([]byte, error) {
	if b, ok := errorEnvelope(v); ok {
		return append([]byte("event: error\ndata: "), b...), nil
	}
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

// ndjsonMarshaler writes each streamed message as one JSON line; a stream
// error is the AIP-193 envelope line {"error": {...}}.
type ndjsonMarshaler struct{ runtime.JSONPb }

func (*ndjsonMarshaler) ContentType(any) string { return contentTypeNDJSON }
func (*ndjsonMarshaler) Delimiter() []byte      { return []byte("\n") }

func (m *ndjsonMarshaler) Marshal(v any) ([]byte, error) {
	if b, ok := errorEnvelope(v); ok {
		return b, nil
	}
	msg, _ := unwrapChunk(v)
	return m.JSONPb.Marshal(msg)
}
