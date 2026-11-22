package errs

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// rendered is an error ready to be sent: its code, client message, ErrorInfo
// and other details, decoded once. Handler builds it straight from an *Error
// (no Any round trip) or from a status, and turns it into a gRPC status or an
// HTTP response.
type rendered struct {
	code      codes.Code
	message   string
	info      *errdetails.ErrorInfo // first ErrorInfo; set by complete
	details   []proto.Message       // other details, in order
	retry     *errdetails.RetryInfo // also in details
	localized bool                  // details has a LocalizedMessage
}

// renderError is ToStatus without the status: the same sanitization rules,
// with an *Error used directly.
func renderError(err error) (r rendered, sanitized bool) {
	var e *Error
	if errors.As(err, &e) {
		if e == nil || e.code == codes.OK {
			return renderOf(ErrInternal), true
		}
		return renderOf(e), false
	}
	st, sanitized := ToStatus(err)
	return renderStatus(st), sanitized
}

// renderOf renders a local *Error. info aliases nothing: complete copies it
// before changing the domain.
func renderOf(e *Error) rendered {
	r := rendered{code: e.code, message: e.Message(), info: e.info(), details: e.details}
	for _, d := range e.details {
		r.note(d)
	}
	return r
}

// renderStatus decodes st's details once. Details of unknown types are
// dropped.
func renderStatus(st *status.Status) rendered {
	r := rendered{code: st.Code(), message: st.Message()}
	for _, d := range st.Details() {
		m, ok := d.(proto.Message)
		if !ok {
			continue
		}
		if info, ok := m.(*errdetails.ErrorInfo); ok && r.info == nil {
			r.info = info
			continue
		}
		r.details = append(r.details, m)
		r.note(m)
	}
	return r
}

func (r *rendered) note(m proto.Message) {
	switch d := m.(type) {
	case *errdetails.RetryInfo:
		if r.retry == nil {
			r.retry = d
		}
	case *errdetails.LocalizedMessage:
		r.localized = true
	}
}

// complete ensures r has an ErrorInfo with a domain and, with a Localizer, a
// LocalizedMessage (appended last).
func (h *Handler) completeRendered(ctx context.Context, r *rendered) {
	switch {
	case r.info == nil:
		r.info = &errdetails.ErrorInfo{Reason: CodeName(r.code), Domain: h.domain}
	case r.info.GetDomain() == "" && h.domain != "":
		info := &errdetails.ErrorInfo{Reason: r.info.GetReason(), Domain: h.domain, Metadata: r.info.GetMetadata()}
		r.info = info
	}
	if h.loc != nil && !r.localized {
		if locale, msg, ok := h.loc.Localize(ctx, r.info); ok {
			r.details = append(r.details[:len(r.details):len(r.details)], &errdetails.LocalizedMessage{Locale: locale, Message: msg})
			r.localized = true
		}
	}
}

// status returns r as a gRPC status: ErrorInfo first, then the details.
func (r rendered) status() *status.Status {
	st := status.New(r.code, r.message)
	ds := make([]protoadapt.MessageV1, 0, 1+len(r.details))
	if r.info != nil {
		ds = append(ds, r.info)
	}
	for _, d := range r.details {
		ds = append(ds, protoadapt.MessageV1Of(d))
	}
	if len(ds) == 0 {
		return st
	}
	if withDetails, err := st.WithDetails(ds...); err == nil {
		return withDetails
	}
	return st
}

// write writes r as the AIP-193 envelope. httpStatus 0 means the status
// derived from the code and reason (HTTPStatusFromStatus).
func (r rendered) write(w http.ResponseWriter, httpStatus int) {
	if httpStatus == 0 {
		httpStatus = httpStatusOf(r.code, r.info.GetReason())
	}
	body, code := r.marshal(httpStatus)
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cache-Control", "no-store")
	if ra, ok := retryAfterOf(r.retry); ok {
		h.Set("Retry-After", strconv.Itoa(ra))
	}
	if v := wwwAuthenticate(r.code, r.info); v != "" {
		h.Set("WWW-Authenticate", v)
	}
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

// JSON forms of the common details, field for field what protojson writes
// for them inside an Any (camelCase names, empty fields omitted).
type (
	jsonErrorInfo struct {
		Type     string            `json:"@type"`
		Reason   string            `json:"reason,omitempty"`
		Domain   string            `json:"domain,omitempty"`
		Metadata map[string]string `json:"metadata,omitempty"`
	}
	jsonLocalized struct {
		Type    string `json:"@type"`
		Locale  string `json:"locale,omitempty"`
		Message string `json:"message,omitempty"`
	}
	jsonRetryInfo struct {
		Type       string `json:"@type"`
		RetryDelay string `json:"retryDelay,omitempty"`
	}
	jsonEnvelope struct {
		Error jsonError `json:"error"`
	}
	jsonError struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
		Details []any  `json:"details,omitempty"`
	}
)

const typePrefix = "type.googleapis.com/"

var (
	typeErrorInfo = typePrefix + string((&errdetails.ErrorInfo{}).ProtoReflect().Descriptor().FullName())
	typeLocalized = typePrefix + string((&errdetails.LocalizedMessage{}).ProtoReflect().Descriptor().FullName())
	typeRetryInfo = typePrefix + string((&errdetails.RetryInfo{}).ProtoReflect().Descriptor().FullName())
)

// marshal renders the envelope. ErrorInfo, LocalizedMessage and RetryInfo
// are encoded directly; other details through protojson.
func (r rendered) marshal(httpStatus int) ([]byte, int) {
	env := jsonEnvelope{Error: jsonError{Code: httpStatus, Message: r.message, Status: CodeName(r.code)}}
	if r.info != nil {
		env.Error.Details = append(env.Error.Details, jsonErrorInfo{
			Type: typeErrorInfo, Reason: r.info.GetReason(), Domain: r.info.GetDomain(), Metadata: r.info.GetMetadata(),
		})
	}
	for _, d := range r.details {
		switch m := d.(type) {
		case *errdetails.LocalizedMessage:
			env.Error.Details = append(env.Error.Details, jsonLocalized{Type: typeLocalized, Locale: m.GetLocale(), Message: m.GetMessage()})
		case *errdetails.RetryInfo:
			j := jsonRetryInfo{Type: typeRetryInfo}
			if m.GetRetryDelay() != nil {
				j.RetryDelay = durationJSON(m.GetRetryDelay())
			}
			env.Error.Details = append(env.Error.Details, j)
		default:
			a, err := anypb.New(m)
			if err != nil {
				continue
			}
			raw, err := protojson.Marshal(a)
			if err != nil {
				continue
			}
			env.Error.Details = append(env.Error.Details, json.RawMessage(raw))
		}
	}
	b, err := json.Marshal(env)
	if err != nil {
		return []byte(fallbackBody), http.StatusInternalServerError
	}
	return b, httpStatus
}

// durationJSON formats d like protojson: seconds with 0, 3, 6 or 9
// fractional digits and an "s" suffix ("1s", "1.500s", "-0.000001s").
func durationJSON(d *durationpb.Duration) string {
	secs, nanos := d.GetSeconds(), int64(d.GetNanos())
	var b strings.Builder
	if secs < 0 || nanos < 0 {
		b.WriteByte('-')
		secs, nanos = -secs, -nanos
	}
	b.WriteString(strconv.FormatInt(secs, 10))
	if nanos != 0 {
		frac := strconv.FormatInt(nanos+1e9, 10)[1:] // 9 digits, zero padded
		switch {
		case strings.HasSuffix(frac, "000000"):
			frac = frac[:3]
		case strings.HasSuffix(frac, "000"):
			frac = frac[:6]
		}
		b.WriteByte('.')
		b.WriteString(frac)
	}
	b.WriteByte('s')
	return b.String()
}

// retryAfterOf returns RetryInfo.retry_delay rounded up to whole seconds.
func retryAfterOf(ri *errdetails.RetryInfo) (int, bool) {
	if ri.GetRetryDelay() == nil {
		return 0, false
	}
	secs := math.Ceil(ri.GetRetryDelay().AsDuration().Seconds())
	if secs < 1 {
		secs = 1
	}
	return int(secs), true
}
