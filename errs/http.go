package errs

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"strconv"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// fallbackBody is written if marshaling ever fails.
const fallbackBody = `{"error":{"code":500,"message":"internal error","status":"INTERNAL"}}`

type httpEnvelope struct {
	Error httpError `json:"error"`
}

type httpError struct {
	Code    int               `json:"code"`
	Message string            `json:"message"`
	Status  string            `json:"status"`
	Details []json.RawMessage `json:"details,omitempty"`
}

// MarshalHTTP renders st as the AIP-193 HTTP error envelope and returns the
// body together with the HTTP status derived from st's code:
//
//	{"error": {"code": 404, "message": "...", "status": "NOT_FOUND",
//	  "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo", ...}]}}
//
// A nil st renders as INTERNAL.
func MarshalHTTP(st *status.Status) (body []byte, httpStatus int) {
	if st == nil {
		st = ErrInternal.GRPCStatus()
	}
	httpStatus = HTTPStatusFromCode(st.Code())
	env := httpEnvelope{Error: httpError{
		Code:    httpStatus,
		Message: st.Message(),
		Status:  reasonFor(st.Code()),
	}}
	for _, a := range st.Proto().GetDetails() {
		raw, err := marshalAny(a)
		if err != nil {
			continue // unknown detail type: skip rather than fail the response
		}
		env.Error.Details = append(env.Error.Details, raw)
	}
	b, err := json.Marshal(env)
	if err != nil {
		return []byte(fallbackBody), http.StatusInternalServerError
	}
	return b, httpStatus
}

func marshalAny(a *anypb.Any) (json.RawMessage, error) {
	b, err := protojson.Marshal(a)
	if err != nil {
		return nil, err
	}
	// protojson output is valid JSON but not byte-stable; compact it so the
	// envelope is deterministic in whitespace.
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// WriteHTTP writes err as an AIP-193 JSON error response. err is converted
// with ToStatus, so unknown errors become INTERNAL; log err yourself when
// that matters (ToStatus reports it). It sets Content-Type, Retry-After when
// the status carries a RetryInfo, and WWW-Authenticate for 401.
func WriteHTTP(w http.ResponseWriter, err error) {
	st, _ := ToStatus(err)
	if st == nil {
		st = ErrInternal.GRPCStatus()
	}
	body, code := MarshalHTTP(st)
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	if ra, ok := retryAfter(st); ok {
		h.Set("Retry-After", strconv.Itoa(ra))
	}
	if st.Code() == codes.Unauthenticated {
		h.Set("WWW-Authenticate", `Bearer`)
	}
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

// retryAfter returns RetryInfo.retry_delay rounded up to whole seconds.
func retryAfter(st *status.Status) (int, bool) {
	for _, d := range st.Details() {
		ri, ok := d.(*errdetails.RetryInfo)
		if !ok || ri.GetRetryDelay() == nil {
			continue
		}
		secs := math.Ceil(ri.GetRetryDelay().AsDuration().Seconds())
		if secs < 1 {
			secs = 1
		}
		return int(secs), true
	}
	return 0, false
}
