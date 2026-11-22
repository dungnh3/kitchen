package errs

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The hand-encoded details must decode to exactly what protojson writes for
// the same status (the previous encoder).
func TestRenderMatchesProtojson(t *testing.T) {
	delays := []time.Duration{0, time.Second, 1500 * time.Millisecond, 1500 * time.Microsecond, 1, 90 * time.Minute, -2500 * time.Millisecond}
	for _, d := range delays {
		st, err := status.New(codes.ResourceExhausted, "slow <down> & \"stop\"\u2028").WithDetails(
			&errdetails.ErrorInfo{Reason: "RATE_LIMITED", Domain: "svc", Metadata: map[string]string{"policy": "a/b", "x": "ü"}},
			&errdetails.RetryInfo{RetryDelay: durationpb.New(d)},
			&errdetails.LocalizedMessage{Locale: "vi", Message: "Chậm lại"},
			&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: "a", Description: "b"}}},
			&errdetails.ErrorInfo{Reason: "SECOND"},
			&errdetails.RetryInfo{},
		)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := marshalHTTP(st, 429)
		var want []any
		for _, a := range st.Proto().GetDetails() {
			b, err := protojson.Marshal(a)
			if err != nil {
				t.Fatal(err)
			}
			var v any
			_ = json.Unmarshal(b, &v)
			want = append(want, v)
		}
		var env struct {
			Error struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
				Status  string `json:"status"`
				Details []any  `json:"details"`
			} `json:"error"`
		}
		if err := json.Unmarshal(got, &env); err != nil {
			t.Fatal(err)
		}
		if env.Error.Code != 429 || env.Error.Status != "RESOURCE_EXHAUSTED" || env.Error.Message != st.Message() {
			t.Fatalf("envelope: %s", got)
		}
		if !reflect.DeepEqual(env.Error.Details, want) {
			t.Errorf("delay %v:\n got  %v\n want %v", d, env.Error.Details, want)
		}
	}
}
