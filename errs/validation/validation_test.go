package validation_test

import (
	"errors"
	"fmt"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/go/protovalidate"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/errs/validation"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestFromError(t *testing.T) {
	idx := uint64(0)
	verr := &protovalidate.ValidationError{Violations: []*protovalidate.Violation{{
		Proto: &validate.Violation{
			Field: &validate.FieldPath{Elements: []*validate.FieldPathElement{
				{FieldName: proto.String("items")},
				{FieldName: proto.String("id"), Subscript: &validate.FieldPathElement_Index{Index: idx}},
			}},
			RuleId:  proto.String("string.min_len"),
			Message: proto.String("value length must be at least 1 characters"),
		},
	}}}
	e, ok := validation.FromError(fmt.Errorf("validate: %w", verr))
	if !ok || !errors.Is(e, errs.ErrInvalidArgument) || !errors.Is(e, verr) {
		t.Fatalf("FromError = %v %v", e, ok)
	}
	var br *errdetails.BadRequest
	for _, d := range status.Convert(e).Details() {
		if m, ok := d.(*errdetails.BadRequest); ok {
			br = m
		}
	}
	fv := br.GetFieldViolations()
	if len(fv) != 1 || fv[0].GetField() != "items.id[0]" || fv[0].GetReason() != "string.min_len" {
		t.Fatalf("violations = %v", fv)
	}
	if _, ok := validation.FromError(errors.New("x")); ok {
		t.Fatal("non-validation error converted")
	}
}
