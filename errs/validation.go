package errs

import (
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// FieldViolation describes one invalid request field.
type FieldViolation struct {
	Field       string // path such as "agent.name" or "items[0].id"
	Description string // client-visible explanation
	Reason      string // optional machine-readable rule, e.g. "string.min_len"
}

// BadRequest returns ErrInvalidArgument carrying a google.rpc.BadRequest with
// the given violations.
func BadRequest(vs ...FieldViolation) *Error {
	br := &errdetails.BadRequest{}
	for _, v := range vs {
		br.FieldViolations = append(br.FieldViolations, &errdetails.BadRequest_FieldViolation{
			Field:       v.Field,
			Description: v.Description,
			Reason:      v.Reason,
		})
	}
	return ErrInvalidArgument.With(Details(br))
}

// FromViolationsProto converts a buf.validate.Violations message (what
// protovalidate's ValidationError.ToProto returns) into BadRequest, without
// importing protovalidate:
//
//	if verr := new(protovalidate.ValidationError); errors.As(err, &verr) {
//		return errs.FromViolationsProto(verr.ToProto())
//	}
//
// Unknown shapes yield a BadRequest without violations.
func FromViolationsProto(m proto.Message) *Error {
	if m == nil {
		return BadRequest()
	}
	r := m.ProtoReflect()
	list, ok := listField(r, "violations")
	if !ok {
		return BadRequest()
	}
	vs := make([]FieldViolation, 0, list.Len())
	for i := range list.Len() {
		v := list.Get(i).Message()
		vs = append(vs, FieldViolation{
			Field:       fieldPath(v),
			Description: stringField(v, "message"),
			Reason:      stringField(v, "rule_id"),
		})
	}
	return BadRequest(vs...)
}

// fieldPath renders Violation.field (a buf.validate.FieldPath) as
// "a.b[0].c[\"k\"]", falling back to the deprecated string field_path.
func fieldPath(v protoreflect.Message) string {
	fd := v.Descriptor().Fields().ByName("field")
	if fd == nil || fd.Message() == nil || !v.Has(fd) {
		return stringField(v, "field_path")
	}
	elems, ok := listField(v.Get(fd).Message(), "elements")
	if !ok {
		return stringField(v, "field_path")
	}
	var b strings.Builder
	for i := range elems.Len() {
		e := elems.Get(i).Message()
		if i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(stringField(e, "field_name"))
		od := e.Descriptor().Oneofs().ByName("subscript")
		if od == nil {
			continue
		}
		sub := e.WhichOneof(od)
		if sub == nil {
			continue
		}
		val := e.Get(sub)
		switch sub.Name() {
		case "index", "uint_key":
			b.WriteString("[" + strconv.FormatUint(val.Uint(), 10) + "]")
		case "int_key":
			b.WriteString("[" + strconv.FormatInt(val.Int(), 10) + "]")
		case "bool_key":
			b.WriteString("[" + strconv.FormatBool(val.Bool()) + "]")
		case "string_key":
			b.WriteString("[" + strconv.Quote(val.String()) + "]")
		}
	}
	return b.String()
}

func listField(m protoreflect.Message, name protoreflect.Name) (protoreflect.List, bool) {
	fd := m.Descriptor().Fields().ByName(name)
	if fd == nil || !fd.IsList() || fd.Message() == nil {
		return nil, false
	}
	return m.Get(fd).List(), true
}

func stringField(m protoreflect.Message, name protoreflect.Name) string {
	fd := m.Descriptor().Fields().ByName(name)
	if fd == nil || fd.Kind() != protoreflect.StringKind || fd.IsList() {
		return ""
	}
	return m.Get(fd).String()
}
