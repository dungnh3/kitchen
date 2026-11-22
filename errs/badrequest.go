package errs

import "google.golang.org/genproto/googleapis/rpc/errdetails"

// FieldViolation describes one invalid request field.
type FieldViolation struct {
	Field       string // path such as "agent.name" or "items[0].id"
	Description string // client-visible explanation
	Reason      string // optional machine-readable rule, e.g. "string.min_len"
}

// BadRequest returns an INVALID_ARGUMENT error carrying a google.rpc.BadRequest
// with the given violations. For protovalidate errors use package
// errs/validation.
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
