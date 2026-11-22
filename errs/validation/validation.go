// Package validation converts protovalidate errors into errs errors. It is a
// separate package so that importing errs does not pull in protovalidate
// and CEL.
package validation

import (
	"errors"

	"buf.build/go/protovalidate"
	"github.com/dungnh3/kitchen/errs"
)

// FromError converts a *protovalidate.ValidationError in err's chain into an
// INVALID_ARGUMENT errs error with a google.rpc.BadRequest (one field
// violation per rule; the field path uses protovalidate.FieldPathString and
// the reason is the rule id). ok is false when err is not a validation
// error, for example a protovalidate.CompilationError, which is a server
// bug.
func FromError(err error) (e *errs.Error, ok bool) {
	var verr *protovalidate.ValidationError
	if !errors.As(err, &verr) {
		return nil, false
	}
	vs := make([]errs.FieldViolation, 0, len(verr.Violations))
	for _, v := range verr.Violations {
		vs = append(vs, errs.FieldViolation{
			Field:       protovalidate.FieldPathString(v.Proto.GetField()),
			Description: v.Proto.GetMessage(),
			Reason:      v.Proto.GetRuleId(),
		})
	}
	return errs.Wrap(err, errs.BadRequest(vs...)), true
}
