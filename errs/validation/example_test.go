package validation_test

import (
	"fmt"

	"buf.build/go/protovalidate"
	"github.com/dungnh3/kitchen/errs/validation"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// checkRequest checks req against its protovalidate rules and returns an
// INVALID_ARGUMENT errs error with a google.rpc.BadRequest when it fails.
// kitchen/server already does this for every request; call it yourself for
// messages that do not arrive through the server (queue payloads, files).
func checkRequest(req proto.Message) error {
	err := protovalidate.Validate(req)
	if err == nil {
		return nil
	}
	if e, ok := validation.FromError(err); ok {
		return e
	}
	return err // a CompilationError: the rules themselves are broken
}

func ExampleFromError() {
	fmt.Println(checkRequest(&emptypb.Empty{}))
	// Output: <nil>
}
