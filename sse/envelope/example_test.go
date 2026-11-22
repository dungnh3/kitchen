package envelope_test

import (
	"context"
	"fmt"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/envelope"
)

// ContextValidator skips envelopes that do not belong to the caller, so
// events never cross users or tenants even if a writer targets the wrong
// stream.
func ExampleContextValidator() {
	validator := envelope.ContextValidator(envelope.ValidatorConfig{
		Logger: log.Discard(), // pass the service logger; one Warn per rejection
		Resolve: func(ctx context.Context) envelope.Rule {
			return envelope.Rule{
				UserIDs:   []string{xcontext.UserID(ctx)},
				TenantIDs: []string{"acme"},
			}
		},
	})

	ctx := xcontext.WithUserID(context.Background(), "user_1")
	transform := validator(ctx) // once per connection
	for _, user := range []string{"user_1", "user_2"} {
		e, _ := envelope.NewBuilder("run_1", "chat", user).WithTenantID("acme").BuildText(sse.TypeToken, "hi")
		m, _ := e.Message()
		act, _ := transform(ctx, &m)
		fmt.Println(user, act == sse.ActionPass)
	}
	// Output:
	// user_1 true
	// user_2 false
}
