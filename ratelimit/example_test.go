package ratelimit_test

import (
	"context"
	"fmt"
	"time"

	"github.com/dungnh3/kitchen/ratelimit"
)

func Example() {
	l, err := ratelimit.New(ratelimit.Config{
		// One shared bucket per key for every method without its own policy.
		Default: &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 300, Per: time.Second, Burst: 600}},
		Policies: map[string]ratelimit.Policy{
			"/agent.v1.AgentService/Chat": {Rule: ratelimit.Rule{Rate: 2, Per: time.Second}},
		},
		// Subject: auth.SubjectFromContext, // limit per user once authenticated
	})
	if err != nil {
		panic(err)
	}
	// grpc.NewServer(grpc.ChainUnaryInterceptor(authn.UnaryServerInterceptor(), l.UnaryServerInterceptor()))
	for range 3 {
		d, _ := l.Allow(context.Background(), "/agent.v1.AgentService/Chat", "sub:u1")
		fmt.Println(d.Allowed, d.Remaining)
	}
	// Output:
	// true 1
	// true 0
	// false 0
}
