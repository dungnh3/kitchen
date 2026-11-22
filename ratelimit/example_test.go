package ratelimit_test

import (
	"context"
	"fmt"
	"time"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/ratelimit"
)

func Example() {
	l, err := ratelimit.New(ratelimit.Config{
		// Users, devices and anonymous subjects 20/s each, unauthenticated
		// IPs 300/s, buckets shared by every method without its own policy.
		Default: &ratelimit.Policy{Limits: ratelimit.DefaultLimits()},
		Policies: map[string]ratelimit.Policy{
			// 2/s per caller, and at most 100 per day per user.
			"/agent.v1.AgentService/Chat": {Limits: []ratelimit.Limit{
				{Rule: ratelimit.Rule{Rate: 2, Per: time.Second}, By: ratelimit.ByCaller},
				{Rule: ratelimit.Rule{Rate: 100, Per: 24 * time.Hour}, By: ratelimit.ByUser},
			}},
		},
		Exempt:    []string{"ip:10.0.0.0/8"},                                               // internal traffic
		Overrides: map[string]ratelimit.Rule{"user:partner": {Rate: 50, Per: time.Second}}, // a bigger caller
		Skip:      []string{"/agent.v1.AgentService/Ping"},
	})
	if err != nil {
		panic(err)
	}
	// With package server: server.WithRateLimit(l). The user id comes from
	// package auth through xcontext.UserID.
	ctx := xcontext.WithUserID(context.Background(), "u1")
	for range 3 {
		d, _ := l.Check(ctx, "/agent.v1.AgentService/Chat")
		fmt.Println(d.Allowed, d.Remaining)
	}
	// Output:
	// true 1
	// true 0
	// false 0
}
