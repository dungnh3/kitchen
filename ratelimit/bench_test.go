package ratelimit_test

import (
	"context"
	"net/netip"
	"testing"
	"time"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/ratelimit"
)

func BenchmarkCheckDeviceTier(b *testing.B) {
	l, _ := ratelimit.New(ratelimit.Config{Default: &ratelimit.Policy{Limits: ratelimit.DefaultLimits()}})
	ctx := xcontext.NewContext(context.Background(), xcontext.Info{DeviceID: "dev-1", ClientIP: netip.MustParseAddr("1.2.3.4")})
	b.ReportAllocs()
	for b.Loop() {
		_, _ = l.Check(ctx, "/s/M")
	}
}

func BenchmarkLocalStoreParallel(b *testing.B) {
	s := ratelimit.NewLocalStore(100000)
	r := ratelimit.Rule{Rate: 1000000, Per: time.Second}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = s.Allow(context.Background(), "k", r)
		}
	})
}
