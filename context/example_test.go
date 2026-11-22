package context_test

import (
	"context"
	"fmt"
	"net/netip"

	xcontext "github.com/dungnh3/kitchen/context"
)

func ExampleFromContext() {
	// A server stores Info once per request; handlers only read it.
	ctx := xcontext.NewContext(context.Background(), xcontext.Info{RequestID: "r-1", Platform: xcontext.PlatformIOS})
	ctx = xcontext.WithUserID(ctx, "user-42") // done by the auth interceptor

	info, _ := xcontext.FromContext(ctx)
	fmt.Println(info.RequestID, info.Platform, xcontext.UserID(ctx))
	// Output: r-1 ios user-42
}

func ExampleTrustedProxies_ClientIP() {
	lb, _ := xcontext.NewTrustedProxies("10.0.0.0/8")
	remote := addr("10.1.2.3")
	fmt.Println(lb.ClientIP(remote, []string{"203.0.113.7, 10.9.9.9"}))
	// Output: 203.0.113.7
}

func addr(s string) netip.Addr { return netip.MustParseAddr(s) }
