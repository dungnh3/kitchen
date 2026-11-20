package reqctx_test

import (
	"context"
	"fmt"
	"net/netip"

	"github.com/dungnh3/kitchen/reqctx"
)

func ExampleFromContext() {
	// A server stores Info once per request; handlers only read it.
	ctx := reqctx.NewContext(context.Background(), reqctx.Info{RequestID: "r-1", Platform: reqctx.PlatformIOS})
	ctx = reqctx.WithUserID(ctx, "user-42") // done by the auth interceptor

	info, _ := reqctx.FromContext(ctx)
	fmt.Println(info.RequestID, info.Platform, reqctx.UserID(ctx))
	// Output: r-1 ios user-42
}

func ExampleTrustedProxies_ClientIP() {
	lb, _ := reqctx.NewTrustedProxies("10.0.0.0/8")
	remote := reqctxAddr("10.1.2.3")
	fmt.Println(lb.ClientIP(remote, []string{"203.0.113.7, 10.9.9.9"}, ""))
	// Output: 203.0.113.7
}

func reqctxAddr(s string) netip.Addr { return netip.MustParseAddr(s) }
