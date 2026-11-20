package reqctx

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// AppendToOutgoing adds the propagated request values (request id, user id
// and client info) to the outgoing gRPC metadata of ctx. Only this explicit
// allowlist is sent; incoming metadata such as authorization is never
// copied.
func AppendToOutgoing(ctx context.Context) context.Context {
	kv := make([]string, 0, 2*(len(propagated)+1))
	if id := UserID(ctx); id != "" {
		kv = append(kv, lower(HeaderUserID), id)
	}
	if p, ok := ctx.Value(infoKey{}).(*Info); ok {
		for _, f := range propagated {
			if v := f.get(p); v != "" {
				kv = append(kv, f.key, v)
			}
		}
	}
	if len(kv) == 0 {
		return ctx
	}
	out, _ := metadata.FromOutgoingContext(ctx)
	for i := 0; i < len(kv); i += 2 {
		if len(out.Get(kv[i])) > 0 {
			// An explicit value set by the caller wins.
			kv[i], kv[i+1] = "", ""
		}
	}
	pairs := kv[:0]
	for i := 0; i < len(kv); i += 2 {
		if kv[i] != "" {
			pairs = append(pairs, kv[i], kv[i+1])
		}
	}
	if len(pairs) == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

// UnaryClientInterceptor propagates request values with AppendToOutgoing.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(AppendToOutgoing(ctx), method, req, reply, cc, opts...)
	}
}

// StreamClientInterceptor propagates request values with AppendToOutgoing.
func StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(AppendToOutgoing(ctx), desc, cc, method, opts...)
	}
}
