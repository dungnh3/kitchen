package server

import (
	"context"
	"net/http"

	"google.golang.org/grpc"
)

type drainKey struct{}

// Draining returns a channel that is closed when the server starts draining.
// Long-lived handlers (streams, SSE, long polls) should select on it, send a
// final message and return, so shutdown finishes before its budget:
//
//	for {
//		select {
//		case <-server.Draining(ctx):
//			return status.Error(codes.Unavailable, "server shutting down, reconnect")
//		case <-ctx.Done():
//			return ctx.Err()
//		case ev := <-events:
//			if err := stream.Send(ev); err != nil {
//				return err
//			}
//		}
//	}
//
// It returns nil (blocks forever) when ctx did not come from a Server.
func Draining(ctx context.Context) <-chan struct{} {
	ch, _ := ctx.Value(drainKey{}).(<-chan struct{})
	return ch
}

func withDrain(ctx context.Context, drain <-chan struct{}) context.Context {
	return context.WithValue(ctx, drainKey{}, drain)
}

func drainUnary(drain <-chan struct{}) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		return h(withDrain(ctx, drain), req)
	}
}

func drainStream(drain <-chan struct{}) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		return h(srv, &ctxStream{ServerStream: ss, ctx: withDrain(ss.Context(), drain)})
	}
}

func drainHTTP(drain <-chan struct{}, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(withDrain(r.Context(), drain)))
	})
}

// ctxStream replaces a ServerStream's context.
type ctxStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *ctxStream) Context() context.Context { return s.ctx }
