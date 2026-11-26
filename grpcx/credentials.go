package grpcx

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/dungnh3/kitchen/errs"
)

// TokenSource returns the bearer token sent with a call. It is called for
// every call, so cache the token (and refresh it before it expires) inside
// the source.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// TokenSourceFunc adapts a function to TokenSource.
type TokenSourceFunc func(ctx context.Context) (string, error)

// Token calls f.
func (f TokenSourceFunc) Token(ctx context.Context) (string, error) { return f(ctx) }

// StaticToken returns a TokenSource that always returns token, for a
// long-lived API key.
func StaticToken(token string) TokenSource {
	return TokenSourceFunc(func(context.Context) (string, error) { return token, nil })
}

// tokenCreds sends the token of a TokenSource on every call. gRPC refuses
// to send it over a connection without transport security.
type tokenCreds struct{ ts TokenSource }

func (c tokenCreds) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	tok, err := c.ts.Token(ctx)
	if err == nil && tok == "" {
		err = errors.New("grpcx: token source returned an empty token")
	}
	if err != nil {
		// A status error keeps its code (UNAUTHENTICATED) through gRPC,
		// which turns a plain error into UNAVAILABLE and would retry it.
		// The message stays in the StatusError cause; DefaultErrorMapper
		// returns INTERNAL to our own callers.
		return nil, errs.Wrap(err, errs.ErrUnauthenticated.With(errs.Message("grpcx: token source: "+err.Error())))
	}
	return map[string]string{"authorization": "Bearer " + tok}, nil
}

func (tokenCreds) RequireTransportSecurity() bool { return true }

// forwardAuthorization adds the incoming bearer token of ctx to its outgoing
// metadata. Nothing is forwarded when the caller set an outgoing
// authorization, or when the incoming one is not exactly one well-formed
// Bearer value.
func forwardAuthorization(ctx context.Context) context.Context {
	if out, _ := metadata.FromOutgoingContext(ctx); len(out.Get("authorization")) > 0 {
		return ctx
	}
	in, _ := metadata.FromIncomingContext(ctx)
	vals := in.Get("authorization")
	if len(vals) != 1 {
		return ctx
	}
	scheme, tok, ok := strings.Cut(vals[0], " ")
	if !ok || !strings.EqualFold(scheme, "bearer") || tok == "" || strings.ContainsAny(tok, " \t\r\n") {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
}

func forwardUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(forwardAuthorization(ctx), method, req, reply, cc, opts...)
}

func forwardStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return streamer(forwardAuthorization(ctx), desc, cc, method, opts...)
}
