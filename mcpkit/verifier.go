package mcpkit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	kauth "github.com/dungnh3/kitchen/auth"
	"github.com/dungnh3/kitchen/errs"
	"github.com/modelcontextprotocol/go-sdk/auth"
)

// principalExtra is the TokenInfo.Extra key holding the kitchen principal.
const principalExtra = "github.com/dungnh3/kitchen/auth.Principal"

// Verifier adapts a kitchen auth.Verifier (auth.NewJWKS, auth.NewStatic,
// auth.NewIntrospection, auth.All) for [Auth.Verifier].
//
// audience is this MCP server's canonical resource URL, e.g.
// "https://mcp.example.com/mcp". It is required and every token must list it
// in aud (RFC 8707), so a token issued for another API is rejected even when
// the kitchen verifier was configured without an Audience.
//
// Rejected tokens yield HTTP 401 with the body "invalid token"; why a token
// was rejected is never sent to the caller. An unavailable key set or
// introspection endpoint (auth.ErrAuthUnavailable) fails closed: behind a
// [Server] it yields the errs response for UNAVAILABLE (HTTP 503, rendered by
// the [WithErrors] handler, which also logs the cause). The SDK's
// auth.RequireBearerToken on its own can only answer 500 for it, still
// without the cause. Tool handlers read the caller with auth.FromContext.
func Verifier(v kauth.Verifier, audience string) (auth.TokenVerifier, error) {
	if v == nil {
		return nil, errors.New("mcpkit: nil verifier")
	}
	if _, err := metadataPath(audience); err != nil {
		return nil, fmt.Errorf("mcpkit: verifier audience: %w", err)
	}
	return func(ctx context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		p, err := v.Verify(ctx, token)
		switch {
		case errors.Is(err, kauth.ErrAuthUnavailable):
			if out, ok := ctx.Value(outcomeKey{}).(*authOutcome); ok {
				out.unavailable = err
			}
			return nil, &authFailure{msg: "authentication temporarily unavailable", errs: []error{err}}
		case err != nil:
			return nil, &authFailure{msg: auth.ErrInvalidToken.Error(), errs: []error{auth.ErrInvalidToken, err}}
		case p == nil || !slices.Contains(p.Audience, audience):
			return nil, &authFailure{msg: auth.ErrInvalidToken.Error(), errs: []error{
				auth.ErrInvalidToken, fmt.Errorf("token audience does not include %s", audience),
			}}
		}
		return &auth.TokenInfo{
			Scopes:     p.Scopes,
			Expiration: p.ExpiresAt,
			UserID:     p.Subject,
			Extra:      map[string]any{principalExtra: p},
		}, nil
	}, nil
}

// authFailure is a verifier error whose text is safe to send: the SDK writes
// err.Error() as the response body, so the cause stays reachable only
// through errors.Is/As.
type authFailure struct {
	msg  string
	errs []error
}

func (e *authFailure) Error() string   { return e.msg }
func (e *authFailure) Unwrap() []error { return e.errs }

// authOutcome carries, for one request, what the SDK middleware cannot
// express: the original ResponseWriter and an auth.ErrAuthUnavailable seen by
// the verifier.
type authOutcome struct {
	w           http.ResponseWriter
	unavailable error
}

type outcomeKey struct{}

// withAuthErrors wraps auth.RequireBearerToken (next) so that an
// auth.ErrAuthUnavailable reported by [Verifier] is written by eh, as
// UNAVAILABLE (503), instead of the SDK's hard-coded 500. The SDK decides
// before writing anything, so its response is replaced whole.
func withAuthErrors(eh *errs.Handler, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		out := &authOutcome{w: w}
		r = r.WithContext(context.WithValue(r.Context(), outcomeKey{}, out))
		next.ServeHTTP(&outcomeWriter{ResponseWriter: w, out: out, r: r, eh: eh}, r)
	})
}

// outcomeWriter is the writer the SDK auth middleware sees. Requests that
// pass authentication get the original writer back in [captureToken].
type outcomeWriter struct {
	http.ResponseWriter
	out      *authOutcome
	r        *http.Request
	eh       *errs.Handler
	replaced bool
}

func (w *outcomeWriter) WriteHeader(code int) {
	if w.replace() {
		return
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *outcomeWriter) Write(b []byte) (int, error) {
	if w.replace() {
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

func (w *outcomeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// replace writes the errs response on the first write after the verifier
// reported the authority unavailable, and swallows the SDK's own response.
func (w *outcomeWriter) replace() bool {
	if w.out.unavailable == nil {
		return false
	}
	if !w.replaced {
		w.replaced = true
		w.Header().Del("Content-Length")
		w.eh.WriteHTTP(w.ResponseWriter, w.r, w.out.unavailable)
	}
	return true
}

// originalWriter returns the writer withAuthErrors received, so handlers
// behind authentication (streaming included) never write through
// outcomeWriter.
func originalWriter(w http.ResponseWriter, r *http.Request) http.ResponseWriter {
	if out, ok := r.Context().Value(outcomeKey{}).(*authOutcome); ok {
		return out.w
	}
	return w
}

// withPrincipal makes the principal set by [Verifier] visible to
// auth.FromContext.
func withPrincipal(ctx context.Context) context.Context {
	ti := auth.TokenInfoFromContext(ctx)
	if ti == nil {
		return ctx
	}
	if p, ok := ti.Extra[principalExtra].(*kauth.Principal); ok {
		return kauth.NewContext(ctx, p)
	}
	return ctx
}
