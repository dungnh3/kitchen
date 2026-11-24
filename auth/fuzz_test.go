package auth_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/auth"
	"github.com/dungnh3/kitchen/httpx"
	"github.com/dungnh3/kitchen/log"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// isKnownRejection reports whether err is one of the client-safe token
// errors (never a panic, a 500 or a leaked cause).
func isKnownRejection(err error) bool {
	return errors.Is(err, auth.ErrTokenInvalid) || errors.Is(err, auth.ErrTokenExpired) ||
		errors.Is(err, auth.ErrTokenMissing) || errors.Is(err, auth.ErrAuthUnavailable)
}

// FuzzAuthenticate feeds arbitrary tokens through routing and JWT
// verification. Whatever is accepted must be a valid, unexpired token of the
// configured issuer and audience.
func FuzzAuthenticate(f *testing.F) {
	t := &testing.T{}
	valid := sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")
	for _, s := range []string{
		valid, "", "a.b.c", "....", "opaque",
		rawToken(map[string]any{"alg": "none"}, baseClaims(issuerA), ""),
		rawToken(map[string]any{"alg": "HS256", "kid": "k1", "crit": []string{"x"}}, baseClaims(issuerA), "AAAA"),
		`{"payload":"e30","signatures":[{"protected":"e30","signature":""}]}`,
		strings.Repeat("A", 9000),
	} {
		f.Add(s)
	}
	a, err := auth.New(auth.Config{
		Verifiers: []auth.Verifier{newHMAC(t, issuerA)}, Audience: audience, Logger: log.Discard(),
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, token string) {
		p, err := a.Authenticate(context.Background(), token)
		if err != nil {
			if !isKnownRejection(err) {
				t.Fatalf("unexpected error kind: %v", err)
			}
			if c := status.Code(err); c != codes.Unauthenticated && c != codes.Unavailable {
				t.Fatalf("code = %v", c)
			}
			return
		}
		if p.Issuer != issuerA || !p.ExpiresAt.After(time.Now().Add(-time.Minute)) ||
			!strings.Contains(strings.Join(p.Audience, " "), audience) {
			t.Fatalf("accepted a bad principal: %+v", p)
		}
	})
}

// FuzzAuthorizationHeader feeds arbitrary Authorization values through the
// HTTP path: only client-safe 401s come out.
func FuzzAuthorizationHeader(f *testing.F) {
	for _, s := range []string{"", "Bearer", "Bearer ", "bearer x.y.z", "Basic dXNlcjpwYXNz", "Bearer a b", "Bearer\tx", "BEARER  tok "} {
		f.Add(s)
	}
	t := &testing.T{}
	a, err := auth.New(auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA)}, Audience: audience, Logger: log.Discard()})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, h string) {
		r := httptest.NewRequest(http.MethodGet, "/x", nil)
		r.Header["Authorization"] = []string{h}
		_, err := a.AuthenticateHTTP(r, "GET /x")
		if err == nil || !isKnownRejection(err) {
			t.Fatalf("header %q: err = %v", h, err)
		}
	})
}

// FuzzJWKSBody feeds arbitrary key-set documents to NewJWKS: no panic, no
// leaked goroutine, and never a symmetric key that verifies.
func FuzzJWKSBody(f *testing.F) {
	for _, s := range []string{
		`{"keys":[]}`, `{}`, `[]`, `null`, `{"keys":[{"kty":"oct","k":"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY","kid":"k1"}]}`,
		`{"keys":[{"kty":"RSA","n":"AQAB","e":"AQAB","kid":"x"}]}`, `{"keys":[{"kty":"EC","crv":"P-256","x":"","y":""}]}`,
	} {
		f.Add([]byte(s))
	}
	t := &testing.T{}
	hsToken := sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")
	f.Fuzz(func(t *testing.T, body []byte) {
		rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
				Body: io.NopCloser(bytes.NewReader(body)), Request: r,
			}, nil
		})
		client, err := httpx.New(httpx.WithBaseURL(memURL), httpx.WithTransport(rt))
		if err != nil {
			t.Fatal(err)
		}
		v, err := auth.NewJWKS(context.Background(), auth.JWKSConfig{
			Issuer: issuerA, URL: memURL, Client: client, Logger: log.Discard(),
		})
		if err != nil {
			return
		}
		defer func() { _ = v.Close(context.Background()) }()
		if _, err := v.Verify(context.Background(), hsToken); err == nil {
			t.Fatal("an HS256 token verified against a JWKS")
		}
	})
}

// FuzzRFC7662Response feeds arbitrary introspection responses: the result
// is a principal, a token rejection or AUTH_UNAVAILABLE, and a principal
// only comes from an active response.
func FuzzRFC7662Response(f *testing.F) {
	for _, s := range []string{
		`{"active":true}`, `{"active":false}`, `{"active":"true"}`, `{"active":true,"exp":1}`,
		`{"active":true,"aud":["` + audience + `"]}`, `{"active":true,"aud":42}`, `not json`, ``,
	} {
		f.Add([]byte(s), 200)
	}
	f.Add([]byte(`{}`), 500)
	f.Fuzz(func(t *testing.T, body []byte, code int) {
		if code < 100 || code > 599 {
			return
		}
		rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: code, Header: http.Header{"Content-Type": {"application/json"}},
				Body: io.NopCloser(bytes.NewReader(body)), Request: r,
			}, nil
		})
		client, err := httpx.New(httpx.WithBaseURL("https://idp.example.com"), httpx.WithTransport(rt))
		if err != nil {
			t.Fatal(err)
		}
		fn := auth.RFC7662(auth.RFC7662Config{Endpoint: "https://idp.example.com/i", Client: client, Audience: audience})
		p, err := fn(context.Background(), "tok")
		switch {
		case err != nil:
			if !isKnownRejection(err) {
				t.Fatalf("unexpected error kind: %v", err)
			}
		case !bytes.Contains(body, []byte("active")) || !strings.Contains(strings.Join(p.Audience, " "), audience):
			t.Fatalf("principal from %q", body)
		}
	})
}

// Verify racing Close: every call either succeeds or fails with
// AUTH_UNAVAILABLE, none blocks, and no goroutine outlives Close (goleak in
// TestMain).
func TestJWKSVerifyRacesClose(t *testing.T) {
	k1, k2 := rsaKey(t, 2048), rsaKey(t, 2048)
	m := newMemJWKS(publicJWK(t, k1, "k1"))
	v := newMemVerifier(t, m, auth.JWKSConfig{MissRefresh: time.Nanosecond})
	m.add(publicJWK(t, k2, "k2"))
	toks := []string{
		sign(t, baseClaims(issuerA), jwa.RS256(), k1, "k1"),
		sign(t, baseClaims(issuerA), jwa.RS256(), k2, "k2"),
		sign(t, baseClaims(issuerA), jwa.RS256(), k2, "junk"),
	}
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			_, err := v.Verify(context.Background(), toks[i%len(toks)])
			if err != nil && !errors.Is(err, auth.ErrAuthUnavailable) && !errors.Is(err, auth.ErrTokenInvalid) {
				t.Errorf("err = %v", err)
			}
		})
		if i == 32 {
			wg.Go(func() { _ = v.Close(context.Background()) })
		}
	}
	wg.Wait()
}
