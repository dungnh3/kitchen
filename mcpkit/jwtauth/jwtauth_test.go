package jwtauth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const (
	issuer   = "https://idp.example.com"
	audience = "https://mcp.example.com/mcp"
)

type idp struct {
	t       *testing.T
	keys    map[string]*ecdsa.PrivateKey
	fetches atomic.Int32
	srv     *httptest.Server
}

func newIDP(t *testing.T, kids ...string) *idp {
	t.Helper()
	p := &idp{t: t, keys: map[string]*ecdsa.PrivateKey{}}
	for _, k := range kids {
		p.addKey(k)
	}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		p.fetches.Add(1)
		var set jose.JSONWebKeySet
		for kid, k := range p.keys {
			set.Keys = append(set.Keys, jose.JSONWebKey{Key: &k.PublicKey, KeyID: kid, Algorithm: string(jose.ES256), Use: "sig"})
		}
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *idp) addKey(kid string) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		p.t.Fatal(err)
	}
	p.keys[kid] = k
}

func (p *idp) sign(kid string, claims any) string {
	p.t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES256, Key: p.keys[kid]},
		(&jose.SignerOptions{}).WithType("at+jwt").WithHeader(jose.HeaderKey("kid"), kid))
	if err != nil {
		p.t.Fatal(err)
	}
	tok, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		p.t.Fatal(err)
	}
	return tok
}

func claims(now time.Time) map[string]any {
	return map[string]any{
		"iss":   issuer,
		"aud":   audience,
		"sub":   "user-1",
		"exp":   now.Add(time.Hour).Unix(),
		"iat":   now.Unix(),
		"scope": "mail.read mail.send",
	}
}

func newVerifier(t *testing.T, p *idp) *Verifier {
	t.Helper()
	v, err := New(Config{Issuer: issuer, Audience: audience, JWKSURL: p.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestVerify(t *testing.T) {
	now := time.Now()
	p := newIDP(t, "k1")
	other := newIDP(t, "k1") // same kid, different key

	tests := []struct {
		name    string
		token   func() string
		wantErr bool
	}{
		{"valid", func() string { return p.sign("k1", claims(now)) }, false},
		{"wrong audience", func() string { c := claims(now); c["aud"] = "https://other"; return p.sign("k1", c) }, true},
		{"wrong issuer", func() string { c := claims(now); c["iss"] = "https://evil"; return p.sign("k1", c) }, true},
		{"expired", func() string { c := claims(now); c["exp"] = now.Add(-time.Hour).Unix(); return p.sign("k1", c) }, true},
		{"missing exp", func() string { c := claims(now); delete(c, "exp"); return p.sign("k1", c) }, true},
		{"not yet valid", func() string { c := claims(now); c["nbf"] = now.Add(time.Hour).Unix(); return p.sign("k1", c) }, true},
		{"bad signature", func() string { return other.sign("k1", claims(now)) }, true},
		{"garbage", func() string { return "not.a.jwt" }, true},
	}
	v := newVerifier(t, p)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := v.Verify(t.Context(), tt.token(), nil)
			if tt.wantErr {
				if !errors.Is(err, auth.ErrInvalidToken) {
					t.Fatalf("err = %v, want ErrInvalidToken", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if info.UserID != "user-1" || !slices.Equal(info.Scopes, []string{"mail.read", "mail.send"}) || info.Expiration.IsZero() {
				t.Errorf("info = %+v", info)
			}
		})
	}
}

func TestRejectsHMACAlgorithm(t *testing.T) {
	if _, err := New(Config{Issuer: issuer, Audience: audience, Algorithms: []jose.SignatureAlgorithm{jose.HS256}}); err == nil {
		t.Fatal("New accepted HS256")
	}
}

func TestKeyRotationAndRateLimit(t *testing.T) {
	now := time.Now()
	p := newIDP(t, "k1")
	v := newVerifier(t, p)
	clock := now
	v.now = func() time.Time { return clock }

	if _, err := v.Verify(t.Context(), p.sign("k1", claims(now)), nil); err != nil {
		t.Fatal(err)
	}
	p.addKey("k2")
	// Unknown kid within MinRefreshInterval: no new fetch, rejected.
	if _, err := v.Verify(t.Context(), p.sign("k2", claims(now)), nil); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("err = %v, want ErrInvalidToken", err)
	}
	if got := p.fetches.Load(); got != 1 {
		t.Fatalf("fetches = %d, want 1", got)
	}
	// After MinRefreshInterval: refresh picks up the rotated key.
	clock = clock.Add(2 * time.Minute)
	if _, err := v.Verify(t.Context(), p.sign("k2", claims(clock)), nil); err != nil {
		t.Fatal(err)
	}
	if got := p.fetches.Load(); got != 2 {
		t.Fatalf("fetches = %d, want 2", got)
	}
}

func TestKeysUnavailable(t *testing.T) {
	p := newIDP(t, "k1")
	tok := p.sign("k1", claims(time.Now()))
	v, err := New(Config{Issuer: issuer, Audience: audience, JWKSURL: "http://127.0.0.1:1/jwks"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = v.Verify(t.Context(), tok, nil)
	if err == nil || errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("err = %v, want a non-401 error", err)
	}
}

func TestDiscoverJWKS(t *testing.T) {
	p := newIDP(t, "k1")
	var meta *httptest.Server
	meta = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/oauth-authorization-server" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                           meta.URL,
			"authorization_endpoint":           meta.URL + "/authorize",
			"token_endpoint":                   meta.URL + "/token",
			"jwks_uri":                         p.srv.URL,
			"response_types_supported":         []string{"code"},
			"code_challenge_methods_supported": []string{"S256"},
		})
	}))
	defer meta.Close()

	c := claims(time.Now())
	c["iss"] = meta.URL
	v, err := New(Config{Issuer: meta.URL, Audience: audience})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(t.Context(), p.sign("k1", c), nil); err != nil {
		t.Fatal(err)
	}
}
