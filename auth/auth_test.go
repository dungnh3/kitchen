package auth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/auth"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/httpx"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"
	"go.uber.org/goleak"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const (
	issuerA  = "https://issuer-a.example.com"
	issuerB  = "https://issuer-b.example.com"
	audience = "api.example.com"
)

var hmacSecret = []byte("0123456789abcdef0123456789abcdef") // 32 bytes

type claims map[string]any

func baseClaims(iss string) claims {
	now := time.Now()
	return claims{
		"iss": iss, "sub": "user-1", "aud": audience,
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
		"scope": "agents:read agents:write",
	}
}

func sign(t *testing.T, c claims, alg jwa.SignatureAlgorithm, key any, kid string) string {
	t.Helper()
	tok := jwt.New()
	for k, v := range c {
		if err := tok.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	hdr := jws.NewHeaders()
	if kid != "" {
		_ = hdr.Set(jws.KeyIDKey, kid)
	}
	b, err := jwt.Sign(tok, jwt.WithKey(alg, key, jws.WithProtectedHeaders(hdr)))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// rawToken builds a JWT by hand so tests can forge headers.
func rawToken(header, payload map[string]any, sig string) string {
	enc := func(v any) string {
		b, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return enc(header) + "." + enc(payload) + "." + sig
}

func newHMAC(t *testing.T, iss string) *auth.Static {
	t.Helper()
	v, err := auth.NewStatic(auth.StaticConfig{
		Issuer: iss, Audience: audience,
		Keys: []auth.Key{{ID: "k1", Alg: jwa.HS256(), Material: hmacSecret}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func newRSA(t *testing.T) (*rsa.PrivateKey, *auth.Static) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	v, err := auth.NewStatic(auth.StaticConfig{
		Issuer: issuerB, Audience: audience,
		Keys: []auth.Key{{ID: "r1", Alg: jwa.RS256(), Material: &priv.PublicKey}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return priv, v
}

func newAuthenticator(t *testing.T, cfg auth.Config) *auth.Authenticator {
	t.Helper()
	a, err := auth.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestSecurity covers forged, unsigned and mis-keyed tokens and the usual
// JWT pitfalls. Every case must fail closed.
func TestSecurity(t *testing.T) {
	rsaPriv, rsaV := newRSA(t)
	a := newAuthenticator(t, auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA), rsaV}, Audience: audience})

	pubDER, _ := x509.MarshalPKIXPublicKey(&rsaPriv.PublicKey)
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})

	expired := baseClaims(issuerA)
	expired["exp"] = time.Now().Add(-time.Hour).Unix()
	future := baseClaims(issuerA)
	future["nbf"] = time.Now().Add(time.Hour).Unix()
	noExp := baseClaims(issuerA)
	delete(noExp, "exp")
	wrongAud := baseClaims(issuerA)
	wrongAud["aud"] = "other-api"

	valid := sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")
	parts := strings.Split(valid, ".")
	tampered := baseClaims(issuerA)
	tampered["sub"] = "admin"

	tests := []struct {
		name  string
		token string
		want  *errs.Error
	}{
		{"unsigned token (empty signature)", rawToken(map[string]any{"alg": "HS256", "kid": "k1"}, baseClaims(issuerA), ""), auth.ErrTokenInvalid},
		{"alg none", rawToken(map[string]any{"alg": "none"}, baseClaims(issuerA), ""), auth.ErrTokenInvalid},
		{"alg None mixed case", rawToken(map[string]any{"alg": "None", "kid": "k1"}, baseClaims(issuerA), ""), auth.ErrTokenInvalid},
		{"alg confusion: HS256 keyed with RSA public key", sign(t, baseClaims(issuerB), jwa.HS256(), pubPEM, "r1"), auth.ErrTokenInvalid},
		{"unknown issuer", sign(t, baseClaims("https://evil.example.com"), jwa.HS256(), hmacSecret, "k1"), auth.ErrTokenInvalid},
		{"missing issuer", sign(t, claims{"sub": "x", "aud": audience, "exp": time.Now().Add(time.Hour).Unix()}, jwa.HS256(), hmacSecret, "k1"), auth.ErrTokenInvalid},
		{"issuer A token signed with issuer B key", sign(t, baseClaims(issuerA), jwa.RS256(), rsaPriv, "r1"), auth.ErrTokenInvalid},
		{"wrong secret", sign(t, baseClaims(issuerA), jwa.HS256(), []byte("ffffffffffffffffffffffffffffffff"), "k1"), auth.ErrTokenInvalid},
		{"wrong audience", sign(t, wrongAud, jwa.HS256(), hmacSecret, "k1"), auth.ErrTokenInvalid},
		{"expired", sign(t, expired, jwa.HS256(), hmacSecret, "k1"), auth.ErrTokenExpired},
		{"not yet valid", sign(t, future, jwa.HS256(), hmacSecret, "k1"), auth.ErrTokenInvalid},
		{"no exp", sign(t, noExp, jwa.HS256(), hmacSecret, "k1"), auth.ErrTokenInvalid},
		{"unknown kid", sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k9"), auth.ErrTokenInvalid},
		{"alg not allowed for key (HS512)", sign(t, baseClaims(issuerA), jwa.HS512(), append(hmacSecret, hmacSecret...), "k1"), auth.ErrTokenInvalid},
		{"tampered payload", parts[0] + "." + rawToken(nil, tampered, "")[strings.Index(rawToken(nil, tampered, ""), ".")+1:strings.LastIndex(rawToken(nil, tampered, ""), ".")] + "." + parts[2], auth.ErrTokenInvalid},
		{"garbage", "a.b.c", auth.ErrTokenInvalid},
		{"opaque without opaque verifier", "opaque-token-123", auth.ErrTokenInvalid},
		{"empty", "", auth.ErrTokenMissing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := a.Authenticate(context.Background(), tt.token)
			if err == nil {
				t.Fatalf("accepted: %+v", p)
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want.Reason())
			}
			if status.Code(err) != tt.want.Code() {
				t.Fatalf("code = %v", status.Code(err))
			}
		})
	}

	// Sanity: the valid tokens pass.
	for _, tok := range []string{valid, sign(t, baseClaims(issuerB), jwa.RS256(), rsaPriv, "r1")} {
		p, err := a.Authenticate(context.Background(), tok)
		if err != nil || p.Subject != "user-1" || !p.HasScope("agents:write") {
			t.Fatalf("valid token rejected: %v %+v", err, p)
		}
	}
}

func TestStaticConfigValidation(t *testing.T) {
	ecPriv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	edPub, _, _ := ed25519.GenerateKey(rand.Reader)
	rsaPriv, _ := rsa.GenerateKey(rand.Reader, 2048)
	bad := []auth.StaticConfig{
		{Keys: []auth.Key{{Alg: jwa.HS256(), Material: hmacSecret}}}, // no issuer
		{Issuer: issuerA}, // no keys
		{Issuer: issuerA, Keys: []auth.Key{{Alg: jwa.HS256(), Material: []byte("short")}}},                                                        // weak secret
		{Issuer: issuerA, Keys: []auth.Key{{Alg: jwa.HS256(), Material: &rsaPriv.PublicKey}}},                                                     // HS with public key
		{Issuer: issuerA, Keys: []auth.Key{{Alg: jwa.RS256(), Material: hmacSecret}}},                                                             // RS with bytes
		{Issuer: issuerA, Keys: []auth.Key{{Alg: jwa.ES256(), Material: edPub}}},                                                                  // ES with ed25519
		{Issuer: issuerA, Keys: []auth.Key{{Alg: jwa.NoSignature(), Material: hmacSecret}}},                                                       // none
		{Issuer: issuerA, Keys: []auth.Key{{ID: "a", Alg: jwa.HS256(), Material: hmacSecret}, {ID: "a", Alg: jwa.HS256(), Material: hmacSecret}}}, // dup
	}
	for i, c := range bad {
		if _, err := auth.NewStatic(c); err == nil {
			t.Errorf("config %d: want error", i)
		}
	}
	good := auth.StaticConfig{Issuer: issuerA, Keys: []auth.Key{
		{ID: "ec", Alg: jwa.ES256(), Material: &ecPriv.PublicKey},
		{ID: "ed", Alg: jwa.EdDSA(), Material: edPub},
	}}
	if _, err := auth.NewStatic(good); err != nil {
		t.Fatal(err)
	}
}

func TestKeyRotationAndSingleKey(t *testing.T) {
	old, cur := hmacSecret, []byte("abcdefghijklmnopqrstuvwxyz012345")
	v, err := auth.NewStatic(auth.StaticConfig{Issuer: issuerA, Keys: []auth.Key{
		{ID: "old", Alg: jwa.HS256(), Material: old},
		{ID: "new", Alg: jwa.HS256(), Material: cur},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, kid := range []string{"old", "new"} {
		key := old
		if kid == "new" {
			key = cur
		}
		if _, err := v.Verify(context.Background(), sign(t, baseClaims(issuerA), jwa.HS256(), key, kid)); err != nil {
			t.Fatalf("kid %s: %v", kid, err)
		}
	}
	if _, err := v.Verify(context.Background(), sign(t, baseClaims(issuerA), jwa.HS256(), old, "")); err == nil {
		t.Fatal("no kid with several keys must fail")
	}
	single := newHMAC(t, issuerA)
	if _, err := single.Verify(context.Background(), sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "")); err != nil {
		t.Fatalf("single key without kid: %v", err)
	}
}

func TestLeeway(t *testing.T) {
	now := time.Now()
	v, _ := auth.NewStatic(auth.StaticConfig{
		Issuer: issuerA, Leeway: time.Minute, Now: func() time.Time { return now },
		Keys: []auth.Key{{Alg: jwa.HS256(), Material: hmacSecret}},
	})
	c := baseClaims(issuerA)
	c["exp"] = now.Add(-30 * time.Second).Unix()
	if _, err := v.Verify(context.Background(), sign(t, c, jwa.HS256(), hmacSecret, "")); err != nil {
		t.Fatalf("within leeway: %v", err)
	}
	c["exp"] = now.Add(-2 * time.Minute).Unix()
	if _, err := v.Verify(context.Background(), sign(t, c, jwa.HS256(), hmacSecret, "")); !errors.Is(err, auth.ErrTokenExpired) {
		t.Fatalf("beyond leeway: %v", err)
	}
}

func TestNewValidation(t *testing.T) {
	v := newHMAC(t, issuerA)
	bad := []auth.Config{
		{},
		{Verifiers: []auth.Verifier{v, newHMAC(t, issuerA)}},
		{Verifiers: []auth.Verifier{v}, Public: []string{"/"}},
		{Verifiers: []auth.Verifier{v}, Public: []string{""}},
		{Verifiers: []auth.Verifier{nil}},
		{Verifiers: []auth.Verifier{v}},                                        // no audience
		{Verifiers: []auth.Verifier{v}, Audience: audience, AnyAudience: true}, // both
	}
	for i, c := range bad {
		if _, err := auth.New(c); err == nil {
			t.Errorf("config %d: want error", i)
		}
	}
}

func TestBearerParsing(t *testing.T) {
	a := newAuthenticator(t, auth.Config{Audience: audience, Verifiers: []auth.Verifier{newHMAC(t, issuerA)}})
	tok := sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")
	ic := a.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/agent.v1.AgentService/Get"}
	call := func(md metadata.MD) error {
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, err := ic(ctx, nil, info, func(ctx context.Context, _ any) (any, error) {
			if _, ok := auth.FromContext(ctx); !ok {
				t.Error("principal missing in handler context")
			}
			return nil, nil
		})
		return err
	}
	tests := []struct {
		name string
		md   metadata.MD
		code codes.Code
	}{
		{"bearer", metadata.Pairs("authorization", "Bearer "+tok), codes.OK},
		{"lowercase scheme", metadata.Pairs("authorization", "bearer "+tok), codes.OK},
		{"no header", metadata.MD{}, codes.Unauthenticated},
		{"basic scheme", metadata.Pairs("authorization", "Basic "+tok), codes.Unauthenticated},
		{"no scheme", metadata.Pairs("authorization", tok), codes.Unauthenticated},
		{"two values", metadata.Pairs("authorization", "Bearer "+tok, "authorization", "Bearer "+tok), codes.Unauthenticated},
		{"empty token", metadata.Pairs("authorization", "Bearer "), codes.Unauthenticated},
	}
	for _, tt := range tests {
		if got := status.Code(call(tt.md)); got != tt.code {
			t.Errorf("%s: code %v, want %v", tt.name, got, tt.code)
		}
	}
	err := call(metadata.MD{})
	if info, ok := errorInfo(err); !ok || info.GetReason() != "TOKEN_MISSING" || info.GetDomain() != "" {
		t.Fatalf("info = %v", info)
	}
	if strings.Contains(status.Convert(call(metadata.Pairs("authorization", "Basic x"))).Message(), "scheme") {
		t.Fatal("rejection reason leaked to client")
	}
}

func TestPublicMethods(t *testing.T) {
	a := newAuthenticator(t, auth.Config{
		Audience:  audience,
		Verifiers: []auth.Verifier{newHMAC(t, issuerA)},
		Public:    []string{"/grpc.health.v1.Health/", "/agent.v1.AgentService/Ping"},
	})
	for m, want := range map[string]bool{
		"/grpc.health.v1.Health/Check":         true,
		"/agent.v1.AgentService/Ping":          true,
		"/agent.v1.AgentService/PingAll":       false,
		"/agent.v1.InternalAgentService/Purge": false, // no name-based bypass
		"/grpc.health.v1.HealthX/Check":        false,
	} {
		if got := a.IsPublic(m); got != want {
			t.Errorf("IsPublic(%s) = %v", m, got)
		}
	}
	ic := a.UnaryServerInterceptor()
	_, err := ic(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"},
		func(context.Context, any) (any, error) { return "ok", nil })
	if err != nil {
		t.Fatal(err)
	}
}

func TestRequireScopes(t *testing.T) {
	a := newAuthenticator(t, auth.Config{
		Audience:  audience,
		Verifiers: []auth.Verifier{newHMAC(t, issuerA)},
		Authorizer: auth.RequireScopes(map[string][]string{
			"/agent.v1.AgentService/Delete": {"agents:admin"},
			"/agent.v1.AgentService/Update": {"agents:write"},
		}),
	})
	ic := a.UnaryServerInterceptor()
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")))
	h := func(context.Context, any) (any, error) { return nil, nil }
	if _, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/agent.v1.AgentService/Update"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/agent.v1.AgentService/List"}, h); err != nil {
		t.Fatal("unlisted methods need authentication only")
	}
	_, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/agent.v1.AgentService/Delete"}, h)
	if status.Code(err) != codes.PermissionDenied || !errors.Is(err, auth.ErrInsufficientScope) {
		t.Fatalf("err = %v", err)
	}
	if info, _ := errorInfo(err); info.GetMetadata()["missing_scope"] != "agents:admin" {
		t.Fatalf("metadata = %v", info.GetMetadata())
	}
}

type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f fakeStream) Context() context.Context { return f.ctx }

func TestStreamInterceptor(t *testing.T) {
	a := newAuthenticator(t, auth.Config{Audience: audience, Verifiers: []auth.Verifier{newHMAC(t, issuerA)}})
	ic := a.StreamServerInterceptor()
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")))
	err := ic(nil, fakeStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: "/s/Stream"}, func(_ any, ss grpc.ServerStream) error {
		if sub, ok := auth.SubjectFromContext(ss.Context()); !ok || sub != "user-1" {
			t.Errorf("subject = %q %v", sub, ok)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = ic(nil, fakeStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/s/Stream"},
		func(any, grpc.ServerStream) error { return nil })
	if status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
}

func TestHTTPMiddleware(t *testing.T) {
	a := newAuthenticator(t, auth.Config{Audience: audience, Verifiers: []auth.Verifier{newHMAC(t, issuerA)}, Public: []string{"GET /healthz"}})
	mux := http.NewServeMux()
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, has := auth.FromContext(r.Context()); !has && r.Pattern != "GET /healthz" {
			t.Error("principal missing")
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("GET /v1/agents", a.Middleware(ok))
	mux.Handle("GET /healthz", a.Middleware(ok))
	do := func(path, authz string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		if authz != "" {
			req.Header.Set("Authorization", authz)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	if rec := do("/v1/agents", ""); rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") != "Bearer" ||
		!strings.Contains(rec.Body.String(), `"reason":"TOKEN_MISSING"`) {
		t.Fatalf("missing token: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do("/v1/agents", "Bearer "+sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")); rec.Code != http.StatusNoContent {
		t.Fatalf("valid: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do("/healthz", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("public: %d", rec.Code)
	}
}

// --- JWKS -----------------------------------------------------------------

type jwksServer struct {
	*httptest.Server
	mu   sync.Mutex
	set  jwk.Set
	hits atomic.Int64
	fail atomic.Bool
}

func newJWKSServer(t *testing.T, keys ...jwk.Key) *jwksServer {
	t.Helper()
	s := &jwksServer{set: jwk.NewSet()}
	for _, k := range keys {
		_ = s.set.AddKey(k)
	}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		s.hits.Add(1)
		if s.fail.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.set)
	}))
	t.Cleanup(s.Close)
	return s
}

func publicJWK(t *testing.T, priv *rsa.PrivateKey, kid string) jwk.Key {
	t.Helper()
	k, err := jwk.Import(&priv.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	_ = k.Set(jwk.KeyIDKey, kid)
	_ = k.Set(jwk.AlgorithmKey, jwa.RS256())
	_ = k.Set(jwk.KeyUsageKey, "sig")
	return k
}

func TestJWKS(t *testing.T) {
	k1, _ := rsa.GenerateKey(rand.Reader, 2048)
	k2, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv := newJWKSServer(t, publicJWK(t, k1, "k1"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err := auth.NewJWKS(ctx, auth.JWKSConfig{Issuer: issuerA, Audience: audience, URL: srv.URL, MissRefresh: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close(context.Background()) }()

	if _, err := v.Verify(ctx, sign(t, baseClaims(issuerA), jwa.RS256(), k1, "k1")); err != nil {
		t.Fatalf("k1: %v", err)
	}

	// Rotation: the IdP adds k2; an unknown kid triggers a refresh.
	srv.mu.Lock()
	_ = srv.set.AddKey(publicJWK(t, k2, "k2"))
	srv.mu.Unlock()
	if _, err := v.Verify(ctx, sign(t, baseClaims(issuerA), jwa.RS256(), k2, "k2")); err != nil {
		t.Fatalf("rotated k2: %v", err)
	}

	// HS256 keyed with the public key's JWK bytes must never verify.
	pubJSON, _ := json.Marshal(publicJWK(t, k1, "k1"))
	if _, err := v.Verify(ctx, sign(t, baseClaims(issuerA), jwa.HS256(), pubJSON, "k1")); !errors.Is(err, auth.ErrTokenInvalid) {
		t.Fatalf("alg confusion: %v", err)
	}
	// alg none.
	if _, err := v.Verify(ctx, rawToken(map[string]any{"alg": "none", "kid": "k1"}, baseClaims(issuerA), "")); err == nil {
		t.Fatal("alg none accepted")
	}
	// Unknown kid stays invalid and refreshes are rate-limited.
	hits := srv.hits.Load()
	v2, err := auth.NewJWKS(ctx, auth.JWKSConfig{Issuer: issuerA, URL: srv.URL, MissRefresh: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v2.Close(context.Background()) }()
	hits = srv.hits.Load() - hits // initial fetch of v2
	for range 20 {
		_, _ = v2.Verify(ctx, sign(t, baseClaims(issuerA), jwa.RS256(), k1, "nope"))
	}
	if extra := srv.hits.Load() - hits; extra > 3 {
		t.Fatalf("unknown kids caused %d fetches", extra)
	}
}

func TestJWKSConfig(t *testing.T) {
	ctx := context.Background()
	if _, err := auth.NewJWKS(ctx, auth.JWKSConfig{Issuer: issuerA}); err == nil {
		t.Error("URL required")
	}
	if _, err := auth.NewJWKS(ctx, auth.JWKSConfig{Issuer: issuerA, URL: "http://x", Algs: []jwa.SignatureAlgorithm{jwa.HS256()}}); err == nil {
		t.Error("HS256 must be rejected for JWKS")
	}
	srv := newJWKSServer(t)
	srv.fail.Store(true)
	cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if v, err := auth.NewJWKS(cctx, auth.JWKSConfig{Issuer: issuerA, URL: srv.URL}); err == nil {
		_ = v.Close(ctx)
		t.Error("unreachable JWKS at start must fail")
	}
}

// --- Introspection ----------------------------------------------------------

func TestIntrospectionCache(t *testing.T) {
	var calls atomic.Int64
	now := time.Now()
	clock := func() time.Time { return now }
	in, err := auth.NewIntrospection(auth.IntrospectionConfig{
		TTL: 30 * time.Second, NegativeTTL: 5 * time.Second, Now: clock,
		Introspect: func(_ context.Context, token string) (*auth.Principal, error) {
			calls.Add(1)
			switch token {
			case "good":
				return &auth.Principal{Subject: "u1", ExpiresAt: now.Add(10 * time.Second)}, nil
			case "down":
				return nil, errors.New("connection refused")
			default:
				return nil, auth.ErrTokenInvalid
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if p, err := in.Verify(ctx, "good"); err != nil || p.Subject != "u1" {
				t.Errorf("good: %v", err)
			}
		})
	}
	wg.Wait()
	if c := calls.Load(); c != 1 {
		t.Fatalf("singleflight/cache: %d calls", c)
	}
	// Cached until the token expiry (10s), which is shorter than TTL.
	now = now.Add(9 * time.Second)
	_, _ = in.Verify(ctx, "good")
	if calls.Load() != 1 {
		t.Fatal("should still be cached")
	}
	now = now.Add(2 * time.Second)
	_, _ = in.Verify(ctx, "good")
	if calls.Load() != 2 {
		t.Fatal("must re-introspect after the token expiry")
	}

	// Negative caching.
	calls.Store(0)
	for range 3 {
		if _, err := in.Verify(ctx, "revoked"); !errors.Is(err, auth.ErrTokenInvalid) {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("negative cache: %d calls", calls.Load())
	}

	// Authority failures fail closed and are not cached.
	calls.Store(0)
	for range 2 {
		_, err := in.Verify(ctx, "down")
		if !errors.Is(err, auth.ErrAuthUnavailable) || status.Code(err) != codes.Unavailable {
			t.Fatalf("down: %v", err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("failures must not be cached")
	}
}

func TestIntrospectionCallerCancel(t *testing.T) {
	release := make(chan struct{})
	in, _ := auth.NewIntrospection(auth.IntrospectionConfig{
		Introspect: func(ctx context.Context, _ string) (*auth.Principal, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return &auth.Principal{Subject: "u"}, nil
		},
	})
	defer in.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := in.Verify(ctx, "t"); !errors.Is(err, auth.ErrAuthUnavailable) {
		t.Fatalf("canceled caller: %v", err)
	}
	close(release)
	// The shared call still completes and serves the next caller.
	if p, err := in.Verify(context.Background(), "t"); err != nil || p.Subject != "u" {
		t.Fatalf("after: %v", err)
	}
}

func TestRFC7662(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		if user != "rs" || pass != "secret" || r.Method != http.MethodPost {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = r.ParseForm()
		w.Header().Set("Content-Type", "application/json")
		switch r.PostForm.Get("token") {
		case "good":
			_, _ = w.Write([]byte(`{"active":true,"sub":"u1","iss":"` + issuerA + `","aud":["api.example.com"],"scope":"a b","exp":` +
				jsonInt(time.Now().Add(time.Hour).Unix()) + `}`))
		case "expired":
			_, _ = w.Write([]byte(`{"active":true,"sub":"u1","iss":"` + issuerA + `","exp":1}`))
		case "other-iss":
			_, _ = w.Write([]byte(`{"active":true,"sub":"u1","iss":"https://evil"}`))
		default:
			_, _ = w.Write([]byte(`{"active":false}`))
		}
	}))
	defer srv.Close()

	fn := auth.RFC7662(auth.RFC7662Config{Endpoint: srv.URL, ClientID: "rs", ClientSecret: "secret", Issuer: issuerA})
	in, _ := auth.NewIntrospection(auth.IntrospectionConfig{Introspect: fn})
	defer in.Close()
	a := newAuthenticator(t, auth.Config{Opaque: in, Audience: audience})
	ctx := context.Background()

	p, err := a.Authenticate(ctx, "good")
	if err != nil || p.Subject != "u1" || !p.HasScope("b") {
		t.Fatalf("good: %v %+v", err, p)
	}
	for tok, want := range map[string]*errs.Error{"expired": auth.ErrTokenExpired, "other-iss": auth.ErrTokenInvalid, "revoked": auth.ErrTokenInvalid} {
		if _, err := a.Authenticate(ctx, tok); !errors.Is(err, want) {
			t.Errorf("%s: %v", tok, err)
		}
	}
	bad := auth.RFC7662(auth.RFC7662Config{Endpoint: srv.URL, ClientID: "rs", ClientSecret: "wrong"})
	if _, err := bad(ctx, "good"); err == nil || errors.Is(err, auth.ErrTokenInvalid) {
		t.Fatalf("endpoint error must not look like an inactive token: %v", err)
	}
}

func jsonInt(n int64) string { b, _ := json.Marshal(n); return string(b) }

func TestAllVerifier(t *testing.T) {
	revoked := map[string]bool{}
	now := time.Now()
	in, _ := auth.NewIntrospection(auth.IntrospectionConfig{
		Issuer: issuerA, Now: func() time.Time { return now },
		Introspect: func(_ context.Context, tok string) (*auth.Principal, error) {
			if revoked[tok] {
				return nil, auth.ErrTokenInvalid
			}
			return &auth.Principal{Subject: "ignored"}, nil
		},
	})
	defer in.Close()
	a := newAuthenticator(t, auth.Config{Audience: audience, Verifiers: []auth.Verifier{auth.All(newHMAC(t, issuerA), in)}})
	tok := sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")
	p, err := a.Authenticate(context.Background(), tok)
	if err != nil || p.Subject != "user-1" {
		t.Fatalf("first verifier's principal expected: %v %+v", err, p)
	}
	revoked[tok] = true
	now = now.Add(time.Minute) // past the cache TTL
	if _, err := a.Authenticate(context.Background(), tok); !errors.Is(err, auth.ErrTokenInvalid) {
		t.Fatalf("revoked: %v", err)
	}
}

// errorInfo extracts the google.rpc.ErrorInfo from a status error.
func errorInfo(err error) (*errdetails.ErrorInfo, bool) {
	for _, d := range status.Convert(err).Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info, true
		}
	}
	return nil, false
}

func TestJWKSFetchesThroughConfiguredClient(t *testing.T) {
	k1, _ := rsa.GenerateKey(rand.Reader, 2048)
	var tagged atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Kitchen-Test") == "jwks" {
			tagged.Add(1)
		}
		set := jwk.NewSet()
		_ = set.AddKey(publicJWK(t, k1, "k1"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(set)
	}))
	defer srv.Close()
	client, err := httpx.New(httpx.WithBaseURL(srv.URL), httpx.WithHeader("X-Kitchen-Test", "jwks"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err := auth.NewJWKS(ctx, auth.JWKSConfig{Issuer: issuerA, URL: srv.URL, Client: client})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close(context.Background()) }()
	if _, err := v.Verify(ctx, sign(t, baseClaims(issuerA), jwa.RS256(), k1, "k1")); err != nil {
		t.Fatal(err)
	}
	if tagged.Load() == 0 {
		t.Fatal("JWKS was not fetched through the configured httpx client")
	}
}
