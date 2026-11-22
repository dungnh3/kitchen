package auth_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/log"

	"github.com/alicebob/miniredis/v2"
	"github.com/dungnh3/kitchen/auth"
	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/httpx"
	"github.com/dungnh3/kitchen/server"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// signTyp signs c with HS256/k1 and the given typ header.
func signTyp(t *testing.T, c claims, typ string) string {
	t.Helper()
	tok := jwt.New()
	for k, v := range c {
		_ = tok.Set(k, v)
	}
	hdr := jws.NewHeaders()
	_ = hdr.Set(jws.KeyIDKey, "k1")
	if typ != "" {
		_ = hdr.Set(jws.TypeKey, typ)
	}
	b, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256(), hmacSecret, jws.WithProtectedHeaders(hdr)))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestTypes(t *testing.T) {
	v, err := auth.NewStatic(auth.StaticConfig{
		Issuer: issuerA, Audience: audience, Types: []string{"at+jwt"},
		Keys: []auth.Key{{ID: "k1", Alg: jwa.HS256(), Material: hmacSecret}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for typ, ok := range map[string]bool{"at+jwt": true, "AT+JWT": true, "application/at+jwt": true, "JWT": false, "": false} {
		_, err := v.Verify(context.Background(), signTyp(t, baseClaims(issuerA), typ))
		if (err == nil) != ok {
			t.Errorf("typ %q: err = %v", typ, err)
		}
		if err != nil && !errors.Is(err, auth.ErrTokenInvalid) {
			t.Errorf("typ %q: want TOKEN_INVALID, got %v", typ, err)
		}
	}
	// No Types: any typ is accepted.
	if _, err := newHMAC(t, issuerA).Verify(context.Background(), signTyp(t, baseClaims(issuerA), "JWT")); err != nil {
		t.Fatal(err)
	}
}

func TestUserIDAndAuthorizerErrors(t *testing.T) {
	a := newAuthenticator(t, auth.Config{
		Verifiers: []auth.Verifier{newHMAC(t, issuerA)},
		Authorizer: auth.AuthorizerFunc(func(_ context.Context, method string, _ *auth.Principal) error {
			if method == "/x.Svc/Boom" {
				return errors.New("policy engine said no")
			}
			return nil
		}),
	})
	ic := a.UnaryServerInterceptor()
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")))
	var uid string
	_, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/x.Svc/Get"}, func(ctx context.Context, _ any) (any, error) {
		uid = xcontext.UserID(ctx)
		return nil, nil
	})
	if err != nil || uid == "" {
		t.Fatalf("user id %q, err %v", uid, err)
	}
	_, err = ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/x.Svc/Boom"}, func(context.Context, any) (any, error) { return nil, nil })
	if status.Code(err) != codes.PermissionDenied || !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("authorizer error must be 403 FORBIDDEN, got %v", err)
	}
	if status.Convert(err).Message() != "permission denied" {
		t.Fatalf("cause leaked: %q", status.Convert(err).Message())
	}
}

func TestWWWAuthenticate(t *testing.T) {
	a := newAuthenticator(t, auth.Config{
		Verifiers:  []auth.Verifier{newHMAC(t, issuerA)},
		Authorizer: auth.RequireScopes(map[string][]string{"GET /admin": {"admin"}}),
	})
	mux := http.NewServeMux()
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.Handle("GET /admin", a.Middleware(ok))
	do := func(authz string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin", nil)
		if authz != "" {
			req.Header.Set("Authorization", authz)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	for _, tt := range []struct {
		authz string
		code  int
		www   string
	}{
		{"", 401, "Bearer"},
		{"Bearer garbage.token.here", 401, `Bearer error="invalid_token"`},
		{"Bearer " + sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1"), 403, `Bearer error="insufficient_scope", scope="admin"`},
	} {
		rec := do(tt.authz)
		if rec.Code != tt.code || rec.Header().Get("WWW-Authenticate") != tt.www {
			t.Errorf("%q: %d %q %s", tt.authz, rec.Code, rec.Header().Get("WWW-Authenticate"), rec.Body.String())
		}
	}
}

// *auth.Authenticator plugs into server.WithAuth.
var _ server.Authenticator = (*auth.Authenticator)(nil)

// Two instances sharing Redis (kitchen/cache L2) ask the authority once.
func TestIntrospectionSharedRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	var calls atomic.Int64
	fn := func(context.Context, string) (*auth.Principal, error) {
		calls.Add(1)
		return &auth.Principal{Subject: "u1", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}
	a, err := auth.NewIntrospection(auth.IntrospectionConfig{Introspect: fn, Redis: rdb, CacheKey: cacheKey, Prefix: "auth:introspection:"})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, _ := auth.NewIntrospection(auth.IntrospectionConfig{Introspect: fn, Redis: rdb, CacheKey: cacheKey, Prefix: "auth:introspection:"})
	defer b.Close()
	for _, in := range []*auth.Introspection{a, b} {
		if p, err := in.Verify(context.Background(), "opaque-token"); err != nil || p.Subject != "u1" {
			t.Fatalf("%v %+v", err, p)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("authority called %d times, want 1", calls.Load())
	}
	for _, k := range mr.Keys() {
		if strings.Contains(k, "opaque-token") {
			t.Fatalf("raw token in a Redis key: %s", k)
		}
	}
}

var cacheKey = []byte("0123456789abcdef0123456789abcdef")

func TestIntrospectionRedisRequiresCacheKey(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	fn := func(context.Context, string) (*auth.Principal, error) { return nil, auth.ErrTokenInvalid }
	for _, k := range [][]byte{nil, []byte("short")} {
		if _, err := auth.NewIntrospection(auth.IntrospectionConfig{Introspect: fn, Redis: rdb, CacheKey: k, Prefix: "p:"}); err == nil {
			t.Fatalf("CacheKey %q accepted with Redis", k)
		}
	}
	// Verifiers sharing one Redis must not share answers: Prefix is required.
	if _, err := auth.NewIntrospection(auth.IntrospectionConfig{Introspect: fn, Redis: rdb, CacheKey: cacheKey}); err == nil {
		t.Fatal("Redis without Prefix accepted")
	}
}

// Whoever can write to Redis must not be able to authenticate a token: a
// forged entry, or a genuine entry moved to another token's key, is ignored
// and the authority decides.
func TestIntrospectionRedisTampering(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	fn := func(_ context.Context, tok string) (*auth.Principal, error) {
		if tok == "victim-token" {
			return &auth.Principal{Subject: "victim", ExpiresAt: time.Now().Add(time.Hour)}, nil
		}
		return nil, auth.ErrTokenInvalid
	}
	newVerifier := func() *auth.Introspection {
		in, err := auth.NewIntrospection(auth.IntrospectionConfig{
			Introspect: fn, Redis: rdb, CacheKey: cacheKey,
			Prefix: "auth:introspection:", Logger: log.Discard(),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(in.Close)
		return in
	}
	ctx := context.Background()
	if _, err := newVerifier().Verify(ctx, "victim-token"); err != nil {
		t.Fatal(err)
	}
	keyOf := func(tok string) string {
		sum := sha256.Sum256([]byte(tok))
		return "auth:introspection:" + hex.EncodeToString(sum[:])
	}
	victim, err := mr.Get(keyOf("victim-token"))
	if err != nil {
		t.Fatal(err)
	}
	// 1. Copy the victim's genuine (MAC-valid) entry to the attacker's key.
	mustSet(t, mr, keyOf("attacker-token"), victim)
	if _, err := newVerifier().Verify(ctx, "attacker-token"); !errors.Is(err, auth.ErrTokenInvalid) {
		t.Fatalf("moved entry: err = %v, want ErrTokenInvalid", err)
	}
	// 2. Forge an entry with a made-up MAC.
	forged := "v" + strings.Repeat("x", sha256.Size) + fmt.Sprintf(`{"k":%q,"p":{"Subject":"admin"},"until":"2999-01-01T00:00:00Z"}`, strings.TrimPrefix(keyOf("forged-token"), "auth:introspection:"))
	mustSet(t, mr, keyOf("forged-token"), forged)
	if _, err := newVerifier().Verify(ctx, "forged-token"); !errors.Is(err, auth.ErrTokenInvalid) {
		t.Fatalf("forged entry: err = %v, want ErrTokenInvalid", err)
	}
	// 3. Plant a negative entry (DoS attempt): ignored, the authority answers.
	mr.Del(keyOf("victim-token"))
	mustSet(t, mr, keyOf("victim-token"), "n")
	if p, err := newVerifier().Verify(ctx, "victim-token"); err != nil || p.Subject != "victim" {
		t.Fatalf("planted negative: %v %+v", err, p)
	}
}

func TestOptionalMethods(t *testing.T) {
	if _, err := auth.New(auth.Config{
		Verifiers: []auth.Verifier{newHMAC(t, issuerA)},
		Public:    []string{"/s/Feed"}, Optional: []string{"/s/Feed"},
	}); err == nil {
		t.Fatal("method both public and optional accepted")
	}
	if _, err := auth.New(auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA)}, Optional: []string{"/"}}); err == nil {
		t.Fatal(`optional "/" accepted`)
	}
	authorized := 0
	a := newAuthenticator(t, auth.Config{
		Verifiers: []auth.Verifier{newHMAC(t, issuerA)},
		Optional:  []string{"/s/Feed"},
		Authorizer: auth.AuthorizerFunc(func(context.Context, string, *auth.Principal) error {
			authorized++
			return nil
		}),
	})
	ic := a.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/s/Feed"}
	var got *auth.Principal
	h := func(ctx context.Context, _ any) (any, error) { got, _ = auth.FromContext(ctx); return nil, nil }

	// No token: anonymous, no Authorizer call.
	if _, err := ic(context.Background(), nil, info, h); err != nil || got != nil || authorized != 0 {
		t.Fatalf("anonymous: err=%v principal=%v authorized=%d", err, got, authorized)
	}
	// Valid token: principal attached, Authorizer runs.
	good := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")))
	if _, err := ic(good, nil, info, h); err != nil || got == nil || got.Subject != "user-1" || authorized != 1 {
		t.Fatalf("with token: err=%v principal=%v authorized=%d", err, got, authorized)
	}
	// Bad token: rejected, never anonymous.
	bad := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer not.a.jwt"))
	if _, err := ic(bad, nil, info, h); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("bad token: err = %v", err)
	}
	// Other methods still require a token.
	if _, err := ic(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/s/Other"}, h); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("other method: err = %v", err)
	}
}

func TestMaxTokenBytes(t *testing.T) {
	a := newAuthenticator(t, auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA)}, MaxTokenBytes: 64})
	tok := sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")
	if _, err := a.Authenticate(context.Background(), tok); !errors.Is(err, auth.ErrTokenInvalid) {
		t.Fatalf("oversized token: err = %v", err)
	}
	def := newAuthenticator(t, auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA)}})
	if _, err := def.Authenticate(context.Background(), strings.Repeat("a", 8193)); !errors.Is(err, auth.ErrTokenInvalid) {
		t.Fatalf("default limit: err = %v", err)
	}
	if _, err := def.Authenticate(context.Background(), tok); err != nil {
		t.Fatal(err)
	}
}

// A stream ends with TOKEN_EXPIRED when its token expires.
func TestStreamEndsAtTokenExpiry(t *testing.T) {
	c := baseClaims(issuerA)
	c["exp"] = time.Now().Add(time.Second).Unix()
	md := metadata.Pairs("authorization", "Bearer "+sign(t, c, jwa.HS256(), hmacSecret, "k1"))
	run := func(outlive bool) (error, bool) {
		a := newAuthenticator(t, auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA)}, StreamOutlivesToken: outlive})
		var hasDeadline bool
		err := a.StreamServerInterceptor()(nil, fakeStream{ctx: metadata.NewIncomingContext(context.Background(), md)},
			&grpc.StreamServerInfo{FullMethod: "/s/Stream"}, func(_ any, ss grpc.ServerStream) error {
				_, hasDeadline = ss.Context().Deadline()
				if outlive {
					return nil
				}
				<-ss.Context().Done()
				return ss.Context().Err()
			})
		return err, hasDeadline
	}
	if err, dl := run(false); !errors.Is(err, auth.ErrTokenExpired) || !dl {
		t.Fatalf("expiring stream: err = %v deadline = %v", err, dl)
	}
	if err, dl := run(true); err != nil || dl {
		t.Fatalf("StreamOutlivesToken: err = %v deadline = %v", err, dl)
	}
}

// A cached introspection principal is not shared between requests.
func TestIntrospectionPrincipalNotShared(t *testing.T) {
	in, err := auth.NewIntrospection(auth.IntrospectionConfig{Introspect: func(context.Context, string) (*auth.Principal, error) {
		return &auth.Principal{Subject: "u1", Scopes: []string{"a"}, Claims: map[string]any{"k": "v"}, ExpiresAt: time.Now().Add(time.Hour)}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	p1, _ := in.Verify(context.Background(), "tok")
	p1.Scopes[0], p1.Claims["k"] = "admin", "changed"
	p2, _ := in.Verify(context.Background(), "tok")
	if p2.Scopes[0] != "a" || p2.Claims["k"] != "v" {
		t.Fatalf("cached principal was mutated: %+v", p2)
	}
}

// An authority error with a NotFound code (an IdP answering 404) is
// AUTH_UNAVAILABLE, asked once per Verify, not a "tampered cache entry".
func TestIntrospectionAuthorityNotFoundIsUnavailable(t *testing.T) {
	var calls atomic.Int64
	fn := func(context.Context, string) (*auth.Principal, error) {
		calls.Add(1)
		return nil, errs.Wrap(errors.New("404 from IdP"), errs.ErrNotFound)
	}
	in, err := auth.NewIntrospection(auth.IntrospectionConfig{Introspect: fn, Logger: log.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	if _, err := in.Verify(context.Background(), "tok"); !errors.Is(err, auth.ErrAuthUnavailable) {
		t.Fatalf("err = %v, want ErrAuthUnavailable", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("authority called %d times, want 1", n)
	}
}

// Owner decision 2026-11-22: without a Logger, auth logs through
// log.Default(), in the schema installed in main.
func TestLogsThroughKitchenLog(t *testing.T) {
	var buf strings.Builder
	var mu sync.Mutex
	w := writerFunc(func(p []byte) (int, error) { mu.Lock(); defer mu.Unlock(); return buf.Write(p) })
	log.SetDefault(log.New(w, log.Options{Schema: log.SchemaGCP, Level: log.LevelDebug}))
	a := newAuthenticator(t, auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA)}})
	_, _ = a.UnaryServerInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x.Svc/Get"},
		func(context.Context, any) (any, error) { return nil, nil })
	mu.Lock()
	out := buf.String()
	mu.Unlock()
	if !strings.Contains(out, `"message":"request rejected"`) || !strings.Contains(out, `"severity":"DEBUG"`) {
		t.Fatalf("auth did not log through kitchen/log: %q", out)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// D7: the JWKS is fetched through kitchen/httpx, so the caller's client
// (hooks, tracing, SSRF policy) sees every fetch.
func TestJWKSFetchesThroughHTTPX(t *testing.T) {
	srv := newJWKSServer(t)
	var seen atomic.Int32
	client, err := httpx.New(httpx.WithBaseURL(srv.URL), httpx.WithResponseHook(func(*httpx.Response) error {
		seen.Add(1)
		return nil
	}))
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
	if seen.Load() == 0 {
		t.Fatal("JWKS fetch bypassed the httpx client")
	}
}

func mustSet(t *testing.T, mr *miniredis.Miniredis, key, value string) {
	t.Helper()
	if err := mr.Set(key, value); err != nil {
		t.Fatal(err)
	}
}
