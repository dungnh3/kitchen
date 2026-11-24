package auth_test

// Attack-style tests: key rotation under load, authority outages, Close,
// critical headers, stream expiry, audience and key-size minimums.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dungnh3/kitchen/auth"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/httpx"
	"github.com/dungnh3/kitchen/log"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// memJWKS serves a JWKS from memory through an httpx transport, so tests
// can run inside a synctest bubble.
type memJWKS struct {
	mu      sync.Mutex
	set     jwk.Set
	header  http.Header
	status  int
	delay   time.Duration
	hits    atomic.Int64
	release chan struct{}    // when set, every fetch waits for it
	raw     []map[string]any // extra keys jwx would refuse to build
}

const memURL = "https://idp.example.com/.well-known/jwks.json"

func newMemJWKS(keys ...jwk.Key) *memJWKS {
	m := &memJWKS{set: jwk.NewSet(), status: http.StatusOK, header: http.Header{}}
	for _, k := range keys {
		_ = m.set.AddKey(k)
	}
	return m
}

func (m *memJWKS) add(k jwk.Key) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.set.AddKey(k)
}

func (m *memJWKS) setStatus(code int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = code
}

func (m *memJWKS) RoundTrip(r *http.Request) (*http.Response, error) {
	m.hits.Add(1)
	m.mu.Lock()
	delay, release := m.delay, m.release
	m.mu.Unlock()
	if release != nil {
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, _ := json.Marshal(m.set)
	if len(m.raw) > 0 {
		var doc struct {
			Keys []any `json:"keys"`
		}
		_ = json.Unmarshal(b, &doc)
		for _, k := range m.raw {
			doc.Keys = append(doc.Keys, k)
		}
		b, _ = json.Marshal(doc)
	}
	h := m.header.Clone()
	h.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: m.status, Status: http.StatusText(m.status), Header: h,
		Body: io.NopCloser(bytes.NewReader(b)), ContentLength: int64(len(b)), Request: r,
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
	}, nil
}

func (m *memJWKS) client(t *testing.T) *httpx.Client {
	t.Helper()
	c, err := httpx.New(httpx.WithBaseURL(memURL), httpx.WithTransport(m), httpx.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newMemVerifier(t *testing.T, m *memJWKS, cfg auth.JWKSConfig) *auth.JWKS {
	t.Helper()
	cfg.Issuer, cfg.URL, cfg.Client = issuerA, memURL, m.client(t)
	if cfg.Logger == nil {
		cfg.Logger = log.Discard()
	}
	v, err := auth.NewJWKS(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close(context.Background()) })
	return v
}

func rsaKey(t *testing.T, bits int) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// While the IdP rotates its key, every concurrent request carrying the
// new kid must wait for the one shared refresh, not get a 401.
func TestJWKSRotationBurstSharesRefresh(t *testing.T) {
	k1, k2 := rsaKey(t, 2048), rsaKey(t, 2048)
	m := newMemJWKS(publicJWK(t, k1, "k1"))
	v := newMemVerifier(t, m, auth.JWKSConfig{})
	m.add(publicJWK(t, k2, "k2"))
	m.mu.Lock()
	m.delay = 50 * time.Millisecond // a real IdP is not instant
	m.mu.Unlock()
	before := m.hits.Load()

	tok := sign(t, baseClaims(issuerA), jwa.RS256(), k2, "k2")
	var bad atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if _, err := v.Verify(context.Background(), tok); err != nil {
				bad.Add(1)
			}
		})
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d/50 requests with the rotated kid were rejected", bad.Load())
	}
	if n := m.hits.Load() - before; n != 1 {
		t.Fatalf("rotation caused %d fetches, want 1", n)
	}
}

// An unknown kid while the IdP is down is "authority unavailable"
// (503), never "invalid token" (401).
func TestJWKSUnknownKidWhileAuthorityDown(t *testing.T) {
	k1, k2 := rsaKey(t, 2048), rsaKey(t, 2048)
	m := newMemJWKS(publicJWK(t, k1, "k1"))
	v := newMemVerifier(t, m, auth.JWKSConfig{})
	m.setStatus(http.StatusServiceUnavailable)

	_, err := v.Verify(context.Background(), sign(t, baseClaims(issuerA), jwa.RS256(), k2, "k2"))
	if !errors.Is(err, auth.ErrAuthUnavailable) {
		t.Fatalf("err = %v, want AUTH_UNAVAILABLE", err)
	}
	// Rate-limited follow-ups keep saying 503 while the last refresh failed.
	_, err = v.Verify(context.Background(), sign(t, baseClaims(issuerA), jwa.RS256(), k2, "k3"))
	if !errors.Is(err, auth.ErrAuthUnavailable) {
		t.Fatalf("second err = %v, want AUTH_UNAVAILABLE", err)
	}
	// Known keys keep working from the last good key set.
	if _, err := v.Verify(context.Background(), sign(t, baseClaims(issuerA), jwa.RS256(), k1, "k1")); err != nil {
		t.Fatalf("known key while down: %v", err)
	}
}

// Verify after Close fails fast with AUTH_UNAVAILABLE instead of waiting
// for the caller's deadline. Close is idempotent.
func TestJWKSVerifyAfterClose(t *testing.T) {
	k1 := rsaKey(t, 2048)
	m := newMemJWKS(publicJWK(t, k1, "k1"))
	v := newMemVerifier(t, m, auth.JWKSConfig{})
	if err := v.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := v.Verify(context.Background(), sign(t, baseClaims(issuerA), jwa.RS256(), k1, "k1"))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, auth.ErrAuthUnavailable) {
			t.Fatalf("err = %v, want AUTH_UNAVAILABLE", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Verify after Close blocked")
	}
	if err := v.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// The key set refreshes in the background on the Cache-Control
// schedule (clamped to Min/MaxRefresh), failures are logged and retried with
// backoff, and Close stops everything.
func TestJWKSBackgroundRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		k1, k2 := rsaKey(t, 2048), rsaKey(t, 2048)
		m := newMemJWKS(publicJWK(t, k1, "k1"))
		m.header.Set("Cache-Control", "public, max-age=600")
		var logs syncBuffer
		v := newMemVerifier(t, m, auth.JWKSConfig{
			MinRefresh: time.Minute, MaxRefresh: time.Hour, MissRefresh: time.Hour,
			Logger: log.New(&logs, log.Options{}),
		})
		if n := m.hits.Load(); n != 1 {
			t.Fatalf("initial fetches = %d", n)
		}
		m.add(publicJWK(t, k2, "k2"))
		time.Sleep(9 * time.Minute)
		synctest.Wait()
		if n := m.hits.Load(); n != 1 {
			t.Fatalf("refreshed before max-age: %d fetches", n)
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if n := m.hits.Load(); n != 2 {
			t.Fatalf("fetches after max-age = %d, want 2", n)
		}
		// The background refresh picked up k2 without a forced refresh.
		if _, err := v.Verify(context.Background(), sign(t, baseClaims(issuerA), jwa.RS256(), k2, "k2")); err != nil {
			t.Fatalf("k2 after background refresh: %v", err)
		}

		m.setStatus(http.StatusInternalServerError)
		time.Sleep(11 * time.Minute)
		synctest.Wait()
		if !strings.Contains(logs.String(), "jwks refresh failed") {
			t.Fatalf("refresh failure not logged: %s", logs.String())
		}
		failed := m.hits.Load()
		time.Sleep(time.Minute)
		synctest.Wait()
		if m.hits.Load() <= failed {
			t.Fatal("failed refresh was not retried within MinRefresh")
		}
		if err := v.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		after := m.hits.Load()
		time.Sleep(2 * time.Hour)
		synctest.Wait()
		if m.hits.Load() != after {
			t.Fatal("refreshing after Close")
		}
	})
}

// Keys that must never verify are dropped from the JWKS: weak RSA, use=enc,
// symmetric keys. A kid shared by two keys with different algorithms still
// finds the right one.
func TestJWKSKeyFiltering(t *testing.T) {
	weak := rsaKey(t, 1024)
	good := rsaKey(t, 2048)
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	encKey := publicJWK(t, good, "enc")
	_ = encKey.Set(jwk.KeyUsageKey, "enc")
	oct, _ := jwk.Import(hmacSecret)
	_ = oct.Set(jwk.KeyIDKey, "oct")
	ecJWK, _ := jwk.Import(&ec.PublicKey)
	_ = ecJWK.Set(jwk.KeyIDKey, "shared")
	_ = ecJWK.Set(jwk.AlgorithmKey, jwa.ES256())

	m := newMemJWKS(encKey, oct, publicJWK(t, good, "shared"), ecJWK)
	// jwx refuses to import RSA-1024, so publish it as raw JSON: one bad key
	// must not poison the rest of the set.
	m.raw = append(m.raw, map[string]any{
		"kty": "RSA", "kid": "weak", "alg": "RS256", "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(weak.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(weak.E)).Bytes()),
	})
	v := newMemVerifier(t, m, auth.JWKSConfig{MissRefresh: time.Hour})

	for name, tok := range map[string]string{
		"RSA-1024":   sign(t, baseClaims(issuerA), jwa.RS256(), weak, "weak"),
		"use=enc":    sign(t, baseClaims(issuerA), jwa.RS256(), good, "enc"),
		"oct in set": sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "oct"),
	} {
		if _, err := v.Verify(context.Background(), tok); !errors.Is(err, auth.ErrTokenInvalid) {
			t.Errorf("%s: err = %v, want TOKEN_INVALID", name, err)
		}
	}
	for name, tok := range map[string]string{
		"shared kid RS256": sign(t, baseClaims(issuerA), jwa.RS256(), good, "shared"),
		"shared kid ES256": sign(t, baseClaims(issuerA), jwa.ES256(), ec, "shared"),
	} {
		if _, err := v.Verify(context.Background(), tok); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// RFC 7515 section 4.1.11: a token naming a critical header extension
// the verifier does not understand must be rejected, even if its signature
// is valid.
func TestCriticalHeaderRejected(t *testing.T) {
	v := newHMAC(t, issuerA)
	for name, set := range map[string]func(jws.Headers){
		"unknown crit": func(h jws.Headers) { _ = h.Set("crit", []string{"x-ext"}); _ = h.Set("x-ext", 1) },
		"b64 false":    func(h jws.Headers) { _ = h.Set("b64", false); _ = h.Set("crit", []string{"b64"}) },
	} {
		t.Run(name, func(t *testing.T) {
			tok := jwt.New()
			for k, c := range baseClaims(issuerA) {
				_ = tok.Set(k, c)
			}
			hdr := jws.NewHeaders()
			_ = hdr.Set(jws.KeyIDKey, "k1")
			set(hdr)
			b, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256(), hmacSecret, jws.WithProtectedHeaders(hdr)))
			if err != nil {
				t.Skipf("jwx refuses to sign it: %v", err)
			}
			if _, err := v.Verify(context.Background(), string(b)); !errors.Is(err, auth.ErrTokenInvalid) {
				t.Fatalf("err = %v, want TOKEN_INVALID", err)
			}
		})
	}
}

type recvStream struct {
	fakeStream
	recv atomic.Int64
}

func (s *recvStream) RecvMsg(any) error { s.recv.Add(1); return nil }
func (s *recvStream) SendMsg(any) error { return nil }

// A handler that never looks at its context still cannot receive or
// send messages after the token expired.
func TestStreamMessagesStopAtTokenExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := baseClaims(issuerA)
		c["iat"] = time.Now().Unix()
		c["exp"] = time.Now().Add(time.Minute).Unix()
		md := metadata.Pairs("authorization", "Bearer "+sign(t, c, jwa.HS256(), hmacSecret, "k1"))
		a := newAuthenticator(t, auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA)}, Audience: audience})
		ss := &recvStream{fakeStream: fakeStream{ctx: metadata.NewIncomingContext(context.Background(), md)}}
		var recvErr, sendErr error
		err := a.StreamServerInterceptor()(nil, ss, &grpc.StreamServerInfo{FullMethod: "/s/Stream"},
			func(_ any, s grpc.ServerStream) error {
				if err := s.RecvMsg(nil); err != nil {
					return fmt.Errorf("recv before expiry: %w", err)
				}
				time.Sleep(2 * time.Minute)
				recvErr = s.RecvMsg(nil)
				sendErr = s.SendMsg(nil)
				return nil
			})
		if !errors.Is(recvErr, auth.ErrTokenExpired) || !errors.Is(sendErr, auth.ErrTokenExpired) {
			t.Fatalf("after expiry: recv = %v, send = %v", recvErr, sendErr)
		}
		if ss.recv.Load() != 1 {
			t.Fatalf("messages received = %d, want 1", ss.recv.Load())
		}
		if !errors.Is(err, auth.ErrTokenExpired) {
			t.Fatalf("stream err = %v, want TOKEN_EXPIRED", err)
		}
	})
}

// Audience is required: New refuses a config that checks no audience
// unless AnyAudience says so explicitly.
func TestAudienceRequired(t *testing.T) {
	if _, err := auth.New(auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA)}}); err == nil {
		t.Fatal("New accepted a config without Audience")
	}
	if _, err := auth.New(auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA)}, Audience: audience, AnyAudience: true}); err == nil {
		t.Fatal("New accepted Audience together with AnyAudience")
	}
	a := newAuthenticator(t, auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA)}, AnyAudience: true})
	if _, err := a.Authenticate(context.Background(), sign(t, baseClaims(issuerA), jwa.HS256(), hmacSecret, "k1")); err != nil {
		t.Fatalf("AnyAudience: %v", err)
	}
	// A token without aud fails the audience check.
	b := newAuthenticator(t, auth.Config{Verifiers: []auth.Verifier{newHMAC(t, issuerA)}, Audience: audience})
	c := baseClaims(issuerA)
	delete(c, "aud")
	if _, err := b.Authenticate(context.Background(), sign(t, c, jwa.HS256(), hmacSecret, "k1")); !errors.Is(err, auth.ErrTokenInvalid) {
		t.Fatalf("no aud: %v", err)
	}
}

// RFC 7518 minimums: HMAC secrets at least the hash size, RSA at least
// 2048 bits; leeway bounded.
func TestKeyMinimums(t *testing.T) {
	small := rsaKey(t, 1024)
	bad := map[string]auth.StaticConfig{
		"HS384 with 32 bytes": {Keys: []auth.Key{{Alg: jwa.HS384(), Material: hmacSecret}}},
		"HS512 with 48 bytes": {Keys: []auth.Key{{Alg: jwa.HS512(), Material: bytes.Repeat([]byte{1}, 48)}}},
		"RSA 1024":            {Keys: []auth.Key{{Alg: jwa.RS256(), Material: &small.PublicKey}}},
		"PS256 RSA 1024":      {Keys: []auth.Key{{Alg: jwa.PS256(), Material: &small.PublicKey}}},
		"leeway 6m":           {Keys: []auth.Key{{Alg: jwa.HS256(), Material: hmacSecret}}, Leeway: 6 * time.Minute},
	}
	for name, cfg := range bad {
		cfg.Issuer = issuerA
		if _, err := auth.NewStatic(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	good := map[string]auth.StaticConfig{
		"HS384 with 48 bytes": {Keys: []auth.Key{{Alg: jwa.HS384(), Material: bytes.Repeat([]byte{1}, 48)}}},
		"HS512 with 64 bytes": {Keys: []auth.Key{{Alg: jwa.HS512(), Material: bytes.Repeat([]byte{1}, 64)}}},
		"leeway 5m":           {Keys: []auth.Key{{Alg: jwa.HS256(), Material: hmacSecret}}, Leeway: 5 * time.Minute},
	}
	for name, cfg := range good {
		cfg.Issuer = issuerA
		if _, err := auth.NewStatic(cfg); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := auth.NewJWKS(context.Background(), auth.JWKSConfig{Issuer: issuerA, URL: memURL, Leeway: time.Hour}); err == nil {
		t.Error("JWKS accepted a 1h leeway")
	}
}

// An authority failure whose cause happens to carry a token reason is still
// a 503: "unavailable" wins over any reason found deeper in the chain.
func TestUnavailableWinsOverNestedTokenReason(t *testing.T) {
	nested := errs.Wrap(errs.Wrap(errors.New("upstream said so"), auth.ErrTokenInvalid), auth.ErrAuthUnavailable)
	var calls atomic.Int64
	in, err := auth.NewIntrospection(auth.IntrospectionConfig{
		Introspect: func(context.Context, string) (*auth.Principal, error) { calls.Add(1); return nil, nested },
		Logger:     log.Discard(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	for range 2 {
		if _, err := in.Verify(context.Background(), "opaque"); !errors.Is(err, auth.ErrAuthUnavailable) {
			t.Fatalf("introspection err = %v, want AUTH_UNAVAILABLE", err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("authority failure was cached: %d calls", calls.Load())
	}

	a := newAuthenticator(t, auth.Config{Opaque: verifierFunc(func(context.Context, string) (*auth.Principal, error) {
		return nil, nested
	}), AnyAudience: true})
	md := metadata.Pairs("authorization", "Bearer opaque")
	_, err = a.UnaryServerInterceptor()(metadata.NewIncomingContext(context.Background(), md), nil,
		&grpc.UnaryServerInfo{FullMethod: "/s/M"}, func(context.Context, any) (any, error) { return nil, nil })
	if !errors.Is(err, auth.ErrAuthUnavailable) || errors.Is(err, auth.ErrTokenInvalid) {
		t.Fatalf("authenticator err = %v, want only AUTH_UNAVAILABLE", err)
	}
}

type verifierFunc func(context.Context, string) (*auth.Principal, error)

func (verifierFunc) Issuer() string { return "" }
func (f verifierFunc) Verify(ctx context.Context, tok string) (*auth.Principal, error) {
	return f(ctx, tok)
}

// RFC 7662: the response's aud is checked when Audience is set, and a
// transport failure is AUTH_UNAVAILABLE.
func TestRFC7662AudienceAndTransport(t *testing.T) {
	body := `{"active":true,"sub":"u","aud":["other-api"],"exp":` + jsonInt(time.Now().Add(time.Hour).Unix()) + `}`
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(body)), Request: r,
		}, nil
	})
	client, _ := httpx.New(httpx.WithBaseURL("https://idp.example.com"), httpx.WithTransport(rt))
	f := auth.RFC7662(auth.RFC7662Config{Endpoint: "https://idp.example.com/introspect", Client: client, Audience: audience})
	if _, err := f(context.Background(), "tok"); !errors.Is(err, auth.ErrTokenInvalid) {
		t.Fatalf("wrong aud: %v", err)
	}
	body = strings.Replace(body, "other-api", audience, 1)
	if p, err := f(context.Background(), "tok"); err != nil || p.Subject != "u" {
		t.Fatalf("right aud: %v", err)
	}

	down, _ := httpx.New(httpx.WithBaseURL("https://idp.example.com"), httpx.WithTransport(roundTripFunc(
		func(*http.Request) (*http.Response, error) { return nil, errors.New("connection refused") })))
	g := auth.RFC7662(auth.RFC7662Config{Endpoint: "https://idp.example.com/introspect", Client: down})
	if _, err := g(context.Background(), "tok"); !errors.Is(err, auth.ErrAuthUnavailable) {
		t.Fatalf("transport failure: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
