package webhook_test

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/webhook"
)

// The test vector of the Standard Webhooks specification.
const (
	specSecret    = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw" //nolint:gosec // G101: the spec's public test vector
	specID        = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	specTimestamp = 1614265330
	specPayload   = `{"test": 2432232314}`
	specSignature = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

var specTime = time.Unix(specTimestamp, 0)

func fixedNow(t time.Time) func() time.Time { return func() time.Time { return t } }

func mustSigner(t *testing.T, secrets ...string) *webhook.Signer {
	t.Helper()
	s, err := webhook.NewSigner(secrets...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mustVerifier(t *testing.T, now time.Time, secrets ...string) *webhook.StandardVerifier {
	t.Helper()
	v, err := webhook.NewVerifier(webhook.VerifierConfig{Secrets: secrets, Now: fixedNow(now)})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestSpecVector(t *testing.T) {
	sig, err := mustSigner(t, specSecret).Sign(specID, specTime, []byte(specPayload))
	if err != nil {
		t.Fatal(err)
	}
	if sig != specSignature {
		t.Fatalf("Sign = %q, want %q", sig, specSignature)
	}
	h := http.Header{}
	h.Set(webhook.HeaderID, specID)
	h.Set(webhook.HeaderTimestamp, strconv.Itoa(specTimestamp))
	h.Set(webhook.HeaderSignature, specSignature)
	m, err := mustVerifier(t, specTime, specSecret).Verify(context.Background(), h, []byte(specPayload))
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != specID || !m.Timestamp.Equal(specTime) || string(m.Body) != specPayload {
		t.Fatalf("message = %+v", m)
	}
}

func TestVerify(t *testing.T) {
	other, err := webhook.GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}
	sk, pk, err := webhook.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	_, otherPK, err := webhook.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(specPayload)
	headers := func(t *testing.T, s *webhook.Signer, id string, ts time.Time) http.Header {
		t.Helper()
		h, err := s.Headers(id, ts, body)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	tests := []struct {
		name    string
		secrets []string // verifier
		header  func(t *testing.T) http.Header
		body    string
		want    error // nil: accepted
	}{
		{"valid", []string{specSecret}, func(t *testing.T) http.Header {
			return headers(t, mustSigner(t, specSecret), specID, specTime)
		}, "", nil},
		{"rotation: signed with old and new, verifier knows the new one", []string{other}, func(t *testing.T) http.Header {
			return headers(t, mustSigner(t, specSecret, other), specID, specTime)
		}, "", nil},
		{"rotation: verifier knows both, signed with one", []string{specSecret, other}, func(t *testing.T) http.Header {
			return headers(t, mustSigner(t, other), specID, specTime)
		}, "", nil},
		{"ed25519", []string{pk}, func(t *testing.T) http.Header {
			return headers(t, mustSigner(t, sk), specID, specTime)
		}, "", nil},
		{"ed25519 with another public key", []string{otherPK}, func(t *testing.T) http.Header {
			return headers(t, mustSigner(t, sk), specID, specTime)
		}, "", webhook.ErrNoMatchingSignature},
		{"v1 signature does not match a public key", []string{pk}, func(t *testing.T) http.Header {
			return headers(t, mustSigner(t, specSecret), specID, specTime)
		}, "", webhook.ErrNoMatchingSignature},
		{"wrong secret", []string{other}, func(t *testing.T) http.Header {
			return headers(t, mustSigner(t, specSecret), specID, specTime)
		}, "", webhook.ErrNoMatchingSignature},
		{"tampered body", []string{specSecret}, func(t *testing.T) http.Header {
			return headers(t, mustSigner(t, specSecret), specID, specTime)
		}, `{"test": 1}`, webhook.ErrNoMatchingSignature},
		{"tampered id", []string{specSecret}, func(t *testing.T) http.Header {
			h := headers(t, mustSigner(t, specSecret), specID, specTime)
			h.Set(webhook.HeaderID, "msg_other")
			return h
		}, "", webhook.ErrNoMatchingSignature},
		{"timestamp too old", []string{specSecret}, func(t *testing.T) http.Header {
			return headers(t, mustSigner(t, specSecret), specID, specTime.Add(-5*time.Minute-time.Second))
		}, "", webhook.ErrTimestampTolerance},
		{"timestamp too new", []string{specSecret}, func(t *testing.T) http.Header {
			return headers(t, mustSigner(t, specSecret), specID, specTime.Add(5*time.Minute+time.Second))
		}, "", webhook.ErrTimestampTolerance},
		{"timestamp at the tolerance", []string{specSecret}, func(t *testing.T) http.Header {
			return headers(t, mustSigner(t, specSecret), specID, specTime.Add(-5*time.Minute))
		}, "", nil},
		{"timestamp not a number", []string{specSecret}, func(t *testing.T) http.Header {
			h := headers(t, mustSigner(t, specSecret), specID, specTime)
			h.Set(webhook.HeaderTimestamp, "1614265330.5")
			return h
		}, "", webhook.ErrMissingHeader},
		{"missing id", []string{specSecret}, func(t *testing.T) http.Header {
			h := headers(t, mustSigner(t, specSecret), specID, specTime)
			h.Del(webhook.HeaderID)
			return h
		}, "", webhook.ErrMissingHeader},
		{"missing timestamp", []string{specSecret}, func(t *testing.T) http.Header {
			h := headers(t, mustSigner(t, specSecret), specID, specTime)
			h.Del(webhook.HeaderTimestamp)
			return h
		}, "", webhook.ErrMissingHeader},
		{"missing signature", []string{specSecret}, func(t *testing.T) http.Header {
			h := headers(t, mustSigner(t, specSecret), specID, specTime)
			h.Del(webhook.HeaderSignature)
			return h
		}, "", webhook.ErrMissingHeader},
		{"id with a dot", []string{specSecret}, func(t *testing.T) http.Header {
			h := headers(t, mustSigner(t, specSecret), specID, specTime)
			h.Set(webhook.HeaderID, "msg.1")
			return h
		}, "", webhook.ErrMissingHeader},
		{"malformed entries are skipped", []string{specSecret}, func(t *testing.T) http.Header {
			h := headers(t, mustSigner(t, specSecret), specID, specTime)
			h.Set(webhook.HeaderSignature, "garbage v1,!!! v2,AAAA "+h.Get(webhook.HeaderSignature))
			return h
		}, "", nil},
		{"too many signatures", []string{specSecret}, func(t *testing.T) http.Header {
			h := headers(t, mustSigner(t, specSecret), specID, specTime)
			h.Set(webhook.HeaderSignature, strings.Repeat("v1,AAAA ", 10)+h.Get(webhook.HeaderSignature))
			return h
		}, "", webhook.ErrSignatureInvalid},
		{"empty signature list", []string{specSecret}, func(t *testing.T) http.Header {
			h := headers(t, mustSigner(t, specSecret), specID, specTime)
			h.Set(webhook.HeaderSignature, "v1,")
			return h
		}, "", webhook.ErrNoMatchingSignature},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := body
			if tc.body != "" {
				b = []byte(tc.body)
			}
			_, err := mustVerifier(t, specTime, tc.secrets...).Verify(context.Background(), tc.header(t), b)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Verify = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.want) || !errors.Is(err, webhook.ErrSignatureInvalid) {
				t.Fatalf("Verify = %v, want %v inside ErrSignatureInvalid", err, tc.want)
			}
			if e := errs.FromError(err); e.Reason() != "WEBHOOK_SIGNATURE_INVALID" || strings.Contains(e.Message(), "timestamp") {
				t.Fatalf("client sees %q %q", e.Reason(), e.Message())
			}
		})
	}
}

func TestSecrets(t *testing.T) {
	sk, pk, err := webhook.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	b64 := func(n int) string { return base64.StdEncoding.EncodeToString(make([]byte, n)) }
	tests := []struct {
		name       string
		secret     string
		signerOK   bool
		verifierOK bool
	}{
		{"symmetric", specSecret, true, true},
		{"secret key", sk, true, false},
		{"secret key as a full private key", webhook.PrefixSecretKey + b64(ed25519.PrivateKeySize), true, false},
		{"public key", pk, false, true},
		{"no prefix", "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw", false, false},
		{"not base64", "whsec_***", false, false},
		{"symmetric too short", webhook.PrefixSymmetric + b64(8), false, false},
		{"public key of the wrong size", webhook.PrefixPublicKey + b64(31), false, false},
		{"secret key of the wrong size", webhook.PrefixSecretKey + b64(40), false, false},
		{"surrounding spaces are trimmed", " " + specSecret + "\n", true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := webhook.NewSigner(tc.secret); (err == nil) != tc.signerOK {
				t.Errorf("NewSigner err = %v, want ok=%v", err, tc.signerOK)
			}
			if _, err := webhook.NewVerifier(webhook.VerifierConfig{Secrets: []string{tc.secret}}); (err == nil) != tc.verifierOK {
				t.Errorf("NewVerifier err = %v, want ok=%v", err, tc.verifierOK)
			}
		})
	}
	if _, err := webhook.NewSigner(); err == nil {
		t.Error("NewSigner without secrets succeeded")
	}
	if _, err := webhook.NewVerifier(webhook.VerifierConfig{}); err == nil {
		t.Error("NewVerifier without secrets succeeded")
	}
	if _, err := webhook.NewVerifier(webhook.VerifierConfig{Secrets: []string{specSecret}, Tolerance: -1}); err == nil {
		t.Error("negative tolerance accepted")
	}
}

func TestSignRefusesBadIDs(t *testing.T) {
	s := mustSigner(t, specSecret)
	for _, id := range []string{"", "a.b", "a b", strings.Repeat("a", 257), "msg_é"} {
		if _, err := s.Sign(id, specTime, nil); err == nil {
			t.Errorf("Sign(%q) succeeded", id)
		}
	}
}

func TestGenerated(t *testing.T) {
	sec, err := webhook.GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}
	sk, pk, err := webhook.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	id := webhook.NewMessageID()
	if !strings.HasPrefix(id, "msg_") || len(id) != 30 || id == webhook.NewMessageID() {
		t.Fatalf("NewMessageID = %q", id)
	}
	h, err := mustSigner(t, sec, sk).Headers(id, specTime, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{sec, pk} {
		if _, err := mustVerifier(t, specTime, secret).Verify(context.Background(), h, []byte("x")); err != nil {
			t.Errorf("verify with %s: %v", secret[:5], err)
		}
	}
}
