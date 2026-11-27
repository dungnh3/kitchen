package webhook

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// Secret prefixes of the Standard Webhooks format.
const (
	// PrefixSymmetric marks an HMAC-SHA256 secret, for signing and verifying.
	PrefixSymmetric = "whsec_"
	// PrefixSecretKey marks an ed25519 secret key, for signing.
	PrefixSecretKey = "whsk_"
	// PrefixPublicKey marks an ed25519 public key, for verifying.
	PrefixPublicKey = "whpk_"
)

// Symmetric secrets shorter than this are refused; the spec recommends 24
// to 64 bytes.
const (
	minSymmetricBytes = 16
	maxSymmetricBytes = 1024
	generatedBytes    = 32
)

type keyKind int

const (
	kindSymmetric keyKind = iota + 1
	kindSecretKey
	kindPublicKey
)

// key is a parsed secret.
type key struct {
	kind keyKind
	raw  []byte
}

// parseKey parses a prefixed secret. The prefix is required, so a key can
// never be used as the wrong kind.
func parseKey(s string) (key, error) {
	s = strings.TrimSpace(s)
	var (
		k   key
		b64 string
	)
	switch {
	case strings.HasPrefix(s, PrefixSymmetric):
		k.kind, b64 = kindSymmetric, s[len(PrefixSymmetric):]
	case strings.HasPrefix(s, PrefixSecretKey):
		k.kind, b64 = kindSecretKey, s[len(PrefixSecretKey):]
	case strings.HasPrefix(s, PrefixPublicKey):
		k.kind, b64 = kindPublicKey, s[len(PrefixPublicKey):]
	default:
		return key{}, fmt.Errorf("webhook: secret must start with %q, %q or %q", PrefixSymmetric, PrefixSecretKey, PrefixPublicKey)
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return key{}, errors.New("webhook: secret is not valid base64")
	}
	switch k.kind {
	case kindSymmetric:
		if len(raw) < minSymmetricBytes || len(raw) > maxSymmetricBytes {
			return key{}, fmt.Errorf("webhook: symmetric secret must be %d to %d bytes, got %d", minSymmetricBytes, maxSymmetricBytes, len(raw))
		}
	case kindSecretKey:
		switch len(raw) {
		case ed25519.SeedSize:
			raw = ed25519.NewKeyFromSeed(raw)
		case ed25519.PrivateKeySize:
		default:
			return key{}, fmt.Errorf("webhook: ed25519 secret key must be %d or %d bytes, got %d", ed25519.SeedSize, ed25519.PrivateKeySize, len(raw))
		}
	case kindPublicKey:
		if len(raw) != ed25519.PublicKeySize {
			return key{}, fmt.Errorf("webhook: ed25519 public key must be %d bytes, got %d", ed25519.PublicKeySize, len(raw))
		}
	}
	k.raw = raw
	return k, nil
}

// GenerateSecret returns a new random symmetric secret ("whsec_" and 32
// random bytes in base64). Give it to the receiver once and store it as a
// secret.
func GenerateSecret() (string, error) {
	b := make([]byte, generatedBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return PrefixSymmetric + base64.StdEncoding.EncodeToString(b), nil
}

// GenerateKeyPair returns a new ed25519 key pair: the secret key ("whsk_")
// signs, the public key ("whpk_") verifies and may be published.
func GenerateKeyPair() (secretKey, publicKey string, err error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", "", err
	}
	return PrefixSecretKey + base64.StdEncoding.EncodeToString(priv.Seed()),
		PrefixPublicKey + base64.StdEncoding.EncodeToString(pub), nil
}

// NewMessageID returns a new random message id: "msg_" and 26 base32
// characters (128 bits).
func NewMessageID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b) // crypto/rand.Read never fails
	return "msg_" + strings.ToLower(base32NoPad.EncodeToString(b))
}
