package webhook

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The Standard Webhooks headers.
const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"
)

// Signature versions.
const (
	versionHMAC    = "v1"
	versionEd25519 = "v1a"
)

// maxIDBytes bounds a message id.
const maxIDBytes = 256

var base32NoPad = base32.StdEncoding.WithPadding(base32.NoPadding)

// checkID accepts 1 to 256 printable ASCII bytes without spaces or dots. A
// dot is refused because it separates the parts of the signed content.
func checkID(id string) error {
	if id == "" || len(id) > maxIDBytes {
		return fmt.Errorf("message id must be 1 to %d bytes", maxIDBytes)
	}
	for i := range len(id) {
		if c := id[i]; c <= ' ' || c > '~' || c == '.' {
			return errors.New("message id must be printable ASCII without spaces or dots")
		}
	}
	return nil
}

// signedContent is "id.timestamp.body".
func signedContent(id, ts string, body []byte) []byte {
	b := make([]byte, 0, len(id)+len(ts)+len(body)+2)
	b = append(b, id...)
	b = append(b, '.')
	b = append(b, ts...)
	b = append(b, '.')
	return append(b, body...)
}

func hmacSum(secret, content []byte) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write(content)
	return m.Sum(nil)
}

// Signer signs outgoing webhooks. It is safe for concurrent use.
type Signer struct {
	keys []key
}

// NewSigner returns a Signer that signs with every secret: symmetric
// ("whsec_", a v1 signature) or ed25519 secret keys ("whsk_", a v1a
// signature). Pass the old and the new secret during a rotation, so
// receivers that know either one accept the request.
func NewSigner(secrets ...string) (*Signer, error) {
	if len(secrets) == 0 {
		return nil, errors.New("webhook: signer needs at least one secret")
	}
	s := &Signer{}
	for i, sec := range secrets {
		k, err := parseKey(sec)
		if err != nil {
			return nil, fmt.Errorf("secret %d: %w", i, err)
		}
		if k.kind == kindPublicKey {
			return nil, fmt.Errorf("webhook: secret %d: a public key (%s) cannot sign", i, PrefixPublicKey)
		}
		s.keys = append(s.keys, k)
	}
	return s, nil
}

// Sign returns the webhook-signature value for the message: one signature
// per secret, separated by spaces.
func (s *Signer) Sign(id string, ts time.Time, body []byte) (string, error) {
	if err := checkID(id); err != nil {
		return "", fmt.Errorf("webhook: %w", err)
	}
	content := signedContent(id, strconv.FormatInt(ts.Unix(), 10), body)
	sigs := make([]string, 0, len(s.keys))
	for _, k := range s.keys {
		switch k.kind {
		case kindSymmetric:
			sigs = append(sigs, versionHMAC+","+base64.StdEncoding.EncodeToString(hmacSum(k.raw, content)))
		case kindSecretKey:
			sigs = append(sigs, versionEd25519+","+base64.StdEncoding.EncodeToString(ed25519.Sign(k.raw, content)))
		}
	}
	return strings.Join(sigs, " "), nil
}

// Headers returns the three webhook headers for the message.
func (s *Signer) Headers(id string, ts time.Time, body []byte) (http.Header, error) {
	sig, err := s.Sign(id, ts, body)
	if err != nil {
		return nil, err
	}
	h := http.Header{}
	h.Set(HeaderID, id)
	h.Set(HeaderTimestamp, strconv.FormatInt(ts.Unix(), 10))
	h.Set(HeaderSignature, sig)
	return h, nil
}
