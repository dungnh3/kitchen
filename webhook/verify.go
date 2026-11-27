package webhook

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
)

// ErrSignatureInvalid is returned for every request that fails
// verification: a missing header, a malformed or stale timestamp, or no
// matching signature. It renders as 401 WEBHOOK_SIGNATURE_INVALID; the
// precise cause (one of the errors below) is kept for logs only, so a
// caller cannot probe which check failed.
var ErrSignatureInvalid = errs.New(codes.Unauthenticated, "WEBHOOK_SIGNATURE_INVALID",
	errs.Message("webhook signature is missing or invalid"))

// Causes wrapped in [ErrSignatureInvalid].
var (
	ErrMissingHeader       = errors.New("webhook: missing or malformed webhook header")
	ErrTimestampTolerance  = errors.New("webhook: timestamp outside the tolerance")
	ErrNoMatchingSignature = errors.New("webhook: no matching signature")
	errTooManySignatures   = errors.New("webhook: too many signatures")
)

// DefaultTolerance is how far the timestamp may be from the receiver's
// clock, in either direction.
const DefaultTolerance = 5 * time.Minute

// maxSignatures bounds the work one request can cause.
const maxSignatures = 10

// Message is a verified webhook.
type Message struct {
	// ID is the webhook-id: the same for every attempt of one message.
	ID string
	// Timestamp is the time of this attempt, as signed by the sender.
	Timestamp time.Time
	// Body is the raw payload.
	Body []byte
}

// Verifier verifies an incoming webhook. [NewVerifier] implements the
// Standard Webhooks scheme; a provider with its own scheme can be added by
// implementing Verifier. Verify must fail closed: any error refuses the
// request. The returned Message.ID is the key used by [Dedup].
type Verifier interface {
	Verify(ctx context.Context, header http.Header, body []byte) (Message, error)
}

// VerifierConfig configures [NewVerifier].
type VerifierConfig struct {
	// Secrets are the keys that are accepted: symmetric secrets ("whsec_")
	// and ed25519 public keys ("whpk_"). List the old and the new one while
	// a rotation is in progress. Required.
	Secrets []string
	// Tolerance is the accepted distance between the signed timestamp and
	// the clock (default 5 minutes). It bounds replays of a captured
	// request.
	Tolerance time.Duration
	// Now is the clock (default time.Now).
	Now func() time.Time
}

// StandardVerifier verifies Standard Webhooks signatures. It is safe for
// concurrent use.
type StandardVerifier struct {
	keys      []key
	tolerance time.Duration
	now       func() time.Time
}

// NewVerifier returns a Standard Webhooks verifier.
func NewVerifier(cfg VerifierConfig) (*StandardVerifier, error) {
	if len(cfg.Secrets) == 0 {
		return nil, errors.New("webhook: verifier needs at least one secret")
	}
	if cfg.Tolerance < 0 {
		return nil, errors.New("webhook: negative tolerance")
	}
	v := &StandardVerifier{tolerance: cfg.Tolerance, now: cfg.Now}
	if v.tolerance == 0 {
		v.tolerance = DefaultTolerance
	}
	if v.now == nil {
		v.now = time.Now
	}
	for i, sec := range cfg.Secrets {
		k, err := parseKey(sec)
		if err != nil {
			return nil, fmt.Errorf("secret %d: %w", i, err)
		}
		if k.kind == kindSecretKey {
			return nil, fmt.Errorf("webhook: secret %d: give the verifier the public key (%s), not the secret key", i, PrefixPublicKey)
		}
		v.keys = append(v.keys, k)
	}
	return v, nil
}

// Verify checks the headers against body. Every error wraps
// [ErrSignatureInvalid].
func (v *StandardVerifier) Verify(_ context.Context, header http.Header, body []byte) (Message, error) {
	m, err := v.verify(header, body)
	if err != nil {
		return Message{}, errs.Wrap(err, ErrSignatureInvalid)
	}
	return m, nil
}

func (v *StandardVerifier) verify(header http.Header, body []byte) (Message, error) {
	id := header.Get(HeaderID)
	tsRaw := header.Get(HeaderTimestamp)
	sigRaw := header.Get(HeaderSignature)
	if id == "" || tsRaw == "" || sigRaw == "" {
		return Message{}, ErrMissingHeader
	}
	if err := checkID(id); err != nil {
		return Message{}, fmt.Errorf("%w: %w", ErrMissingHeader, err)
	}
	sec, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil || sec < 0 {
		return Message{}, fmt.Errorf("%w: timestamp is not a Unix time in seconds", ErrMissingHeader)
	}
	ts := time.Unix(sec, 0)
	if d := v.now().Sub(ts); d > v.tolerance || d < -v.tolerance {
		return Message{}, ErrTimestampTolerance
	}
	sigs := strings.Fields(sigRaw)
	if len(sigs) > maxSignatures {
		return Message{}, errTooManySignatures
	}
	content := signedContent(id, tsRaw, body)
	var macs [][]byte // computed lazily, once per symmetric key
	for _, s := range sigs {
		version, b64, ok := strings.Cut(s, ",")
		if !ok {
			continue
		}
		got, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			continue
		}
		switch version {
		case versionHMAC:
			if macs == nil {
				macs = make([][]byte, len(v.keys))
			}
			for i, k := range v.keys {
				if k.kind != kindSymmetric {
					continue
				}
				if macs[i] == nil {
					macs[i] = hmacSum(k.raw, content)
				}
				if hmac.Equal(macs[i], got) {
					return Message{ID: id, Timestamp: ts, Body: body}, nil
				}
			}
		case versionEd25519:
			for _, k := range v.keys {
				if k.kind == kindPublicKey && ed25519.Verify(ed25519.PublicKey(k.raw), content, got) {
					return Message{ID: id, Timestamp: ts, Body: body}, nil
				}
			}
		}
	}
	return Message{}, ErrNoMatchingSignature
}
