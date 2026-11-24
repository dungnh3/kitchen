package auth

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dungnh3/kitchen/httpx"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/timex"
	"github.com/lestrrat-go/httpcc"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
)

// JWKSConfig configures NewJWKS.
type JWKSConfig struct {
	Issuer   string // required; must match the token's iss
	Audience string // when set, must be in the token's aud
	URL      string // required; the JWKS endpoint (https in production)
	// Algs is the algorithm allow-list. Default: RS256, ES256, EdDSA.
	// Symmetric (HS*) algorithms are rejected: a JWKS holds public keys.
	Algs []jwa.SignatureAlgorithm
	// Client fetches the JWKS (kitchen/httpx: tracing, metrics, retries,
	// redirect and SSRF policy). Default: httpx.New(httpx.WithBaseURL(URL),
	// httpx.WithTimeout(10*time.Second)). The body is capped at 1 MiB.
	Client *httpx.Client
	// MinRefresh and MaxRefresh bound the background refresh interval: the
	// response's Cache-Control max-age, clamped to [MinRefresh, MaxRefresh];
	// MinRefresh without max-age. A failed refresh is retried with backoff
	// (at most MinRefresh apart) and the last good key set stays in use.
	// Defaults 5m and 1h.
	MinRefresh, MaxRefresh time.Duration
	// MissRefresh is the minimum gap between refreshes started by an unknown
	// key ID (key rotation). Requests that arrive while such a refresh is
	// running wait for it. Default 30s.
	MissRefresh time.Duration
	// Leeway is the allowed clock skew for exp, nbf and iat. Default 30s,
	// at most 5m.
	Leeway time.Duration
	// Types, when set, lists the accepted JWT "typ" header values (RFC 8725
	// explicit typing), for example []string{"at+jwt"} for RFC 9068 access
	// tokens, so an ID token of the same issuer cannot be used as an access
	// token. Matching ignores case and an "application/" prefix. Empty: typ is
	// not checked (many IdPs send "JWT").
	Types []string
	Now   func() time.Time
	// Logger receives refresh failures and dropped keys. nil means
	// log.Default().
	Logger *log.Logger
}

// JWKS verifies JWTs with public keys from a JWKS URL. It refreshes the key
// set in the background; call Close to stop.
//
// Lookups read an immutable snapshot (no lock, no I/O). Refreshes, from the
// schedule or from an unknown kid, never overlap: a request that needs one
// while it runs waits for it.
type JWKS struct {
	check       jwtCheck
	url         string
	client      *httpx.Client
	minR, maxR  time.Duration
	missRefresh time.Duration
	log         *log.Logger

	keys    atomic.Pointer[keyIndex]
	healthy atomic.Bool // the last refresh succeeded

	mu         sync.Mutex
	inflight   *refreshCall
	lastForced time.Time

	life   context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed atomic.Bool
}

// refreshCall is one fetch shared by everyone who waits for it.
type refreshCall struct {
	done chan struct{}
	err  error
	next time.Duration // when to refresh again after this fetch
}

// keyIndex is an immutable key set, holding only keys that may verify.
type keyIndex struct {
	byKID map[string][]jwk.Key
	all   []jwk.Key
}

// fetchTimeout bounds one refresh, whoever started it.
const fetchTimeout = 30 * time.Second

// retryBase is the first retry delay after a failed background refresh.
const retryBase = time.Second

// NewJWKS fetches the key set once (bounded by ctx) and returns a verifier
// that keeps it fresh until Close.
func NewJWKS(ctx context.Context, cfg JWKSConfig) (*JWKS, error) {
	if cfg.Issuer == "" || cfg.URL == "" {
		return nil, errors.New("auth: jwks: issuer and URL are required")
	}
	algs := cfg.Algs
	if len(algs) == 0 {
		algs = []jwa.SignatureAlgorithm{jwa.RS256(), jwa.ES256(), jwa.EdDSA()}
	}
	for _, a := range algs {
		if strings.HasPrefix(a.String(), "HS") || a.String() == jwa.NoSignature().String() {
			return nil, fmt.Errorf("auth: jwks: algorithm %s is not allowed", a)
		}
	}
	leeway, err := checkLeeway(cfg.Leeway)
	if err != nil {
		return nil, fmt.Errorf("auth: jwks: %w", err)
	}
	j := &JWKS{
		check: jwtCheck{
			issuer: cfg.Issuer, audience: cfg.Audience, algs: algs, types: normTypes(cfg.Types),
			leeway: leeway, now: nowOr(cfg.Now),
		},
		url: cfg.URL, client: cfg.Client, minR: cfg.MinRefresh, maxR: cfg.MaxRefresh,
		missRefresh: cfg.MissRefresh, log: log.OrDefault(cfg.Logger),
	}
	if j.minR <= 0 {
		j.minR = 5 * time.Minute
	}
	if j.maxR <= 0 {
		j.maxR = time.Hour
	}
	j.maxR = max(j.maxR, j.minR)
	if j.missRefresh <= 0 {
		j.missRefresh = 30 * time.Second
	}
	if j.client == nil {
		if j.client, err = httpx.New(httpx.WithBaseURL(cfg.URL), httpx.WithTimeout(10*time.Second)); err != nil {
			return nil, fmt.Errorf("auth: jwks: %w", err)
		}
	}
	idx, next, err := j.fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf("auth: jwks: initial fetch of %s: %w", redact(cfg.URL), err)
	}
	j.keys.Store(idx)
	j.healthy.Store(true)
	// Refreshes run until Close, past ctx. They start from Background, not
	// ctx: a startup span in ctx must not parent every later refresh.
	j.life, j.cancel = context.WithCancel(context.Background()) //nolint:contextcheck // see above
	j.wg.Go(func() { j.run(next) })                             //nolint:contextcheck // runs until Close, on j.life (see above)
	return j, nil
}

// Issuer implements Verifier.
func (j *JWKS) Issuer() string { return j.check.issuer }

// Verify implements Verifier.
func (j *JWKS) Verify(ctx context.Context, token string) (*Principal, error) {
	return j.check.verify(ctx, token, j)
}

// Close stops background refreshing and waits for it (bounded by ctx).
// Verify fails with ErrAuthUnavailable afterwards. Close is idempotent.
func (j *JWKS) Close(ctx context.Context) error {
	// Under mu, so no refresh starts (wg.Add) once Close waits.
	j.mu.Lock()
	j.closed.Store(true)
	j.mu.Unlock()
	j.cancel()
	done := make(chan struct{})
	go func() { j.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (j *JWKS) key(ctx context.Context, kid string, alg jwa.SignatureAlgorithm) (any, error) {
	if j.closed.Load() {
		return nil, fmt.Errorf("%w: verifier closed", errKeySourceUnavailable)
	}
	if k, ok := j.keys.Load().find(kid, alg); ok {
		return k, nil
	}
	// Unknown kid: maybe the IdP rotated its keys.
	call := j.startRefresh(true) //nolint:contextcheck // shared refresh on j.life; this caller only waits for it, bounded by ctx
	if call == nil {
		if !j.healthy.Load() {
			// The last refresh failed: the key may well exist.
			return nil, fmt.Errorf("%w: key set refresh failing, unknown key id %q", errKeySourceUnavailable, kid)
		}
		return nil, fmt.Errorf("auth: unknown key id %q", kid)
	}
	select {
	case <-call.done:
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w", errKeySourceUnavailable, context.Cause(ctx))
	}
	if call.err != nil {
		return nil, fmt.Errorf("%w: %w", errKeySourceUnavailable, call.err)
	}
	if k, ok := j.keys.Load().find(kid, alg); ok {
		return k, nil
	}
	return nil, fmt.Errorf("auth: unknown key id %q", kid)
}

// startRefresh returns the running refresh, or starts one. A refresh for an
// unknown kid (miss) starts at most once per MissRefresh; nil means it was
// rate-limited.
func (j *JWKS) startRefresh(miss bool) *refreshCall {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.inflight != nil {
		return j.inflight
	}
	if j.closed.Load() {
		return nil
	}
	if miss {
		now := time.Now()
		if !j.lastForced.IsZero() && now.Sub(j.lastForced) < j.missRefresh {
			return nil
		}
		j.lastForced = now
	}
	c := &refreshCall{done: make(chan struct{})}
	j.inflight = c
	j.wg.Go(func() {
		// Detached from whoever asked: a cancelled request must not abort a
		// refresh others are waiting for.
		ctx, cancel := context.WithTimeout(j.life, fetchTimeout)
		defer cancel()
		idx, next, err := j.fetch(ctx)
		if err == nil {
			j.keys.Store(idx)
		}
		j.healthy.Store(err == nil)
		c.err, c.next = err, next
		j.mu.Lock()
		j.inflight = nil
		j.mu.Unlock()
		close(c.done)
	})
	return c
}

// run refreshes the key set on schedule until Close.
func (j *JWKS) run(next time.Duration) {
	timer := time.NewTimer(next)
	defer timer.Stop()
	failures := 0
	for {
		select {
		case <-j.life.Done():
			return
		case <-timer.C:
		}
		c := j.startRefresh(false)
		if c == nil {
			return // closed
		}
		select {
		case <-j.life.Done():
			return
		case <-c.done:
		}
		if c.err != nil {
			failures++
			next = timex.Backoff(retryBase, j.minR, failures)
			if !errors.Is(c.err, context.Canceled) {
				j.log.Warn("auth: jwks refresh failed, keeping the last key set",
					log.String("url", redact(j.url)), log.Int("attempt", failures),
					log.Duration("retry_in", next), log.Err(c.err))
			}
		} else {
			failures = 0
			next = c.next
		}
		timer.Reset(next)
	}
}

// redact drops credentials and query values from u for logs and errors.
func redact(u string) string {
	p, err := url.Parse(u)
	if err != nil {
		return "(invalid URL)"
	}
	return httpx.RedactURL(p)
}

// maxJWKSBody caps a JWKS response; real key sets are a few KiB.
const maxJWKSBody = 1 << 20

// fetch downloads and indexes the key set, and says when to refresh it.
func (j *JWKS) fetch(ctx context.Context) (*keyIndex, time.Duration, error) {
	res, err := j.client.Get(j.url).Header("Accept", "application/json").Send(ctx)
	if err != nil {
		return nil, 0, err
	}
	body := res.Body()
	defer func() { _ = body.Close() }()
	b, err := io.ReadAll(io.LimitReader(body, maxJWKSBody+1))
	switch {
	case err != nil:
		return nil, 0, fmt.Errorf("read: %w", err)
	case len(b) > maxJWKSBody:
		return nil, 0, fmt.Errorf("key set larger than %d bytes", maxJWKSBody)
	}
	set, err := jwk.Parse(b)
	if err != nil {
		// One key jwx refuses (RSA under 2048 bits, an unknown kty) must not
		// poison the whole set: keep the others, but never replace a good set
		// with an empty one.
		lenient, lerr := jwk.Parse(b, jwk.WithIgnoreParseError(true))
		if lerr != nil || lenient.Len() == 0 {
			return nil, 0, fmt.Errorf("parse: %w", err)
		}
		j.log.WarnContext(ctx, "auth: jwks keys ignored", log.String("url", redact(j.url)), log.Err(err))
		set = lenient
	}
	// A set that parsed cleanly replaces the old one even when it is empty:
	// an IdP may withdraw every key on purpose (revocation).
	return j.index(set), j.nextRefresh(res.Header().Get("Cache-Control")), nil
}

// nextRefresh is max-age clamped to [minR, maxR], or minR without it.
func (j *JWKS) nextRefresh(cacheControl string) time.Duration {
	if cacheControl == "" {
		return j.minR
	}
	dir, err := httpcc.ParseResponse(cacheControl)
	if err != nil {
		return j.minR
	}
	age, ok := dir.MaxAge()
	if !ok {
		return j.minR
	}
	d := j.maxR
	if age < 1<<32 {
		d = time.Duration(age) * time.Second //nolint:gosec // G115: age < 2^32, no overflow
	}
	return min(max(d, j.minR), j.maxR)
}

// index keeps the keys that may verify a signature: public (a published
// private key is reduced to its public part), not symmetric, not use=enc,
// and RSA keys of at least 2048 bits.
func (j *JWKS) index(set jwk.Set) *keyIndex {
	idx := &keyIndex{byKID: make(map[string][]jwk.Key)}
	for i := range set.Len() {
		k, _ := set.Key(i)
		kid, _ := k.KeyID()
		if reason := unusable(k); reason != "" {
			j.log.Warn("auth: jwks key ignored", log.String("kid", kid), log.String("reason", reason))
			continue
		}
		if pub, err := k.PublicKey(); err == nil {
			k = pub
		}
		idx.all = append(idx.all, k)
		if kid != "" {
			idx.byKID[kid] = append(idx.byKID[kid], k)
		}
	}
	return idx
}

func unusable(k jwk.Key) string {
	if k.KeyType().String() == "oct" {
		return "symmetric key in a public key set"
	}
	if use, ok := k.KeyUsage(); ok && use != "sig" {
		return "use is not sig"
	}
	if ops, ok := k.KeyOps(); ok && len(ops) > 0 && !slices.Contains(ops, jwk.KeyOpVerify) {
		return "key_ops lacks verify"
	}
	var raw rsa.PublicKey
	if k.KeyType().String() == "RSA" {
		if pub, err := k.PublicKey(); err != nil || jwk.Export(pub, &raw) != nil || raw.N == nil {
			return "invalid RSA key"
		}
		if raw.N.BitLen() < minRSABits {
			return fmt.Sprintf("RSA key shorter than %d bits", minRSABits)
		}
	}
	return ""
}

// find returns the key for kid that accepts alg. Without kid, a set of
// exactly one key is used. Several keys may share a kid (one per algorithm).
func (x *keyIndex) find(kid string, alg jwa.SignatureAlgorithm) (jwk.Key, bool) {
	cands := x.byKID[kid]
	if kid == "" {
		if len(x.all) != 1 {
			return nil, false
		}
		cands = x.all
	}
	for _, k := range cands {
		if ka, has := k.Algorithm(); has && ka.String() != alg.String() {
			continue
		}
		if !keyTypeFits(k, alg) {
			continue
		}
		return k, true
	}
	return nil, false
}

// keyTypeFits checks the algorithm family against the key type, so a kid
// shared by an RSA and an EC key picks the right one.
func keyTypeFits(k jwk.Key, alg jwa.SignatureAlgorithm) bool {
	name, kty := alg.String(), k.KeyType().String()
	switch {
	case strings.HasPrefix(name, "RS"), strings.HasPrefix(name, "PS"):
		return kty == "RSA"
	case strings.HasPrefix(name, "ES"):
		return kty == "EC"
	case name == "EdDSA" || name == "Ed25519":
		return kty == "OKP"
	}
	return false
}
