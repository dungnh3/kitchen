// Package jwtauth verifies JWT access tokens issued by an external OAuth 2.1
// authorization server, for use as mcpkit.Auth.Verifier.
//
// It checks the signature against the issuer's JWKS (cached, refreshed on
// expiry and on an unknown key id, rate limited), the algorithm allow-list,
// iss, aud (the MCP server's resource URL, RFC 8707) and exp/nbf/iat.
package jwtauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/modelcontextprotocol/go-sdk/auth"
)

// Config configures a [Verifier].
type Config struct {
	// Issuer is the exact expected "iss" claim. Required.
	Issuer string
	// Audience is the expected "aud" value: the MCP server's canonical
	// resource URL, e.g. "https://mcp.example.com/mcp". Required.
	Audience string
	// JWKSURL is the issuer's key set. If empty it is discovered from the
	// issuer's authorization server metadata on first use.
	JWKSURL string
	// Algorithms allowed for signatures. Default: RS256, PS256, ES256, EdDSA.
	Algorithms []jose.SignatureAlgorithm
	// HTTPClient fetches metadata and keys. Default: a client with a 10s timeout.
	HTTPClient *http.Client
	// CacheTTL is how long fetched keys are trusted before a refresh.
	// Default: 1h.
	CacheTTL time.Duration
	// MinRefreshInterval limits refreshes triggered by unknown key ids.
	// Default: 1m.
	MinRefreshInterval time.Duration
	// Leeway tolerates clock skew on exp, nbf and iat. Default: 30s.
	Leeway time.Duration
}

// Verifier verifies JWT bearer tokens. It is safe for concurrent use and
// starts no goroutines.
type Verifier struct {
	cfg Config
	now func() time.Time

	mu          sync.Mutex
	jwksURL     string
	keys        *jose.JSONWebKeySet
	fetchedAt   time.Time
	lastAttempt time.Time
}

var errKeysUnavailable = errors.New("jwtauth: signing keys unavailable")

// New validates cfg and returns a Verifier. It performs no I/O.
func New(cfg Config) (*Verifier, error) {
	if cfg.Issuer == "" || cfg.Audience == "" {
		return nil, errors.New("jwtauth: Issuer and Audience are required")
	}
	if len(cfg.Algorithms) == 0 {
		cfg.Algorithms = []jose.SignatureAlgorithm{jose.RS256, jose.PS256, jose.ES256, jose.EdDSA}
	}
	for _, a := range cfg.Algorithms {
		if strings.HasPrefix(string(a), "HS") || a == "none" {
			return nil, fmt.Errorf("jwtauth: algorithm %s is not allowed for access tokens", a)
		}
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = time.Hour
	}
	if cfg.MinRefreshInterval <= 0 {
		cfg.MinRefreshInterval = time.Minute
	}
	if cfg.Leeway < 0 {
		return nil, errors.New("jwtauth: Leeway must not be negative")
	}
	if cfg.Leeway == 0 {
		cfg.Leeway = 30 * time.Second
	}
	return &Verifier{cfg: cfg, now: time.Now, jwksURL: cfg.JWKSURL}, nil
}

// Verify implements auth.TokenVerifier: pass v.Verify as mcpkit.Auth.Verifier.
// Invalid tokens yield an error wrapping auth.ErrInvalidToken (HTTP 401);
// key-fetch failures yield another error (HTTP 500).
func (v *Verifier) Verify(ctx context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
	tok, err := jwt.ParseSigned(token, v.cfg.Algorithms)
	if err != nil {
		return nil, invalid(err)
	}
	if len(tok.Headers) != 1 {
		return nil, invalid(errors.New("expected exactly one signature"))
	}
	if typ, _ := tok.Headers[0].ExtraHeaders[jose.HeaderType].(string); typ != "" &&
		!strings.EqualFold(typ, "at+jwt") && !strings.EqualFold(typ, "application/at+jwt") && !strings.EqualFold(typ, "JWT") {
		return nil, invalid(fmt.Errorf("unexpected token type %q", typ))
	}
	key, err := v.key(ctx, tok.Headers[0].KeyID)
	if err != nil {
		return nil, err
	}

	var (
		std    jwt.Claims
		scopes struct {
			Scope string   `json:"scope"`
			Scp   []string `json:"scp"`
		}
		all map[string]any
	)
	if err := tok.Claims(key, &std, &scopes, &all); err != nil {
		return nil, invalid(err)
	}
	if std.Expiry == nil {
		return nil, invalid(errors.New("missing exp"))
	}
	err = std.ValidateWithLeeway(jwt.Expected{
		Issuer:      v.cfg.Issuer,
		AnyAudience: jwt.Audience{v.cfg.Audience},
		Time:        v.now(),
	}, v.cfg.Leeway)
	if err != nil {
		return nil, invalid(err)
	}

	sc := scopes.Scp
	if scopes.Scope != "" {
		sc = append(strings.Fields(scopes.Scope), sc...)
	}
	return &auth.TokenInfo{
		Scopes:     sc,
		Expiration: std.Expiry.Time(),
		UserID:     std.Subject,
		Extra:      all,
	}, nil
}

func invalid(err error) error { return fmt.Errorf("%w: %w", auth.ErrInvalidToken, err) }

// key returns the verification key for kid, refreshing the key set when it
// is stale or does not contain kid (at most once per MinRefreshInterval).
func (v *Verifier) key(ctx context.Context, kid string) (*jose.JSONWebKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	now := v.now()
	if v.keys != nil {
		k := find(v.keys, kid)
		fresh := now.Sub(v.fetchedAt) < v.cfg.CacheTTL
		if k != nil && fresh {
			return k, nil
		}
		// Rate-limit refreshes (successful or not) so that tokens with
		// random key ids or an unreachable issuer cannot cause a fetch storm.
		if now.Sub(v.lastAttempt) < v.cfg.MinRefreshInterval {
			if k != nil {
				return k, nil
			}
			return nil, invalid(fmt.Errorf("unknown key id %q", kid))
		}
	}
	v.lastAttempt = now
	if err := v.refresh(ctx); err != nil {
		// Keep serving cached keys if the issuer is briefly unreachable.
		if v.keys != nil {
			if k := find(v.keys, kid); k != nil {
				return k, nil
			}
		}
		return nil, fmt.Errorf("%w: %w", errKeysUnavailable, err)
	}
	if k := find(v.keys, kid); k != nil {
		return k, nil
	}
	return nil, invalid(fmt.Errorf("unknown key id %q", kid))
}

func find(set *jose.JSONWebKeySet, kid string) *jose.JSONWebKey {
	var candidates []jose.JSONWebKey
	if kid != "" {
		candidates = set.Key(kid)
	} else {
		candidates = set.Keys
	}
	var match *jose.JSONWebKey
	n := 0
	for i := range candidates {
		k := &candidates[i]
		if k.Use == "enc" || !k.IsPublic() {
			continue
		}
		match = k
		n++
	}
	if kid == "" && n != 1 {
		return nil // ambiguous without a key id
	}
	return match
}

func (v *Verifier) refresh(ctx context.Context) error {
	if v.jwksURL == "" {
		asm, err := auth.GetAuthServerMetadata(ctx, v.cfg.Issuer, v.cfg.HTTPClient)
		if err != nil {
			return err
		}
		if asm == nil || asm.JWKSURI == "" {
			return errors.New("issuer metadata has no jwks_uri")
		}
		v.jwksURL = asm.JWKSURI
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := v.cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks: unexpected status %d", resp.StatusCode)
	}
	var set jose.JSONWebKeySet
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&set); err != nil {
		return fmt.Errorf("jwks: %w", err)
	}
	v.keys, v.fetchedAt = &set, v.now()
	return nil
}
