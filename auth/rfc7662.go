package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/httpx"
)

// RFC7662Config configures an RFC 7662 token introspection client.
type RFC7662Config struct {
	Endpoint     string // required, e.g. https://idp.example.com/oauth2/introspect
	ClientID     string // HTTP Basic credentials of this resource server
	ClientSecret string
	// Issuer, when set, must equal the response's iss.
	Issuer string
	// Audience, when set, must be in the response's aud (RFC 7662 section
	// 4: the resource server checks the token is meant for it). A response
	// without aud is then rejected.
	Audience string
	// Client sends the requests. Default: an httpx client (kitchen/httpx:
	// traced and metered, bounded error bodies, safe redirects) with a 5s
	// timeout. No retry: NewIntrospection already caches and coalesces calls.
	Client *httpx.Client
}

type rfc7662Response struct {
	Active    bool            `json:"active"`
	Scope     string          `json:"scope"`
	ClientID  string          `json:"client_id"`
	Username  string          `json:"username"`
	TokenType string          `json:"token_type"`
	Exp       int64           `json:"exp"`
	Iat       int64           `json:"iat"`
	Nbf       int64           `json:"nbf"`
	Sub       string          `json:"sub"`
	Aud       json.RawMessage `json:"aud"`
	Iss       string          `json:"iss"`
	Jti       string          `json:"jti"`
}

// RFC7662 returns an IntrospectFunc that POSTs the token to an RFC 7662
// endpoint. Use it with NewIntrospection for caching.
func RFC7662(cfg RFC7662Config) IntrospectFunc {
	client, clientErr := cfg.Client, error(nil)
	if client == nil {
		client, clientErr = httpx.New(httpx.WithBaseURL(cfg.Endpoint), httpx.WithTimeout(5*time.Second))
	}
	var basic string
	if cfg.ClientID != "" {
		basic = "Basic " + base64.StdEncoding.EncodeToString([]byte(url.QueryEscape(cfg.ClientID)+":"+url.QueryEscape(cfg.ClientSecret)))
	}
	return func(ctx context.Context, token string) (*Principal, error) {
		switch {
		case cfg.Endpoint == "":
			return nil, errors.New("auth: rfc7662: endpoint is required")
		case clientErr != nil:
			return nil, fmt.Errorf("auth: rfc7662: %w", clientErr)
		}
		req := client.Post(cfg.Endpoint).
			Form(url.Values{"token": {token}, "token_type_hint": {"access_token"}}).
			Header("Accept", "application/json")
		if basic != "" {
			req = req.Header("Authorization", basic)
		}
		var r rfc7662Response
		if err := req.Do(ctx, &r); err != nil {
			// The authority did not answer: never ErrTokenInvalid, whatever
			// the response said.
			return nil, errs.Wrap(fmt.Errorf("auth: rfc7662: %w", err), ErrAuthUnavailable)
		}
		aud := audience(r.Aud)
		now := time.Now()
		switch {
		case !r.Active:
			return nil, ErrTokenInvalid
		case cfg.Issuer != "" && r.Iss != cfg.Issuer:
			return nil, ErrTokenInvalid
		case r.Exp != 0 && !now.Before(time.Unix(r.Exp, 0)):
			return nil, ErrTokenExpired
		case r.Nbf != 0 && now.Before(time.Unix(r.Nbf, 0)):
			return nil, ErrTokenInvalid
		case cfg.Audience != "" && !slices.Contains(aud, cfg.Audience):
			return nil, ErrTokenInvalid
		}
		p := &Principal{
			Subject:  r.Sub,
			Issuer:   r.Iss,
			Audience: aud,
			Scopes:   strings.Fields(r.Scope),
			TokenID:  r.Jti,
			Claims: map[string]any{
				"client_id": r.ClientID, "username": r.Username, "token_type": r.TokenType,
			},
		}
		if r.Exp != 0 {
			p.ExpiresAt = time.Unix(r.Exp, 0)
		}
		if r.Iat != 0 {
			p.IssuedAt = time.Unix(r.Iat, 0)
		}
		return p, nil
	}
}

// audience decodes aud as a string or an array of strings.
func audience(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var one string
	if json.Unmarshal(raw, &one) == nil {
		if one == "" {
			return nil
		}
		return []string{one}
	}
	var many []string
	_ = json.Unmarshal(raw, &many)
	return many
}
