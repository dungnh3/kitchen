package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// RFC7662Config configures an RFC 7662 token introspection client.
type RFC7662Config struct {
	Endpoint     string // required, e.g. https://idp.example.com/oauth2/introspect
	ClientID     string // HTTP Basic credentials of this resource server
	ClientSecret string
	// Issuer, when set, must equal the response's iss.
	Issuer string
	// HTTPClient defaults to a client with a 5s timeout.
	HTTPClient *http.Client
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
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return func(ctx context.Context, token string) (*Principal, error) {
		if cfg.Endpoint == "" {
			return nil, errors.New("auth: rfc7662: endpoint is required")
		}
		form := url.Values{"token": {token}, "token_type_hint": {"access_token"}}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Endpoint, strings.NewReader(form.Encode()))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")
		if cfg.ClientID != "" {
			req.SetBasicAuth(url.QueryEscape(cfg.ClientID), url.QueryEscape(cfg.ClientSecret))
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("auth: rfc7662: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			return nil, fmt.Errorf("auth: rfc7662: status %d", resp.StatusCode)
		}
		var r rfc7662Response
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&r); err != nil {
			return nil, fmt.Errorf("auth: rfc7662: decode: %w", err)
		}
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
		}
		p := &Principal{
			Subject:  r.Sub,
			Issuer:   r.Iss,
			Audience: audience(r.Aud),
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
