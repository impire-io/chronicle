// Package github is the bridge's one identity seam (decision 0026): token
// validation for the callout responder, and the device flow for
// `chronicle login`. Endpoints are configurable so tests run against local
// fakes — the NATS half of the bridge is tested against the real embedded
// server, the GitHub half against a fake; both halves honest.
package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// DefaultAPIBase serves REST calls — token validation.
	DefaultAPIBase = "https://api.github.com"
	// DefaultOAuthBase serves the device flow.
	DefaultOAuthBase = "https://github.com"
	// tokenExpiryHeader carries the expiring-token deadline on API
	// responses, "2006-01-02 15:04:05 MST".
	tokenExpiryHeader = "GitHub-Authentication-Token-Expiration"
)

// Identity is who a validated token belongs to. The numeric ID is the
// stable key; the login is display material.
type Identity struct {
	ID    int64
	Login string
	// TokenExpiry is when GitHub retires the token, zero when GitHub does
	// not say. The bridge caps its placements at min(TTL, this).
	TokenExpiry time.Time
}

// TokenValidator answers who holds a token. The bridge depends on this
// interface, never on the concrete client.
type TokenValidator interface {
	ValidateToken(ctx context.Context, token string) (Identity, error)
}

// ErrInvalidToken is every validation refusal — the bridge maps it to its
// one uniform authentication error.
var ErrInvalidToken = errors.New("github: token not valid")

// Client validates tokens and runs the device flow against one GitHub App.
type Client struct {
	// ClientID is the GitHub App's client ID — install configuration.
	ClientID string
	// APIBase and OAuthBase default to github.com's endpoints.
	APIBase   string
	OAuthBase string
	// HTTPClient defaults to a 10s-timeout client.
	HTTPClient *http.Client
}

func (c *Client) api() string   { return orDefault(c.APIBase, DefaultAPIBase) }
func (c *Client) oauth() string { return orDefault(c.OAuthBase, DefaultOAuthBase) }
func (c *Client) http() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func orDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

// ValidateToken asks GitHub who holds the token.
func (c *Client) ValidateToken(ctx context.Context, token string) (Identity, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.api()+"/user", nil)
	if err != nil {
		return Identity{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := c.http().Do(req)
	if err != nil {
		return Identity{}, fmt.Errorf("github: validate: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return Identity{}, ErrInvalidToken
	}
	if resp.StatusCode != http.StatusOK {
		return Identity{}, fmt.Errorf("github: validate: unexpected status %d", resp.StatusCode)
	}
	var body struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Identity{}, fmt.Errorf("github: decode user: %w", err)
	}
	if body.ID == 0 {
		return Identity{}, ErrInvalidToken
	}
	id := Identity{ID: body.ID, Login: body.Login}
	if h := resp.Header.Get(tokenExpiryHeader); h != "" {
		if t, err := time.Parse("2006-01-02 15:04:05 MST", h); err == nil {
			id.TokenExpiry = t
		}
	}
	return id, nil
}

// DeviceCode is the device flow's first half: show the user the code and
// URI, then Poll.
type DeviceCode struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

// Token is what the flow (or a refresh) hands back.
type Token struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	Error        string `json:"error"`
}

// StartDeviceFlow requests a device code for the App.
func (c *Client) StartDeviceFlow(ctx context.Context) (DeviceCode, error) {
	var dc DeviceCode
	if err := c.postForm(ctx, c.oauth()+"/login/device/code",
		url.Values{"client_id": {c.ClientID}}, &dc); err != nil {
		return DeviceCode{}, err
	}
	if dc.DeviceCode == "" {
		return DeviceCode{}, errors.New("github: device flow refused (is the client id right, and the flow enabled on the App?)")
	}
	return dc, nil
}

// PollDeviceFlow polls until the user approves, the code expires, or ctx
// ends.
func (c *Client) PollDeviceFlow(ctx context.Context, dc DeviceCode) (Token, error) {
	interval := time.Duration(max(dc.Interval, 5)) * time.Second
	deadline := time.Now().Add(time.Duration(dc.ExpiresIn) * time.Second)
	for {
		select {
		case <-ctx.Done():
			return Token{}, ctx.Err()
		case <-time.After(interval):
		}
		if time.Now().After(deadline) {
			return Token{}, errors.New("github: device code expired before approval")
		}
		var tok Token
		if err := c.postForm(ctx, c.oauth()+"/login/oauth/access_token", url.Values{
			"client_id":   {c.ClientID},
			"device_code": {dc.DeviceCode},
			"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
		}, &tok); err != nil {
			return Token{}, err
		}
		switch tok.Error {
		case "":
			if tok.AccessToken == "" {
				return Token{}, errors.New("github: empty token grant")
			}
			return tok, nil
		case "authorization_pending":
		case "slow_down":
			interval += 5 * time.Second
		default:
			return Token{}, fmt.Errorf("github: device flow: %s", tok.Error)
		}
	}
}

// Refresh exchanges a refresh token for a fresh pair.
func (c *Client) Refresh(ctx context.Context, refreshToken string) (Token, error) {
	var tok Token
	if err := c.postForm(ctx, c.oauth()+"/login/oauth/access_token", url.Values{
		"client_id":     {c.ClientID},
		"refresh_token": {refreshToken},
		"grant_type":    {"refresh_token"},
	}, &tok); err != nil {
		return Token{}, err
	}
	if tok.Error != "" {
		return Token{}, fmt.Errorf("github: refresh: %s", tok.Error)
	}
	if tok.AccessToken == "" {
		return Token{}, errors.New("github: refresh returned no token")
	}
	return tok, nil
}

func (c *Client) postForm(ctx context.Context, endpoint string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint,
		strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http().Do(req)
	if err != nil {
		return fmt.Errorf("github: %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("github: %s: unexpected status %d", endpoint, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
