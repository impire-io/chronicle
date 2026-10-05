package bridge

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

// defaultOAuthBase serves GitHub's device flow and token grants.
const defaultOAuthBase = "https://github.com"

// GitHub runs the device flow against one GitHub App — the client side of
// signing in (decision 0041: the device flow needs no client secret). Token
// validation and the web flow's code exchange are the install's, not here.
type GitHub struct {
	// ClientID is the GitHub App's client ID, from the install profile.
	ClientID string
	// OAuthBase defaults to github.com.
	OAuthBase string
	// HTTPClient defaults to a 10s-timeout client.
	HTTPClient *http.Client
}

// GitHubOf is the profile's GitHub: its App, and its endpoint when it is
// not github.com's.
func GitHubOf(p Profile) *GitHub {
	return &GitHub{ClientID: p.GithubClientID, OAuthBase: p.GithubOAuthBase}
}

func (g *GitHub) oauth() string {
	if g.OAuthBase != "" {
		return g.OAuthBase
	}
	return defaultOAuthBase
}

func (g *GitHub) http() *http.Client {
	if g.HTTPClient != nil {
		return g.HTTPClient
	}
	return &http.Client{Timeout: 10 * time.Second}
}

// DeviceCode is the device flow's first half: show the user the code and
// the URI, then poll.
type DeviceCode struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

// Token is what the flow, or a refresh, hands back.
type Token struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	// RefreshTokenExpiresIn is the refresh token's life, in seconds.
	RefreshTokenExpiresIn int    `json:"refresh_token_expires_in"`
	Error                 string `json:"error"`
}

// ErrGrantRefused is GitHub refusing a refresh token: expired, already
// used, or not this App's.
var ErrGrantRefused = errors.New("github: grant refused")

// StartDeviceFlow requests a device code for the App.
func (g *GitHub) StartDeviceFlow(ctx context.Context) (DeviceCode, error) {
	var dc DeviceCode
	if err := g.postForm(ctx, g.oauth()+"/login/device/code",
		url.Values{"client_id": {g.ClientID}}, &dc); err != nil {
		return DeviceCode{}, err
	}
	if dc.DeviceCode == "" {
		return DeviceCode{}, errors.New("github: device flow refused (is the client id right, and the flow enabled on the App?)")
	}
	return dc, nil
}

// PollDeviceFlow polls until the user approves, the code expires, or ctx
// ends.
func (g *GitHub) PollDeviceFlow(ctx context.Context, dc DeviceCode) (Token, error) {
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
		if err := g.postForm(ctx, g.oauth()+"/login/oauth/access_token", url.Values{
			"client_id":   {g.ClientID},
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

// Refresh exchanges a refresh token for a fresh pair, with the App's
// client ID alone: GitHub requires the client secret to refresh a user
// token "unless the user access token was generated using the device
// flow" — and the device flow is the only way this package signs in.
func (g *GitHub) Refresh(ctx context.Context, refreshToken string) (Token, error) {
	var tok Token
	if err := g.postForm(ctx, g.oauth()+"/login/oauth/access_token", url.Values{
		"client_id":     {g.ClientID},
		"refresh_token": {refreshToken},
		"grant_type":    {"refresh_token"},
	}, &tok); err != nil {
		return Token{}, err
	}
	if tok.Error != "" {
		return Token{}, fmt.Errorf("%w: refresh: %s", ErrGrantRefused, tok.Error)
	}
	if tok.AccessToken == "" {
		return Token{}, errors.New("github: refresh returned no token")
	}
	return tok, nil
}

func (g *GitHub) postForm(ctx context.Context, endpoint string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := g.http().Do(req)
	if err != nil {
		return fmt.Errorf("github: %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("github: %s: unexpected status %d", endpoint, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
