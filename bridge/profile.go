// Package bridge is the client half of the browser identity bridge
// (chronicle-hq decisions 0026, 0035, 0043): the install profile a site
// publishes, the connect token that carries a GitHub token through the
// sentinel to the install's auth callout, the identity plane a signed-in
// identity lands in before it has an account or when it has several, and
// GitHub's device flow that produces the token. It is the open contract's
// share of the bridge (11-the-two-forms.md § the line): subjects and shapes
// an open client speaks, answered only where an install runs a bridge. The
// callout, custody and minting are the managed service's and are not here.
package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// DefaultSite is the hosted install's site (decision 0043): where
// `chronicle login` fetches the install profile when no other site is
// named.
const DefaultSite = "https://chronicle.impire.dev"

// WellKnownPath is where a site serves its install profile (decision
// 0038).
const WellKnownPath = "/.well-known/chronicle/profile.json"

// maxProfileBytes bounds a fetched profile: a few short fields and a creds
// file fit many times over.
const maxProfileBytes = 64 << 10

// Profile is the hand-out that makes a bridge login possible (decision
// 0026): where to connect, which GitHub App to sign in with, and the
// sentinel creds that trigger the callout. Public material only: the
// sentinel is public by design, worthless without a valid identity behind
// it.
type Profile struct {
	URL            string `json:"url"`
	GithubClientID string `json:"github_client_id"`
	Sentinel       string `json:"sentinel"`
	// GithubAPIBase and GithubOAuthBase name GitHub's endpoints when they
	// are not github.com's — an install against its own GitHub, and tests
	// against a fake. Absent means github.com.
	GithubAPIBase   string `json:"github_api_base,omitempty"`
	GithubOAuthBase string `json:"github_oauth_base,omitempty"`
	// WebsocketURL is where a browser dials the same install; absent means
	// the install has no listener for a browser.
	WebsocketURL string `json:"websocket_url,omitempty"`
	// GithubTokenURL is where a browser trades the web flow's code, and a
	// refresh token, for GitHub tokens (decision 0041); absent means the
	// install has no web-flow sign-in.
	GithubTokenURL string `json:"github_token_url,omitempty"`
}

// ParseSite accepts an https origin and nothing else — no path, no plain
// http: the certificate is the profile's integrity, and the profile names
// where a GitHub token is sent.
func ParseSite(site string) (*url.URL, error) {
	u, err := url.Parse(site)
	if err != nil {
		return nil, fmt.Errorf("site %q: %w", site, err)
	}
	if u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("site %q: the install profile is only read over https", site)
	}
	if strings.Trim(u.Path, "/") != "" || u.RawQuery != "" {
		return nil, fmt.Errorf("site %q: name the site's origin, not a path", site)
	}
	return u, nil
}

// FetchProfile reads a site's install profile with hc, returning it and
// the bytes as served. A redirect to any other host is refused: the
// profile is the site's or nobody's.
func FetchProfile(ctx context.Context, hc *http.Client, site *url.URL) (Profile, []byte, error) {
	var p Profile
	addr := site.Scheme + "://" + site.Host + WellKnownPath
	c := *hc
	c.CheckRedirect = func(req *http.Request, _ []*http.Request) error {
		if req.URL.Scheme != "https" || req.URL.Host != site.Host {
			return fmt.Errorf("redirected off %s to %s", site.Host, req.URL.Redacted())
		}
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr, nil)
	if err != nil {
		return p, nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return p, nil, fmt.Errorf("fetch install profile %s: %w", addr, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return p, nil, fmt.Errorf("fetch install profile %s: %s", addr, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxProfileBytes+1))
	if err != nil {
		return p, nil, fmt.Errorf("fetch install profile %s: %w", addr, err)
	}
	if len(data) > maxProfileBytes {
		return p, nil, fmt.Errorf("install profile %s is larger than %d bytes", addr, maxProfileBytes)
	}
	if p, err = DecodeProfile(data, addr); err != nil {
		return p, nil, err
	}
	return p, data, nil
}

// DecodeProfile reads a profile and requires what a login needs; origin
// names where it came from, for the error.
func DecodeProfile(data []byte, origin string) (Profile, error) {
	var p Profile
	if err := json.Unmarshal(data, &p); err != nil {
		return p, fmt.Errorf("decode install profile %s: %w", origin, err)
	}
	if p.URL == "" || p.GithubClientID == "" || p.Sentinel == "" {
		return p, fmt.Errorf("install profile %s is incomplete", origin)
	}
	return p, nil
}
