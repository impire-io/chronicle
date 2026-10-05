package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/bridge"
)

// The install profile on this machine (decisions 0038, 0043): a site
// publishes its profile at a well-known address, `login` fetches it there,
// and it is cached beside the contexts for every sentence after the login.
// The profile is public; what it needs is integrity — it names where a
// GitHub token is sent — so it is only read over HTTPS from the site's own
// host.

// profileHTTP is the client profile fetches go through.
var profileHTTP = &http.Client{Timeout: 10 * time.Second}

// SetProfileHTTPClient swaps the client install profiles are fetched with
// and returns the undo — for a build or a test that serves a profile over
// TLS its own client trusts.
func SetProfileHTTPClient(c *http.Client) (restore func()) {
	prev := profileHTTP
	profileHTTP = c
	return func() { profileHTTP = prev }
}

// siteOf is the install's site: the flag, else CHRONICLE_SITE, else the
// hosted install.
func siteOf(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if v := os.Getenv("CHRONICLE_SITE"); v != "" {
		return v
	}
	return bridge.DefaultSite
}

// cachedProfilePath is where a site's fetched profile is kept: beside the
// contexts, named for the site's host. A host never holds "_", so a port's
// ":" is stored as one and read back unambiguously.
func cachedProfilePath(site *url.URL) (string, error) {
	root, err := ConfigRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "profiles", strings.ReplaceAll(site.Host, ":", "_")+".json"), nil
}

// siteOfCachedProfile is the site a cached profile was fetched from; ok is
// false for a profile that is a handed-out file.
func siteOfCachedProfile(path string) (*url.URL, bool) {
	root, err := ConfigRoot()
	if err != nil || filepath.Dir(path) != filepath.Join(root, "profiles") || !strings.HasSuffix(path, ".json") || strings.HasSuffix(path, ".login.json") {
		return nil, false
	}
	host := strings.ReplaceAll(strings.TrimSuffix(filepath.Base(path), ".json"), "_", ":")
	return &url.URL{Scheme: "https", Host: host}, true
}

func readProfile(path string) (bridge.Profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return bridge.Profile{}, fmt.Errorf("read install profile: %w", err)
	}
	return bridge.DecodeProfile(data, path)
}

// refreshProfile fetches the site's profile and writes it to the cache.
func refreshProfile(ctx context.Context, site *url.URL) (string, bridge.Profile, error) {
	path, err := cachedProfilePath(site)
	if err != nil {
		return "", bridge.Profile{}, err
	}
	p, data, err := bridge.FetchProfile(ctx, profileHTTP, site)
	if err != nil {
		return path, p, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return path, p, fmt.Errorf("cache install profile: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return path, p, fmt.Errorf("cache install profile: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return path, p, fmt.Errorf("cache install profile: %w", err)
	}
	return path, p, nil
}

// resolveProfile is login's profile: fetched fresh from the site, or —
// when the site cannot be reached — the copy cached at the last fetch,
// said so on warn.
func resolveProfile(ctx context.Context, site *url.URL, warn io.Writer) (string, bridge.Profile, error) {
	path, p, err := refreshProfile(ctx, site)
	if err == nil {
		return path, p, nil
	}
	if path == "" {
		return "", p, err
	}
	cached, cerr := readProfile(path)
	if cerr != nil {
		return "", p, err
	}
	fmt.Fprintf(warn, "warning: %v — using the profile cached at %s\n", err, path)
	return path, cached, nil
}

// dialProfile dials through the profile at path. When that profile is a
// cached published one and the server refuses the connection at
// authorization, the profile is refetched once and the dial retried: a
// re-seal or a re-key changes the sentinel, and a cached copy must not
// outlive it. Any other profile, or a second refusal, fails as is.
func dialProfile[T any](path string, dial func(bridge.Profile) (T, error)) (T, bridge.Profile, error) {
	var zero T
	p, err := readProfile(path)
	if err != nil {
		return zero, p, err
	}
	conn, err := dial(p)
	if err == nil || !errors.Is(err, nats.ErrAuthorization) {
		return conn, p, err
	}
	site, ok := siteOfCachedProfile(path)
	if !ok {
		return zero, p, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, fresh, ferr := refreshProfile(ctx, site); ferr == nil {
		p = fresh
		conn, err = dial(p)
	}
	return conn, p, err
}

// loginHint is the command that renews a login through this profile.
func loginHint(profilePath string) string {
	site, ok := siteOfCachedProfile(profilePath)
	if !ok {
		return "chronicle login --bridge " + profilePath
	}
	if site.Scheme+"://"+site.Host == bridge.DefaultSite {
		return "chronicle login"
	}
	return "chronicle login --site " + site.Scheme + "://" + site.Host
}
