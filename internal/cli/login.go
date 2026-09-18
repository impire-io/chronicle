package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/identity/github"
)

// `chronicle login` and the --bridge dial (decision 0026): the device flow
// against the install's bridge profile, the refresh token stored beside
// it, and client verbs connecting through the bridge instead of a creds
// file. The profile is public material; the login state is the user's
// secret and sits 0600.

// loginState is what a completed login keeps: enough to connect until the
// refresh token dies, at which point login runs again.
type loginState struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	Expiry       time.Time `json:"expiry,omitzero"`
}

func loginStatePath(profilePath string) string {
	return filepath.Join(filepath.Dir(profilePath), "login.json")
}

func defaultProfilePath(dir string) string { return filepath.Join(dir, "bridge.json") }

func readProfile(path string) (contract.BridgeProfile, error) {
	var p contract.BridgeProfile
	data, err := os.ReadFile(path)
	if err != nil {
		return p, fmt.Errorf("read bridge profile: %w (an install with the bridge enabled writes one; ask for it like you would a creds file)", err)
	}
	if err := json.Unmarshal(data, &p); err != nil {
		return p, fmt.Errorf("decode bridge profile: %w", err)
	}
	if p.URL == "" || p.GithubClientID == "" || p.Sentinel == "" {
		return p, fmt.Errorf("bridge profile %s is incomplete", path)
	}
	return p, nil
}

func login(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle login", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "local fleet data dir")
	profilePath := fs.String("bridge", "", "bridge profile (default <dir>/bridge.json)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("login takes no positionals")
	}
	path := *profilePath
	if path == "" {
		path = defaultProfilePath(*dir)
	}
	p, err := readProfile(path)
	if err != nil {
		return err
	}

	gh := &github.Client{ClientID: p.GithubClientID}
	dc, err := gh.StartDeviceFlow(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "visit  %s\nenter  %s\nwaiting for approval...\n", dc.VerificationURI, dc.UserCode)
	tok, err := gh.PollDeviceFlow(ctx, dc)
	if err != nil {
		return err
	}
	if err := saveLoginState(path, tok); err != nil {
		return err
	}
	fmt.Fprintf(out, "logged in; verbs take --bridge %s --tenant <tenant> from here\n", path)
	return nil
}

func saveLoginState(profilePath string, tok github.Token) error {
	st := loginState{AccessToken: tok.AccessToken, RefreshToken: tok.RefreshToken}
	if tok.ExpiresIn > 0 {
		st.Expiry = time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second)
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(loginStatePath(profilePath), data, 0o600); err != nil {
		return fmt.Errorf("store login: %w", err)
	}
	return nil
}

// bridgeDial connects a client verb through the bridge, refreshing the
// stored token when it is stale.
func bridgeDial(profilePath, tenant string) (*client.Client, error) {
	p, err := readProfile(profilePath)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(loginStatePath(profilePath))
	if err != nil {
		return nil, fmt.Errorf("no login next to %s — run: chronicle login --bridge %s", profilePath, profilePath)
	}
	var st loginState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("decode login state: %w", err)
	}
	if !st.Expiry.IsZero() && time.Until(st.Expiry) < time.Minute {
		if st.RefreshToken == "" {
			return nil, fmt.Errorf("github token expired — run: chronicle login --bridge %s", profilePath)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		gh := &github.Client{ClientID: p.GithubClientID}
		tok, err := gh.Refresh(ctx, st.RefreshToken)
		if err != nil {
			return nil, fmt.Errorf("refresh failed — run: chronicle login --bridge %s (%w)", profilePath, err)
		}
		if err := saveLoginState(profilePath, tok); err != nil {
			return nil, err
		}
		st.AccessToken = tok.AccessToken
	}
	return client.ConnectBridge(p.URL, []byte(p.Sentinel), tenant, st.AccessToken)
}
