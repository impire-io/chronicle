package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/impire-io/chronicle/bridge"
	"github.com/impire-io/chronicle/client"
)

// `chronicle login`, `account create` and the --bridge dial (decisions
// 0026, 0035, 0038 and 0043): GitHub's device flow against the install's
// profile — published at the site's well-known address and cached beside
// the contexts, or a handed-out file — the refresh token stored beside that
// profile, and a login that ends inside an account: the one you hold, a
// choice when you hold several, a fresh one named after your GitHub login
// when you hold none, saved and selected as a context, so the account
// sentences run from there with neither flag. The profile is public; the
// login state is the user's secret and sits 0600.

// loginState is what a completed login keeps: enough to connect until the
// refresh token dies, at which point login runs again.
type loginState struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	Expiry       time.Time `json:"expiry,omitzero"`
}

// loginStatePath is the login kept for one profile, named after it, so two
// installs never share one.
func loginStatePath(profilePath string) string {
	return strings.TrimSuffix(profilePath, filepath.Ext(profilePath)) + ".login.json"
}

// legacyLoginStatePath is where logins were kept before the login state
// was named after its profile: one login.json beside the profile.
func legacyLoginStatePath(profilePath string) string {
	return filepath.Join(filepath.Dir(profilePath), "login.json")
}

// stdinChooser reads the choice from stdin when it is a terminal; nil
// otherwise, and login teaches --account instead of asking.
var stdinChooser = func() io.Reader {
	if fi, err := os.Stdin.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
		return os.Stdin
	}
	return nil
}

func login(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle login", flag.ContinueOnError)
	fs.SetOutput(out)
	siteFlag := fs.String("site", "", "the install's site, whose install profile login fetches (default $CHRONICLE_SITE, else "+bridge.DefaultSite+")")
	profilePath := fs.String("bridge", "", "an install profile handed out as a file, instead of the site's")
	account := fs.String("account", "", "the account to land in (default: the one you hold; a choice when several; a new one named after your login when none)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("login takes no positionals")
	}
	path, p, err := loginProfile(ctx, *profilePath, *siteFlag, out)
	if err != nil {
		return err
	}

	gh := bridge.GitHubOf(p)
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
	return landIn(ctx, out, path, p, tok.AccessToken, *account, stdinChooser())
}

// loginProfile is the profile a login runs against: a handed-out file when
// --bridge names one, the site's published profile otherwise.
func loginProfile(ctx context.Context, bridgeFlag, siteFlag string, warn io.Writer) (string, bridge.Profile, error) {
	if bridgeFlag != "" {
		path, err := filepath.Abs(bridgeFlag)
		if err != nil {
			return "", bridge.Profile{}, err
		}
		p, err := readProfile(path)
		return path, p, err
	}
	site, err := bridge.ParseSite(siteOf(siteFlag))
	if err != nil {
		return "", bridge.Profile{}, err
	}
	return resolveProfile(ctx, site, warn)
}

// landIn ends the login inside an account (07-the-cli.md § self-service):
// the account named, else what the identity plane says — one membership is
// the account, none means create one named after the login, several means
// choose — then a context named <account>-<login>, saved and selected.
func landIn(ctx context.Context, out io.Writer, profilePath string, p bridge.Profile, accessToken, account string, chooser io.Reader) error {
	if account == "" {
		id, err := bridge.ConnectIdentity(p.URL, []byte(p.Sentinel), accessToken)
		if err != nil {
			return err
		}
		defer id.Close()
		ms, err := id.Memberships(ctx)
		if err != nil {
			return err
		}
		switch len(ms.Memberships) {
		case 0:
			fmt.Fprintf(out, "no account yet: creating %s (the %s plan) — its node is placed before this returns\n", id.Login, ms.Plan)
			created, err := id.CreateAccount(ctx, "")
			if err != nil {
				var serr *client.ServiceError
				if errors.As(err, &serr) && serr.Code == "tenant-exists" {
					return fmt.Errorf("%s — then: chronicle login --account <name>", serr.Desc)
				}
				return err
			}
			account = created.Name
		case 1:
			account = ms.Memberships[0].Account
		default:
			account, err = choose(out, ms.Memberships, chooser)
			if err != nil {
				return err
			}
		}
	}

	c, err := bridge.Connect(p.URL, []byte(p.Sentinel), account, accessToken)
	if err != nil {
		return err
	}
	principal := c.Author()
	c.Close()
	name := account + "-" + principal
	if err := SaveContext(name, Context{URL: p.URL, Bridge: profilePath, Account: account}); err != nil {
		return fmt.Errorf("context not saved: %w", err)
	}
	if err := SelectContext(name); err != nil {
		return fmt.Errorf("context %s saved, not selected: %w", name, err)
	}
	fmt.Fprintf(out, "logged in as %s in account %s; context %s saved and selected\n", principal, account, name)
	return nil
}

// choose presents the accounts and reads a choice — a number, or a name —
// from the chooser; without one (no terminal) it teaches --account.
func choose(out io.Writer, ms []bridge.Membership, chooser io.Reader) (string, error) {
	names := make([]string, len(ms))
	for i, m := range ms {
		names[i] = m.Account
	}
	teach := fmt.Errorf("you are a member of several accounts (%s): chronicle login --account <name>", strings.Join(names, ", "))
	if chooser == nil {
		return "", teach
	}
	fmt.Fprintf(out, "you are a member of several accounts:\n")
	for i, m := range ms {
		fmt.Fprintf(out, "  %d) %s  as %s (%s)\n", i+1, m.Account, m.Principal, m.Role)
	}
	fmt.Fprintf(out, "which one? ")
	line, _ := bufio.NewReader(chooser).ReadString('\n')
	line = strings.TrimSpace(line)
	if line == "" {
		// Nothing to read — a terminal that closed, or no terminal after
		// all: the same teaching as having none.
		return "", teach
	}
	if n, err := strconv.Atoi(line); err == nil && n >= 1 && n <= len(ms) {
		return ms[n-1].Account, nil
	}
	for _, name := range names {
		if name == line {
			return name, nil
		}
	}
	return "", fmt.Errorf("%q is none of %s: chronicle login --account <name>", line, strings.Join(names, ", "))
}

func saveLoginState(profilePath string, tok bridge.Token) error {
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

// readLoginState reads the login kept for the profile. A login kept under
// the older shared name is moved to the profile's own name on first read.
func readLoginState(profilePath string) ([]byte, error) {
	path := loginStatePath(profilePath)
	data, err := os.ReadFile(path)
	if !errors.Is(err, os.ErrNotExist) {
		return data, err
	}
	legacy := legacyLoginStatePath(profilePath)
	data, lerr := os.ReadFile(legacy)
	if lerr != nil {
		return nil, err
	}
	if werr := os.WriteFile(path, data, 0o600); werr != nil {
		return nil, fmt.Errorf("move login state: %w", werr)
	}
	if rerr := os.Remove(legacy); rerr != nil {
		return nil, fmt.Errorf("move login state: %w", rerr)
	}
	return data, nil
}

// freshToken reads the stored login beside the profile, refreshing it when
// it is stale.
func freshToken(profilePath string, p bridge.Profile) (string, error) {
	data, err := readLoginState(profilePath)
	if err != nil {
		return "", fmt.Errorf("not logged in through %s — run: %s", profilePath, loginHint(profilePath))
	}
	var st loginState
	if err := json.Unmarshal(data, &st); err != nil {
		return "", fmt.Errorf("decode login state: %w", err)
	}
	if !st.Expiry.IsZero() && time.Until(st.Expiry) < time.Minute {
		if st.RefreshToken == "" {
			return "", fmt.Errorf("github token expired — run: %s", loginHint(profilePath))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		tok, err := bridge.GitHubOf(p).Refresh(ctx, st.RefreshToken)
		if err != nil {
			return "", fmt.Errorf("refresh failed — run: %s (%w)", loginHint(profilePath), err)
		}
		if err := saveLoginState(profilePath, tok); err != nil {
			return "", err
		}
		st.AccessToken = tok.AccessToken
	}
	return st.AccessToken, nil
}

// dialThroughBridge is how an account sentence dials a --bridge identity;
// a test swaps it.
var dialThroughBridge = bridgeDial

// bridgeDial connects an account sentence through the bridge into the
// account, refreshing the stored token when it is stale.
func bridgeDial(profilePath, account string) (*client.Client, error) {
	c, _, err := dialProfile(profilePath, func(p bridge.Profile) (*client.Client, error) {
		token, err := freshToken(profilePath, p)
		if err != nil {
			return nil, err
		}
		return bridge.Connect(p.URL, []byte(p.Sentinel), account, token)
	})
	return c, err
}

// identityDial connects through the bridge into the identity plane — the
// self-service `account create`.
func identityDial(profilePath string) (*bridge.Identity, bridge.Profile, error) {
	return dialProfile(profilePath, func(p bridge.Profile) (*bridge.Identity, error) {
		token, err := freshToken(profilePath, p)
		if err != nil {
			return nil, err
		}
		return bridge.ConnectIdentity(p.URL, []byte(p.Sentinel), token)
	})
}

// accountVerb is the open `account`: `create <name>`, an identity's further
// account of its own through the bridge. A build that mints accounts
// itself (the managed service's operator form) wraps it through
// Extension.Override.
func accountVerb(ctx context.Context, args []string, out io.Writer) error {
	if len(args) >= 1 && args[0] == "create" {
		return accountCreate(ctx, args[1:], out)
	}
	return fmt.Errorf("account: create <name> [--bridge F] — a further account of your own, through the bridge you logged in with")
}

// accountCreate creates an account of the signed-in identity's own through
// the identity plane — the plan says how many — and lands in it. The bridge
// is --bridge, else the selected context's.
func accountCreate(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle account create", flag.ContinueOnError)
	fs.SetOutput(out)
	bridgeFlag := fs.String("bridge", "", "install profile — create the account as the identity logged in there (default: the selected context's)")
	ctxName := fs.String("context", "", "context name (default: CHRONICLE_CONTEXT, else the selection)")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("account create: exactly one account name")
	}
	profile := *bridgeFlag
	if profile == "" {
		if sc, ok, err := LoadContext(*ctxName); err == nil && ok {
			profile = sc.Bridge
		}
	}
	if profile == "" {
		return fmt.Errorf("account create needs a login: chronicle login first, or --bridge F")
	}
	return AccountCreateThroughBridge(ctx, out, profile, pos[0])
}

// AccountCreateThroughBridge asks the identity plane for an account named
// name and lands in it: a context named <account>-<login>, saved and
// selected. It is exported for a build whose own `account create` takes
// this form when a bridge is chosen.
func AccountCreateThroughBridge(ctx context.Context, out io.Writer, profilePath, name string) error {
	abs, err := filepath.Abs(profilePath)
	if err != nil {
		return err
	}
	profilePath = abs
	id, p, err := identityDial(profilePath)
	if err != nil {
		return err
	}
	defer id.Close()
	created, err := id.CreateAccount(ctx, name)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "account %s minted: %s (the %s plan; admin %s, bound to your GitHub identity)\n", created.Name, created.Account, created.Plan, created.Admin)
	token, err := freshToken(profilePath, p)
	if err != nil {
		return err
	}
	c, err := bridge.Connect(p.URL, []byte(p.Sentinel), created.Name, token)
	if err != nil {
		return fmt.Errorf("minted, but the login into it failed: %w", err)
	}
	principal := c.Author()
	c.Close()
	ctxName := created.Name + "-" + principal
	if err := SaveContext(ctxName, Context{URL: p.URL, Bridge: profilePath, Account: created.Name}); err != nil {
		fmt.Fprintf(out, "context not saved: %v\n", err)
		return nil
	}
	if err := SelectContext(ctxName); err != nil {
		fmt.Fprintf(out, "context %s saved, not selected: %v\n", ctxName, err)
		return nil
	}
	fmt.Fprintf(out, "context %s saved and selected\n", ctxName)
	return nil
}
