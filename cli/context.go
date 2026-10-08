package cli

// The context store: chronicle's own records under the user config dir.
// A context says where commands go — a connection, one way of being
// someone, and the selected store. The store is CLI-layer only; the client
// keeps taking (url, credential).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/impire-io/chronicle/devdir"
)

// storedContext is one context record: contexts/<name>.json — the
// connection, one way of being someone (a credential file, an nkey seed
// with the principal stated, or a bridge profile with the account the
// sign-in lands in), and the selected store.
type storedContext struct {
	URL       string `json:"url,omitempty"`
	Creds     string `json:"creds,omitempty"`
	Nkey      string `json:"nkey,omitempty"`
	Principal string `json:"principal,omitempty"`
	Bridge    string `json:"bridge,omitempty"`
	Account   string `json:"account,omitempty"`
	Store     string `json:"store,omitempty"`
}

var contextNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// LocalContextName is the context `chronicle up` saves for its one user.
const LocalContextName = "local"

// ConfigRoot is the CLI's config directory, where saved contexts live:
// CHRONICLE_CONFIG_HOME when set (tests and scripts isolate), the user
// config dir otherwise. Exported so a build that adds verbs keeps its own
// files beside the contexts (chronicle-hq decision 0038).
func ConfigRoot() (string, error) {
	if v := os.Getenv("CHRONICLE_CONFIG_HOME"); v != "" {
		return v, nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("user config dir: %w", err)
	}
	return filepath.Join(base, "chronicle"), nil
}

func contextFile(root, name string) string {
	return filepath.Join(root, "contexts", name+".json")
}

func currentFile(root string) string {
	return filepath.Join(root, "current")
}

func validContextName(name string) error {
	if !contextNameRe.MatchString(name) {
		return fmt.Errorf("context name %q: letters, digits, '.', '_' and '-' only", name)
	}
	return nil
}

func loadStoredContext(root, name string) (storedContext, error) {
	raw, err := os.ReadFile(contextFile(root, name))
	if errors.Is(err, fs.ErrNotExist) {
		return storedContext{}, fmt.Errorf("context %q does not exist (chronicle context add %s --creds FILE, or chronicle login)", name, name)
	}
	if err != nil {
		return storedContext{}, fmt.Errorf("read context %s: %w", name, err)
	}
	var sc storedContext
	if err := json.Unmarshal(raw, &sc); err != nil {
		return storedContext{}, fmt.Errorf("decode context %s: %w", name, err)
	}
	return sc, nil
}

func saveStoredContext(root, name string, sc storedContext) error {
	if err := validContextName(name); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(root, "contexts"), 0o700); err != nil {
		return fmt.Errorf("create context store: %w", err)
	}
	raw, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(contextFile(root, name), raw, 0o600); err != nil {
		return fmt.Errorf("write context %s: %w", name, err)
	}
	return nil
}

// currentContextName is the selection: CHRONICLE_CONTEXT beats the
// store's current file; empty means nothing selected.
func currentContextName(root string) string {
	if v := os.Getenv("CHRONICLE_CONTEXT"); v != "" {
		return v
	}
	raw, err := os.ReadFile(currentFile(root))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func selectStoredContext(root, name string) error {
	if _, err := loadStoredContext(root, name); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create context store: %w", err)
	}
	if err := os.WriteFile(currentFile(root), []byte(name+"\n"), 0o600); err != nil {
		return fmt.Errorf("select context %s: %w", name, err)
	}
	return nil
}

func listStoredContexts(root string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(root, "contexts"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list contexts: %w", err)
	}
	var names []string
	for _, e := range entries {
		if name, ok := strings.CutSuffix(e.Name(), ".json"); ok && !e.IsDir() {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func removeStoredContext(root, name string) error {
	if err := os.Remove(contextFile(root, name)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("context %q does not exist", name)
		}
		return fmt.Errorf("remove context %s: %w", name, err)
	}
	// A selection naming a removed context would dangle — clear it.
	if raw, err := os.ReadFile(currentFile(root)); err == nil && strings.TrimSpace(string(raw)) == name {
		_ = os.Remove(currentFile(root))
	}
	return nil
}

// updateSelectedStore records the selected store on the named context —
// the selection lives and dies with the context that can reach it.
func updateSelectedStore(root, name, store string) error {
	sc, err := loadStoredContext(root, name)
	if err != nil {
		return err
	}
	sc.Store = store
	return saveStoredContext(root, name, sc)
}

// Context is a stored context as a build sees it: the connection, one way
// of being someone, the selected store. It is the extension's way to end
// onboarding connected after issuing a credential of its own — the
// managed build's account and member verbs save and select the context
// for the credential they mint, and its login saves the bridge profile
// with the account it landed in (0035).
type Context struct {
	URL       string
	Creds     string
	Nkey      string
	Principal string
	Bridge    string
	Account   string
	Store     string
}

// SaveContext stores a context under the user config dir, field-wise
// over what the name already holds: an empty field keeps the stored one,
// and one way of being someone replaces the others.
func SaveContext(name string, c Context) error {
	root, err := ConfigRoot()
	if err != nil {
		return err
	}
	sc, _ := loadStoredContext(root, name)
	if c.URL != "" {
		sc.URL = c.URL
	}
	if c.Creds != "" || c.Nkey != "" || c.Bridge != "" {
		sc.Creds, sc.Nkey, sc.Principal, sc.Bridge, sc.Account = c.Creds, c.Nkey, c.Principal, c.Bridge, c.Account
	}
	if c.Store != "" {
		sc.Store = c.Store
	}
	return saveStoredContext(root, name, sc)
}

// LoadContext reads a stored context as a build sees it — the named one,
// or the selection when the name is empty (CHRONICLE_CONTEXT beats the
// store's current file); ok is false when nothing is selected.
func LoadContext(name string) (Context, bool, error) {
	root, err := ConfigRoot()
	if err != nil {
		return Context{}, false, err
	}
	if name == "" {
		name = currentContextName(root)
	}
	if name == "" {
		return Context{}, false, nil
	}
	sc, err := loadStoredContext(root, name)
	if err != nil {
		return Context{}, false, err
	}
	return Context(sc), true, nil
}

// SelectContext makes a saved context the selection.
func SelectContext(name string) error {
	root, err := ConfigRoot()
	if err != nil {
		return err
	}
	return selectStoredContext(root, name)
}

// SaveLocalContext is what `chronicle up` does for the quick start
// (decision 0045 § 5): save the context `local` — the embedded server's
// url and its one user — and select it when nothing is selected, so the
// README's second command works. A `local` context saved by an earlier
// run is refreshed, never a different one touched.
func SaveLocalContext(url, dir string) error {
	root, err := ConfigRoot()
	if err != nil {
		return err
	}
	sc, _ := loadStoredContext(root, LocalContextName)
	sc.URL, sc.Nkey, sc.Principal, sc.Creds, sc.Bridge, sc.Account = url, devdir.UserNkeyPath(dir), devdir.LocalPrincipal, "", "", ""
	if err := saveStoredContext(root, LocalContextName, sc); err != nil {
		return err
	}
	if currentContextName(root) == "" {
		return selectStoredContext(root, LocalContextName)
	}
	return nil
}

// The teaching errors: a refusal names its fixes.
var (
	errNoContext   = errors.New("no context selected — chronicle context add NAME --creds FILE, then chronicle context select NAME; or chronicle login")
	errNoCreds     = errors.New("no credential — select a context (chronicle context select NAME), sign in (chronicle login), or run chronicle up")
	errNoPrincipal = errors.New("no principal — an nkey seed carries no name: pass --principal, or save it on the context")
	errNoStore     = errors.New("no store selected — chronicle store select NAME, or --store NAME")
)

func (x *runner) contextNoun() *noun {
	return &noun{
		name:    "context",
		summary: "where commands go: a connection, who you are on it, and the selected store",
		verbs: []command{
			{"list", "the saved contexts; * marks the selection", func(_ context.Context, args []string, out io.Writer) error { return contextList(args, out) }},
			{"show [NAME]", "one context in full (default: the selection)", func(_ context.Context, args []string, out io.Writer) error { return contextShow(args, out) }},
			{"select NAME", "make a saved context the selection", func(_ context.Context, args []string, out io.Writer) error { return contextSelect(args, out) }},
			{"add NAME (--creds FILE | --nkey FILE --principal NAME) [--url URL]", "save a context for a credential on any NATS", func(_ context.Context, args []string, out io.Writer) error { return contextAdd(args, out) }},
			{"remove NAME", "forget a saved context", func(_ context.Context, args []string, out io.Writer) error { return contextRemove(args, out) }},
		},
	}
}

func contextList(args []string, out io.Writer) error {
	fs := flags("context list", "list", "the saved contexts; * marks the selection", out)
	format := outputFlag(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 0, "no arguments"); err != nil {
		return err
	}
	if err := checkFormat(*format); err != nil {
		return err
	}
	root, err := ConfigRoot()
	if err != nil {
		return err
	}
	names, err := listStoredContexts(root)
	if err != nil {
		return err
	}
	current := currentContextName(root)
	var rows [][]string
	var items []any
	for _, name := range names {
		sc, err := loadStoredContext(root, name)
		if err != nil {
			return err
		}
		marker := ""
		if name == current {
			marker = "*"
		}
		rows = append(rows, []string{marker, name, sc.URL, whoOf(sc), sc.Store})
		items = append(items, map[string]any{"name": name, "selected": name == current, "url": sc.URL, "as": whoOf(sc), "store": sc.Store})
	}
	return printList(out, *format, []string{"", "NAME", "URL", "AS", "STORE"}, rows, items)
}

// whoOf says who a context speaks as, in one cell.
func whoOf(sc storedContext) string {
	switch {
	case sc.Nkey != "":
		return sc.Principal
	case sc.Bridge != "":
		return "GitHub sign-in (" + sc.Account + ")"
	case sc.Creds != "":
		return filepath.Base(sc.Creds)
	}
	return ""
}

func contextShow(args []string, out io.Writer) error {
	fs := flags("context show", "show [NAME]", "one context in full (default: the selection)", out)
	format := outputFlag(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if len(pos) > 1 {
		return positionals(fs, pos, 1, "at most one context name")
	}
	if err := checkFormat(*format); err != nil {
		return err
	}
	root, err := ConfigRoot()
	if err != nil {
		return err
	}
	name := currentContextName(root)
	if len(pos) == 1 {
		name = pos[0]
	}
	if name == "" {
		return errNoContext
	}
	sc, err := loadStoredContext(root, name)
	if err != nil {
		return err
	}
	return printValue(out, *format, func() {
		fmt.Fprintf(out, "context:   %s\n", name)
		if sc.URL != "" {
			fmt.Fprintf(out, "url:       %s\n", sc.URL)
		} else {
			fmt.Fprintf(out, "url:       (the quick start's recorded url)\n")
		}
		switch {
		case sc.Nkey != "":
			fmt.Fprintf(out, "nkey:      %s\n", sc.Nkey)
			fmt.Fprintf(out, "as:        %s\n", sc.Principal)
		case sc.Bridge != "":
			fmt.Fprintf(out, "sign-in:   GitHub, through %s\n", sc.Bridge)
			fmt.Fprintf(out, "account:   %s\n", sc.Account)
		default:
			fmt.Fprintf(out, "creds:     %s\n", sc.Creds)
		}
		if sc.Store != "" {
			fmt.Fprintf(out, "store:     %s\n", sc.Store)
		} else {
			fmt.Fprintf(out, "store:     (none selected — chronicle store select NAME)\n")
		}
	}, map[string]any{"name": name, "url": sc.URL, "creds": sc.Creds, "nkey": sc.Nkey, "principal": sc.Principal, "bridge": sc.Bridge, "account": sc.Account, "store": sc.Store})
}

func contextSelect(args []string, out io.Writer) error {
	fs := flags("context select", "select NAME", "make a saved context the selection", out)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one context name"); err != nil {
		return err
	}
	root, err := ConfigRoot()
	if err != nil {
		return err
	}
	if err := selectStoredContext(root, pos[0]); err != nil {
		return err
	}
	sc, err := loadStoredContext(root, pos[0])
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "context %s selected", pos[0])
	if sc.Store != "" {
		fmt.Fprintf(out, "; store %s", sc.Store)
	}
	fmt.Fprintln(out)
	return nil
}

func contextAdd(args []string, out io.Writer) error {
	fs := flags("context add", "add NAME (--creds FILE | --nkey FILE --principal NAME) [--url URL]", "save a context for a credential on any NATS", out)
	creds := fs.String("creds", "", "a credential file; the principal is the one it names")
	nkey := fs.String("nkey", "", "an nkey seed — a user on your own NATS; needs --principal")
	principal := fs.String("principal", "", "your principal, for --nkey")
	bridge := fs.String("bridge", "", "an install profile handed out as a file, to sign in through (chronicle login does this for you); needs --account")
	account := fs.String("account", "", "the account a --bridge sign-in lands in")
	url := fs.String("url", "", "the NATS url (default: the quick start's recorded url, at use)")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one context name"); err != nil {
		return err
	}
	ways := 0
	for _, w := range []string{*creds, *nkey, *bridge} {
		if w != "" {
			ways++
		}
	}
	switch {
	case ways == 0:
		fs.Usage()
		return fmt.Errorf("%w: --creds FILE, or --nkey FILE --principal NAME", ErrUsage)
	case ways > 1:
		return fmt.Errorf("--creds, --nkey and --bridge are three ways to be someone; pick one")
	case *nkey != "" && *principal == "":
		return fmt.Errorf("--nkey needs --principal (the seed carries no name)")
	case *nkey == "" && *principal != "":
		return fmt.Errorf("a credential file names its principal; --principal goes with --nkey")
	case *bridge != "" && *account == "":
		return fmt.Errorf("--bridge needs --account (the account the sign-in lands in)")
	case *bridge == "" && *account != "":
		return fmt.Errorf("--account goes with --bridge")
	}
	root, err := ConfigRoot()
	if err != nil {
		return err
	}
	// A re-add is field-wise: the url and the selected store survive
	// unless replaced — a new credential swaps who, not where.
	sc, _ := loadStoredContext(root, pos[0])
	switch {
	case *creds != "":
		abs, err := filepath.Abs(*creds)
		if err != nil {
			return err
		}
		sc.Creds, sc.Nkey, sc.Principal, sc.Bridge, sc.Account = abs, "", "", "", ""
	case *nkey != "":
		abs, err := filepath.Abs(*nkey)
		if err != nil {
			return err
		}
		sc.Creds, sc.Nkey, sc.Principal, sc.Bridge, sc.Account = "", abs, *principal, "", ""
	default:
		abs, err := filepath.Abs(*bridge)
		if err != nil {
			return err
		}
		sc.Creds, sc.Nkey, sc.Principal, sc.Bridge, sc.Account = "", "", "", abs, *account
	}
	if *url != "" {
		sc.URL = *url
	}
	if err := saveStoredContext(root, pos[0], sc); err != nil {
		return err
	}
	fmt.Fprintf(out, "context %s added\n", pos[0])
	if currentContextName(root) == "" {
		if err := selectStoredContext(root, pos[0]); err == nil {
			fmt.Fprintf(out, "context %s selected\n", pos[0])
		}
	} else if currentContextName(root) != pos[0] {
		fmt.Fprintf(out, "select it: chronicle context select %s\n", pos[0])
	}
	return nil
}

func contextRemove(args []string, out io.Writer) error {
	fs := flags("context remove", "remove NAME", "forget a saved context", out)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one context name"); err != nil {
		return err
	}
	root, err := ConfigRoot()
	if err != nil {
		return err
	}
	if err := removeStoredContext(root, pos[0]); err != nil {
		return err
	}
	fmt.Fprintf(out, "context %s removed\n", pos[0])
	return nil
}
