package cli

// The context store (0025): chronicle's own records under the user config
// dir. A context names a connection and the working log; the store is
// CLI-layer only — the client keeps taking (url, creds).

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// storedContext is one context record: contexts/<name>.json — the
// connection, one way of being someone (a creds file, an nkey seed with
// the principal stated, or a bridge profile with the account the login
// lands in), and the working log.
type storedContext struct {
	URL       string `json:"url,omitempty"`
	Creds     string `json:"creds,omitempty"`
	Nkey      string `json:"nkey,omitempty"`
	Principal string `json:"principal,omitempty"`
	Bridge    string `json:"bridge,omitempty"`
	Account   string `json:"account,omitempty"`
	Log       string `json:"log,omitempty"`
}

var contextName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// configRoot is the store's directory: CHRONICLE_CONFIG_HOME when set
// (tests and scripts isolate), the user config dir otherwise.
func configRoot() (string, error) {
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
	if !contextName.MatchString(name) {
		return fmt.Errorf("context name %q: letters, digits, '.', '_' and '-' only", name)
	}
	return nil
}

func loadStoredContext(root, name string) (storedContext, error) {
	raw, err := os.ReadFile(contextFile(root, name))
	if errors.Is(err, fs.ErrNotExist) {
		return storedContext{}, fmt.Errorf("context %q is not saved (chronicle context save %s --creds F)", name, name)
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
			return fmt.Errorf("context %q is not saved", name)
		}
		return fmt.Errorf("remove context %s: %w", name, err)
	}
	// A selection naming a removed context would dangle — clear it.
	if raw, err := os.ReadFile(currentFile(root)); err == nil && strings.TrimSpace(string(raw)) == name {
		_ = os.Remove(currentFile(root))
	}
	return nil
}

// updateSelectedLog records the working log on the named context — the
// selection lives and dies with the context that can reach it (0025).
func updateSelectedLog(root, name, log string) error {
	sc, err := loadStoredContext(root, name)
	if err != nil {
		return err
	}
	sc.Log = log
	return saveStoredContext(root, name, sc)
}

// Context is a stored context as a build sees it: the connection, one way
// of being someone, the working log. It is the extension's way to end
// onboarding connected (0025 § 1) after issuing a credential of its own —
// the managed build's account and member verbs save and select the
// context for the creds they mint, and its login saves the bridge
// profile with the account it landed in (0035).
type Context struct {
	URL       string
	Creds     string
	Nkey      string
	Principal string
	Bridge    string
	Account   string
	Log       string
}

// SaveContext stores a context under the user config dir, field-wise
// over what the name already holds: an empty field keeps the stored one,
// and one way of being someone replaces the others.
func SaveContext(name string, c Context) error {
	root, err := configRoot()
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
	if c.Log != "" {
		sc.Log = c.Log
	}
	return saveStoredContext(root, name, sc)
}

// SelectContext makes a saved context the selection.
func SelectContext(name string) error {
	root, err := configRoot()
	if err != nil {
		return err
	}
	return selectStoredContext(root, name)
}

// The teaching errors (0025): a refusal names its fixes.
var (
	errNoContext   = errors.New("no context selected — chronicle context save <name> --creds F, then chronicle context select <name>")
	errNoCreds     = errors.New("no credentials — select a context (chronicle context select <name>), pass --creds or --nkey, or run chronicle up")
	errNoPrincipal = errors.New("no principal — an nkey seed carries no name: pass --principal, or save it on the context")
	errNoLog       = errors.New("no log selected — chronicle log select <log>, or --log")
)
