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

// storedContext is one context record: contexts/<name>.json.
type storedContext struct {
	URL   string `json:"url,omitempty"`
	Creds string `json:"creds"`
	Log   string `json:"log,omitempty"`
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

// The teaching errors (0025): a refusal names its fixes.
var (
	errNoContext = errors.New("no context selected — chronicle context save <name> --creds F, then chronicle context select <name>")
	errNoCreds   = errors.New("no credentials — select a context (chronicle context select <name>) or pass --creds")
	errNoLog     = errors.New("no log selected — chronicle log select <log>, or --log")
)
