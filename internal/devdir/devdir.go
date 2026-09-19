// Package devdir declares the data-dir conventions of `chronicle up`: the
// files the CLI needs to find a running local fleet. It is the one seam
// the adapters share with the bootstrap, and it imports nothing.
package devdir

import (
	"fmt"
	"os"
	"path/filepath"
)

// File names inside the data dir that the CLI reads.
const (
	// ClientURLFile records the running server's client URL.
	ClientURLFile = "client.url"
	// BundlesDir holds the credentials bundles the root issued, one
	// directory per instance (design 10 § instances).
	BundlesDir = "bundles"
	// CLIBundle is the instance name of the operator's CLI: a fleet-template
	// user `up` issues for it.
	CLIBundle = "cli"
	// ControlCredsFile is the CONTROL-account credentials file inside a
	// bundle.
	ControlCredsFile = "control.creds"
)

// Default is the local-development data dir: ~/.chronicle/dev.
func Default() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".chronicle/dev"
	}
	return filepath.Join(home, ".chronicle", "dev")
}

// ClientURLPath is where the running fleet records its client URL.
func ClientURLPath(dir string) string { return filepath.Join(dir, ClientURLFile) }

// ControlCredsPath is where the CLI finds its control-plane credentials:
// the CLI's own bundle, a fleet-template user that reaches every control
// verb and never the AUTH bucket.
func ControlCredsPath(dir string) string {
	return filepath.Join(dir, BundlesDir, CLIBundle, ControlCredsFile)
}

// ReadClientURL reads the recorded client URL — the CLI's way back to a
// running `chronicle up`.
func ReadClientURL(dir string) (string, error) {
	p, err := os.ReadFile(ClientURLPath(dir))
	if err != nil {
		return "", fmt.Errorf("read client url in %s (is `chronicle up` running?): %w", dir, err)
	}
	return string(p), nil
}
