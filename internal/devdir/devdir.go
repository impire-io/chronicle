// Package devdir declares the data-dir conventions of `chronicle up`: the
// two files the CLI needs to find a running local fleet. It is the one
// seam the adapters share with the bootstrap, and it imports nothing.
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
	// ControlCredsFile is the control-plane credentials file.
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

// ControlCredsPath is where the CLI finds control-plane credentials.
func ControlCredsPath(dir string) string { return filepath.Join(dir, ControlCredsFile) }

// ReadClientURL reads the recorded client URL — the CLI's way back to a
// running `chronicle up`.
func ReadClientURL(dir string) (string, error) {
	p, err := os.ReadFile(ClientURLPath(dir))
	if err != nil {
		return "", fmt.Errorf("read client url in %s (is `chronicle up` running?): %w", dir, err)
	}
	return string(p), nil
}
