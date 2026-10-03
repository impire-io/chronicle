// Package devdir declares the data-dir conventions of the open quick start
// (`chronicle up`): the files the CLI needs to find a running local server
// and to be someone on it. It is the one seam the adapters share with the
// composition, and it imports nothing.
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
	// WebsocketURLFile records the running server's websocket URL, when
	// `chronicle up` was asked for the listener; absent otherwise.
	WebsocketURLFile = "websocket.url"
	// UserNkeyFile is the seed of the one user the quick start's account
	// has — the operator's own identity on their own machine.
	UserNkeyFile = "user.nk"
)

// LocalPrincipal is the member ID the quick start's user acts as: the
// registry's first admin, seeded when the dir is born.
const LocalPrincipal = "admin"

// Default is the local-development data dir: ~/.chronicle/dev.
func Default() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".chronicle/dev"
	}
	return filepath.Join(home, ".chronicle", "dev")
}

// ClientURLPath is where the running server records its client URL.
func ClientURLPath(dir string) string { return filepath.Join(dir, ClientURLFile) }

// WebsocketURLPath is where the running server records its websocket URL.
func WebsocketURLPath(dir string) string { return filepath.Join(dir, WebsocketURLFile) }

// UserNkeyPath is where the quick start keeps its user's seed.
func UserNkeyPath(dir string) string { return filepath.Join(dir, UserNkeyFile) }

// ReadClientURL reads the recorded client URL — the CLI's way back to a
// running `chronicle up`.
func ReadClientURL(dir string) (string, error) {
	p, err := os.ReadFile(ClientURLPath(dir))
	if err != nil {
		return "", fmt.Errorf("read client url in %s (is `chronicle up` running?): %w", dir, err)
	}
	return string(p), nil
}
