// Command chronicle is the open CLI: the account sentences, and — through
// `chronicle up` — the quick start in one process. The managed service
// builds the same binary with its verbs added (11-the-two-forms.md § the
// managed CLI).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/impire-io/chronicle/cli"
	"github.com/impire-io/chronicle/up"
)

const upUsage = `Run locally:
  up               run chronicle locally on an embedded NATS: one account, one user, the node and its indexes`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := cli.RunWith(ctx, os.Args[1:], os.Stdout, &cli.Extension{
		Verbs: map[string]cli.Verb{"up": runUp},
		Usage: upUsage,
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		if !errors.Is(err, cli.ErrUsage) {
			fmt.Fprintln(os.Stderr, "chronicle:", err)
		}
		os.Exit(1)
	}
}

// runUp is the quick start with the CLI's local context saved once it is
// up, so the README's second command works (decision 0045 § 5).
func runUp(ctx context.Context, args []string, out io.Writer) error {
	return up.RunWith(ctx, args, out, func(l *up.Local, dir string) error {
		return cli.SaveLocalContext(l.URL, dir)
	})
}
