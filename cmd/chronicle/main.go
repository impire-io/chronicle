// Command chronicle is the open CLI: the tenant sentences, and — through
// `chronicle up` — the quick start in one process. The managed service
// builds the same binary with its verbs added (11-the-two-forms.md § the
// managed CLI).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/impire-io/chronicle/cli"
	"github.com/impire-io/chronicle/up"
)

const upUsage = `run locally
  chronicle up [--dir D] [--port N]                      an embedded server, one account, the node, its indexers — a quick start, not production
      production is your own NATS: chronicle-node and chronicle-workload as your own processes

`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := cli.RunWith(ctx, os.Args[1:], os.Stdout, &cli.Extension{
		Verbs: map[string]cli.Verb{"up": up.Run},
		Usage: upUsage,
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "chronicle:", err)
		os.Exit(1)
	}
}
