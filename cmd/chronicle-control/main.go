// Command chronicle-control is one control instance: control's verbs and
// its bridge, standing over a substrate the operator runs, holding one
// secret — its bundle — and reading everything else from the AUTH bucket
// (chronicle-hq/02-DESIGN/10-custody.md; 09-hosted-environment.md § the
// stand-up ceremony). The workload service is its own binary (0029) and
// the executors their own units. `chronicle up` composes the same control
// over its embedded server; both roots go through internal/fleet.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/impire-io/chronicle/internal/fleet"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := fleet.RunControl(ctx, os.Args[1:], os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "chronicle-control:", err)
		os.Exit(1)
	}
}
