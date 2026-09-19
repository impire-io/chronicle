// Command chronicle-control is the standalone control plane: control's
// verbs and bridge plus one chronicle-workloads instance, standing over a
// substrate the operator runs — the hosted environment's control unit
// (chronicle-hq/02-DESIGN/09-hosted-environment.md § the stand-up
// ceremony). `chronicle up` composes the same plane over its embedded
// server; both roots go through internal/fleet.
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
