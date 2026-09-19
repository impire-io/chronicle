// Command chronicle is the CLI: `up` runs the local fleet composition;
// every other verb is a client on the product surface.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/impire-io/chronicle/internal/cli"
	"github.com/impire-io/chronicle/internal/fleet"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch {
	case len(os.Args) > 1 && os.Args[1] == "up":
		err = fleet.Run(ctx, os.Args[2:], os.Stdout)
	case len(os.Args) > 2 && os.Args[1] == "operator" && os.Args[2] == "rotate-signing-key":
		// The trust-root ceremony is the composition root's, like `up`:
		// it works the fleet dir's custody, not the product surface.
		err = fleet.RotateSigningKey(os.Args[3:], os.Stdout)
	case len(os.Args) > 2 && os.Args[1] == "operator" && os.Args[2] == "emit-cluster-config":
		// The stand-up rendering ceremony works the same custody.
		err = fleet.EmitClusterConfig(os.Args[3:], os.Stdout)
	case len(os.Args) > 2 && os.Args[1] == "operator" && os.Args[2] == "init":
		// Design 10's first boot: birth the offline root.
		err = fleet.InitRoot(os.Args[3:], os.Stdout)
	case len(os.Args) > 2 && os.Args[1] == "operator" && os.Args[2] == "seal":
		// … move the working keys into the AUTH bucket once the cluster serves.
		err = fleet.Seal(ctx, os.Args[3:], os.Stdout)
	case len(os.Args) > 2 && os.Args[1] == "operator" && os.Args[2] == "export":
		// … and refresh the disaster-recovery export.
		err = fleet.Export(ctx, os.Args[3:], os.Stdout)
	default:
		err = cli.Run(ctx, os.Args[1:], os.Stdout)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "chronicle:", err)
		os.Exit(1)
	}
}
