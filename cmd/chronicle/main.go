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
		// The service's step of the clustered rotation — or, with --dir
		// alone, the dev shape around it.
		err = fleet.RotateSigningKey(ctx, os.Args[3:], os.Stdout)
	case len(os.Args) > 2 && os.Args[1] == "operator" && os.Args[2] == "seal":
		// Design 10's first boot, the service's half: the environment's
		// seeds become the AUTH bucket and the first instance.
		err = fleet.Seal(ctx, os.Args[3:], os.Stdout)
	case len(os.Args) > 2 && os.Args[1] == "operator" && os.Args[2] == "export":
		// … and refresh the disaster-recovery export.
		err = fleet.Export(ctx, os.Args[3:], os.Stdout)
	case len(os.Args) > 2 && os.Args[1] == "operator" && os.Args[2] == "instance":
		// … and grow or shrink the fleet by one instance over the bucket.
		err = fleet.Instance(ctx, os.Args[3:], os.Stdout)
	default:
		err = cli.Run(ctx, os.Args[1:], os.Stdout)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "chronicle:", err)
		os.Exit(1)
	}
}
