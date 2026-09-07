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
	if len(os.Args) > 1 && os.Args[1] == "up" {
		err = fleet.Run(ctx, os.Args[2:], os.Stdout)
	} else {
		err = cli.Run(ctx, os.Args[1:], os.Stdout)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "chronicle:", err)
		os.Exit(1)
	}
}
