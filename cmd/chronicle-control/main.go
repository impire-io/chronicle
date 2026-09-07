// Command chronicle-control runs the cross-account control plane against a
// bootstrap data dir: the minting material lives here and nowhere else.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/impire-io/chronicle/internal/control"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chronicle-control:", err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", devdir.Default(), "data dir holding the bootstrap material")
	url := flag.String("url", "", "NATS url (default: the data dir's recorded url)")
	flag.Parse()

	b, err := mint.LoadOrInitBootstrap(*dir)
	if err != nil {
		return err
	}
	target := *url
	if target == "" {
		target, err = devdir.ReadClientURL(*dir)
		if err != nil {
			return err
		}
	}

	sysConn, err := mint.ConnectCreds(target, b.SysCreds, "chronicle-sys")
	if err != nil {
		return fmt.Errorf("connect system user: %w", err)
	}
	defer sysConn.Close()
	ctrlConn, err := mint.ConnectCreds(target, b.ControlCreds, "chronicle-control")
	if err != nil {
		return fmt.Errorf("connect control user: %w", err)
	}
	defer ctrlConn.Close()

	svc, err := control.Start(ctrlConn, control.Config{
		Driver: &mint.JWTDriver{
			OperatorSigningSeed: b.OperatorSigningSeed,
			SysConn:             sysConn,
			URL:                 target,
		},
		URL:         target,
		AccountsDir: b.AccountsDir(),
	})
	if err != nil {
		return err
	}
	defer func() { _ = svc.Stop() }()

	fmt.Printf("chronicle-control %s serving on %s (Ctrl-C to stop)\n", version.Version, target)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return nil
}
