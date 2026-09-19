// Command chronicle-control runs the cross-account control plane against a
// bootstrap data dir: the minting material lives here and nowhere else.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	dir := flag.String("dir", devdir.Default(), "the root holding this instance's bundle")
	url := flag.String("url", "", "NATS url (default: the root's recorded url)")
	flag.Parse()

	r, err := mint.LoadRoot(*dir)
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
	bundle, err := mint.ReadBundle(r.BundleDir("instance-1"))
	if err != nil {
		return err
	}

	sysConn, err := mint.ConnectCreds(target, bundle.SysCreds, "chronicle-sys")
	if err != nil {
		return fmt.Errorf("connect system user: %w", err)
	}
	defer sysConn.Close()
	ctrlConn, err := mint.ConnectCreds(target, bundle.ControlCreds, "chronicle-control")
	if err != nil {
		return fmt.Errorf("connect control user: %w", err)
	}
	defer ctrlConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	custody, err := mint.OpenCustody(ctx, ctrlConn)
	if err != nil {
		cancel()
		return err
	}
	driver, err := mint.NewJWTDriver(ctx, custody, sysConn, target)
	cancel()
	if err != nil {
		return err
	}

	svc, err := control.Start(ctrlConn, control.Config{Driver: driver, URL: target})
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
