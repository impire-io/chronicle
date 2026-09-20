// Command chronicle-node runs one tenant's node: fold, state buckets, and
// the CHRON.API.> verbs, connected to the tenant's account on the
// operator's own NATS — with a creds file, or an nkey seed. The first
// run of a fresh account seeds the registry with its admin (--admin);
// after that, membership is the registry's.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/internal/version"
	"github.com/impire-io/chronicle/node"
	"github.com/impire-io/chronicle/registry"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chronicle-node:", err)
		os.Exit(1)
	}
}

func run() error {
	url := flag.String("url", "nats://127.0.0.1:4222", "NATS url")
	creds := flag.String("creds", "", "the tenant's service-user credentials (.creds)")
	nkey := flag.String("nkey", "", "the tenant's service user as an nkey seed file (instead of --creds)")
	admin := flag.String("admin", "", "seed the registry with this admin principal when the account has none yet")
	flag.Parse()
	if (*creds == "") == (*nkey == "") {
		return fmt.Errorf("exactly one of --creds and --nkey is required")
	}

	var (
		c   *client.Client
		err error
	)
	if *creds != "" {
		c, err = client.ConnectFile(*url, *creds)
	} else {
		c, err = client.ConnectNkeyFile(*url, *nkey, "chronicle-node")
	}
	if err != nil {
		return err
	}
	defer c.Close()

	startCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if *admin != "" {
		js, err := jetstream.New(c.Conn())
		if err != nil {
			return fmt.Errorf("jetstream: %w", err)
		}
		if _, err := registry.Seed(startCtx, js, *admin); err != nil {
			return err
		}
	}
	n, err := node.Start(startCtx, c.Conn(), node.Config{})
	if err != nil {
		return err
	}
	defer n.Stop()

	fmt.Printf("chronicle-node %s serving on %s (Ctrl-C to stop)\n", version.Version, *url)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return nil
}
