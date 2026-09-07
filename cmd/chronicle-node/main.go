// Command chronicle-node runs one tenant's node: fold, state buckets, and
// the CHRON.API.> verbs, connected as that tenant's service user only.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/internal/node"
	"github.com/impire-io/chronicle/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chronicle-node:", err)
		os.Exit(1)
	}
}

func run() error {
	url := flag.String("url", "nats://127.0.0.1:4222", "NATS url")
	creds := flag.String("creds", "", "the tenant's service-user credentials (required)")
	flag.Parse()
	if *creds == "" {
		return fmt.Errorf("--creds is required")
	}

	c, err := client.ConnectFile(*url, *creds)
	if err != nil {
		return err
	}
	defer c.Close()

	startCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
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
