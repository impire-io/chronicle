// Command chronicle-workloads is the workload service, standalone: the
// fleet log's only writer, the dispatch surface, the auctions, the level
// scan — n instances, each on its own credential under the workloads
// template (chronicle-hq/03-DECISIONS/0029-the-workload-service-is-its-own-
// binary.md; 02-DESIGN/06-scheduler.md; 10-custody.md § the fence). It is
// thin over internal/workloads and holds nothing but its credential.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/internal/version"
	"github.com/impire-io/chronicle/internal/workloads"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chronicle-workloads:", err)
		os.Exit(1)
	}
}

func run() error {
	url := flag.String("url", "", "the cluster's client url (required)")
	creds := flag.String("creds", "", "this instance's control.creds from `chronicle operator instance add <name> --template workloads` (required)")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("chronicle-workloads takes no positionals")
	}
	if *url == "" || *creds == "" {
		return fmt.Errorf("chronicle-workloads needs --url and --creds")
	}

	nc, err := nats.Connect(*url, nats.UserCredentials(*creds), nats.Name("chronicle-workloads"), nats.Timeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer nc.Close()

	startCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	svc, err := workloads.Start(startCtx, nc, workloads.Config{})
	if err != nil {
		return err
	}
	defer svc.Stop()

	fmt.Printf("chronicle-workloads %s serving on %s (SIGTERM to stop)\n", version.Version, *url)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return nil
}
