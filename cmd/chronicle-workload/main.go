// Command chronicle-workload is the scheduled form of the fleet's
// workload kinds (chronicle-hq/02-DESIGN/06-scheduler.md § the workload
// contract): the one binary a placement runs, whatever the backend.
// Backend zero never execs it — it starts the same kinds in-process — but
// every real backend boots exactly this, with the two channels the
// contract names: non-secret boot configuration as flags, the service
// creds as a mounted file. A URL naming the msb-gateway host is resolved
// against the guest's own routing table at boot.
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
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/guestnet"
	"github.com/impire-io/chronicle/internal/index/graph"
	"github.com/impire-io/chronicle/internal/index/search"
	"github.com/impire-io/chronicle/internal/node"
	"github.com/impire-io/chronicle/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chronicle-workload:", err)
		os.Exit(1)
	}
}

func run() error {
	kind := flag.String("kind", "", "workload kind: node or index-search (required)")
	logName := flag.String("log", "", "the indexed log (index kinds only)")
	index := flag.String("index", "", "the index name (index kinds only)")
	url := flag.String("url", "nats://127.0.0.1:4222", "NATS url; host msb-gateway resolves to the guest's default gateway")
	creds := flag.String("creds", "", "the tenant's service-user credentials (required)")
	flag.Parse()
	if *creds == "" {
		return fmt.Errorf("--creds is required")
	}

	resolved, err := guestnet.ResolveURL(*url)
	if err != nil {
		return err
	}
	c, err := client.ConnectFile(resolved, *creds)
	if err != nil {
		return err
	}
	defer c.Close()

	startCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var stopWorkload func()
	switch *kind {
	case contract.WorkloadKindNode:
		n, err := node.Start(startCtx, c.Conn(), node.Config{})
		if err != nil {
			return err
		}
		stopWorkload = n.Stop
	case contract.WorkloadKindIndexSearch:
		if *logName == "" || *index == "" {
			return fmt.Errorf("kind %s needs --log and --index", *kind)
		}
		svc, err := search.Start(startCtx, c.Conn(), search.Config{Log: *logName, Index: *index})
		if err != nil {
			return err
		}
		stopWorkload = svc.Stop
	case contract.WorkloadKindIndexGraph:
		if *logName == "" || *index == "" {
			return fmt.Errorf("kind %s needs --log and --index", *kind)
		}
		svc, err := graph.Start(startCtx, c.Conn(), graph.Config{Log: *logName, Index: *index})
		if err != nil {
			return err
		}
		stopWorkload = svc.Stop
	default:
		return fmt.Errorf("kind %q is not in this build's vocabulary (node, index-search)", *kind)
	}
	defer stopWorkload()

	fmt.Printf("chronicle-workload %s serving kind %s on %s (SIGTERM to stop)\n", version.Version, *kind, resolved)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return nil
}
