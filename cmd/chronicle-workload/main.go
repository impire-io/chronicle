// Command chronicle-workload is the placement binary: a node or an index
// kind as one process, whoever starts it (chronicle-hq/02-DESIGN/06-
// scheduler.md § the workload contract). On the operator's own NATS it is
// the unit they run per declared index; in the managed service every real
// backend boots exactly this, with the two channels the contract names:
// non-secret boot configuration as flags, the service creds as a mounted
// file. A URL naming the msb-gateway host is resolved against the guest's
// own routing table at boot, and a TLS URL is verified against the name
// the executor hands it (--tls-server-name), not the gateway address —
// the certificate names the server, never a sandbox's gateway.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/guestnet"
	"github.com/impire-io/chronicle/index/graph"
	"github.com/impire-io/chronicle/index/search"
	"github.com/impire-io/chronicle/index/semantic"
	"github.com/impire-io/chronicle/internal/version"
	"github.com/impire-io/chronicle/node"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chronicle-workload:", err)
		os.Exit(1)
	}
}

func run() error {
	kind := flag.String("kind", "", "workload kind: node, index-search, index-graph, or index-semantic (required)")
	logName := flag.String("log", "", "the indexed log (index kinds only)")
	index := flag.String("index", "", "the index name (index kinds only)")
	url := flag.String("url", "nats://127.0.0.1:4222", "NATS url; host msb-gateway resolves to the guest's default gateway")
	tlsServerName := flag.String("tls-server-name", "", "the name the server's certificate is verified against when the URL's host cannot be named by it, as a guest dialing its gateway; empty means the URL's host")
	creds := flag.String("creds", "", "the tenant's service-user credentials (.creds)")
	nkey := flag.String("nkey", "", "the tenant's service user as an nkey seed file (instead of --creds)")
	embedURL := flag.String("embedding-url", "", "OpenAI-compatible embedding endpoint (semantic kind)")
	embedModel := flag.String("embedding-model", "", "embedding model (semantic kind)")
	embedKeyFile := flag.String("embedding-key-file", "", "file carrying the provider key (semantic kind)")
	flag.Parse()
	if (*creds == "") == (*nkey == "") {
		return fmt.Errorf("exactly one of --creds and --nkey is required")
	}

	resolved, err := guestnet.ResolveURL(*url)
	if err != nil {
		return err
	}
	var c *client.Client
	if *creds != "" {
		c, err = client.ConnectFile(resolved, *creds, client.TLSServerName(*tlsServerName))
	} else {
		c, err = client.ConnectNkeyFile(resolved, *nkey, "chronicle-workload", client.TLSServerName(*tlsServerName))
	}
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
	case contract.WorkloadKindIndexSemantic:
		if *logName == "" || *index == "" {
			return fmt.Errorf("kind %s needs --log and --index", *kind)
		}
		provider := semantic.ProviderConfig{BaseURL: *embedURL, Model: *embedModel}
		if *embedKeyFile != "" {
			key, err := os.ReadFile(*embedKeyFile)
			if err != nil {
				return fmt.Errorf("read embedding key: %w", err)
			}
			provider.APIKey = strings.TrimSpace(string(key))
		}
		svc, err := semantic.Start(startCtx, c.Conn(), semantic.Config{Log: *logName, Index: *index, Provider: provider})
		if err != nil {
			return err
		}
		stopWorkload = svc.Stop
	default:
		return fmt.Errorf("kind %q is not in this build's vocabulary (node, index-search, index-graph, index-semantic)", *kind)
	}
	defer stopWorkload()

	fmt.Printf("chronicle-workload %s serving kind %s on %s (SIGTERM to stop)\n", version.Version, *kind, resolved)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return nil
}
