// Command chronicle-executor is the standalone form of the fleet's
// muscle (chronicle-hq/02-DESIGN/06-scheduler.md § the executors): one
// per host, operator-started infrastructure, never itself scheduled. It
// registers on the fleet roster, bids from live local state, and carries
// what it is delegated on this host's one configured backend. Its
// credential is an executor-template user of the control account, issued
// by `chronicle operator instance add <id> --template executor` (design 10
// § the fence, 0032): its own register, report and creds subjects, the
// auction, its own endpoints — and nothing else. The instance name is the
// executor's ID; --id may restate it, never change it.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/impire-io/chronicle/internal/executor"
	"github.com/impire-io/chronicle/internal/index/semantic"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chronicle-executor:", err)
		os.Exit(1)
	}
}

func run() error {
	url := flag.String("url", "nats://127.0.0.1:4222", "NATS url of the fleet's server")
	creds := flag.String("creds", "", "this executor's control.creds from `chronicle operator instance add <id> --template executor` (required)")
	id := flag.String("id", "", "this executor's durable identity — the instance name the credential was issued under (default: read from the credential)")
	backendName := flag.String("backend", "microsandbox", "this host's backend: inprocess or microsandbox")
	workloadBinary := flag.String("workload-binary", "", "linux chronicle-workload (host arch) for the microsandbox backend")
	image := flag.String("image", "", "base image for microsandbox placements (default alpine)")
	embedURL := flag.String("embedding-url", "", "OpenAI-compatible embedding endpoint for the semantic kind (key via CHRONICLE_EMBEDDING_API_KEY)")
	embedModel := flag.String("embedding-model", "", "default embedding model for the semantic kind")
	flag.Parse()
	if *creds == "" {
		return fmt.Errorf("--creds is required")
	}
	credsData, err := os.ReadFile(*creds)
	if err != nil {
		return fmt.Errorf("read creds: %w", err)
	}
	// The credential names the instance it was issued for, and the
	// server lets it speak on that name's subjects only: the ID is not a
	// choice, so it is read from the credential and --id may only agree.
	issuedAs, err := mint.InstanceOf(credsData)
	if err != nil {
		return err
	}
	if *id == "" {
		*id = issuedAs
	} else if *id != issuedAs {
		return fmt.Errorf("--id %s does not match the credential, issued for instance %s", *id, issuedAs)
	}

	nc, err := mint.ConnectCreds(*url, credsData, "chronicle-executor-"+*id)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer nc.Close()

	pull := func(ctx context.Context, tenant, workload string) ([]byte, error) {
		return executor.PullCreds(ctx, nc, *id, tenant, workload)
	}
	var embedding *semantic.ProviderConfig
	if *embedURL != "" && *embedModel != "" {
		embedding = &semantic.ProviderConfig{BaseURL: *embedURL, Model: *embedModel, APIKey: os.Getenv("CHRONICLE_EMBEDDING_API_KEY")}
	}
	var backend executor.Backend
	switch *backendName {
	case "inprocess":
		backend = &executor.InProcess{URL: *url, Creds: pull, Embedding: embedding}
	case "microsandbox":
		if *workloadBinary == "" {
			return fmt.Errorf("the microsandbox backend needs --workload-binary (make workload-linux builds it)")
		}
		backend = &executor.Microsandbox{
			WorkloadBinary: *workloadBinary,
			HostURL:        *url,
			Image:          *image,
			Creds:          pull,
			Embedding:      embedding,
		}
	default:
		return fmt.Errorf("backend %q is not in this build's vocabulary (inprocess, microsandbox)", *backendName)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ex, err := executor.Start(startCtx, nc, executor.Config{ID: *id, Backend: backend})
	if err != nil {
		return err
	}
	defer ex.Stop()

	fmt.Printf("chronicle-executor %s: %s carrying backend %s on %s (SIGTERM to stop)\n", version.Version, *id, *backendName, *url)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return nil
}
