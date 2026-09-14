package executor

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/guestnet"
	"github.com/impire-io/chronicle/internal/index/semantic"
)

// Microsandbox is the first 0004 backend (06-scheduler.md § backends):
// placements as microVMs through the msb CLI, pinned against msb 0.6.8.
// The surface this backend uses — run --name --net host --copy-file with
// a trailing command, stop, rm — is the pinned API; an msb upgrade is a
// deliberate revisit of exactly that list. The foreground `msb run`
// process is the supervision seam: its lifetime is the placement's, its
// exit the death the executor's restart budget answers.
type Microsandbox struct {
	// Image is the base image the workload binary is copied into at
	// boot. A published chronicle image replaces this default when a
	// release flow exists; until then the image is configuration, not
	// contract.
	Image string
	// WorkloadBinary is the host path of the linux chronicle-workload
	// binary (the host's architecture) copied into every guest.
	WorkloadBinary string
	// HostURL is the NATS URL as the host knows it; the guest receives
	// the same port on the msb-gateway host and resolves it itself
	// (internal/guestnet).
	HostURL string
	// Creds pulls a workload's service creds — the record-verified pull.
	Creds func(ctx context.Context, tenant, workload string) ([]byte, error)
	// StageDir is where a placement's creds rest while it runs — the
	// executor host's named at-rest exposure (0014). Empty means the OS
	// temp dir.
	StageDir string
	// MSB is the msb binary; empty means "msb" on PATH.
	MSB string
	// Embedding is the install's provider (0016); nil means no provider,
	// and this host does not bid for semantic workloads. The key travels
	// the secret channel: staged beside the creds, copied into the
	// rootfs, never env.
	Embedding *semantic.ProviderConfig
	// Logger; nil means slog.Default.
	Logger *slog.Logger
}

// Name is the backend's roster identity.
func (b *Microsandbox) Name() string { return "microsandbox" }

// Supports names this host's vocabulary: semantic only with a provider.
func (b *Microsandbox) Supports(kind string) bool {
	switch kind {
	case contract.WorkloadKindNode, contract.WorkloadKindIndexSearch, contract.WorkloadKindIndexGraph:
		return true
	case contract.WorkloadKindIndexSemantic:
		return b.Embedding.Configured()
	}
	return false
}

// DefaultImage is the base the walking skeleton boots: small, cached
// after the first pull, enough for a static binary.
const DefaultImage = "alpine"

// guestBinaryPath and guestCredsPath are where the two copy-files land in
// the rootfs — the workload contract's two channels made concrete.
const (
	guestBinaryPath = "/chronicle-workload"
	guestCredsPath  = "/service.creds"
)

// sandboxName is the placement's msb identity: stable per workload so a
// crashed predecessor is removable by name before its successor boots.
func sandboxName(tenant, workload string) string {
	return "chron-" + tenant + "-" + workload
}

// guestEmbedKeyPath is where a semantic placement's provider key lands —
// the second secret on the same channel as the creds.
const guestEmbedKeyPath = "/embedding.key"

// msbRunArgs builds the run invocation — pure, so the pinned surface is
// unit-tested without the substrate. stagedKey is empty for kinds that
// carry no provider secret.
func msbRunArgs(image, name, binary, stagedCreds, stagedKey, guestURL string, spec Spec, embed *semantic.ProviderConfig) []string {
	args := []string{
		"run", image,
		"--name", name,
		"--net", "host",
		"--copy-file", binary + ":" + guestBinaryPath,
		"--copy-file", stagedCreds + ":" + guestCredsPath,
	}
	if stagedKey != "" {
		args = append(args, "--copy-file", stagedKey+":"+guestEmbedKeyPath)
	}
	args = append(args,
		"--",
		guestBinaryPath,
		"--kind", spec.Kind,
		"--url", guestURL,
		"--creds", guestCredsPath,
	)
	if spec.Log != "" {
		args = append(args, "--log", spec.Log)
	}
	if spec.Index != "" {
		args = append(args, "--index", spec.Index)
	}
	if spec.Kind == contract.WorkloadKindIndexSemantic && embed != nil {
		args = append(args, "--embedding-url", embed.BaseURL, "--embedding-model", embed.Model)
		if stagedKey != "" {
			args = append(args, "--embedding-key-file", guestEmbedKeyPath)
		}
	}
	return args
}

// guestURL rewrites the host URL's host to the msb-gateway placeholder,
// keeping the port — the guest resolves the rest.
func guestURL(hostURL string) (string, error) {
	u, err := url.Parse(hostURL)
	if err != nil {
		return "", fmt.Errorf("parse host url: %w", err)
	}
	port := u.Port()
	if port == "" {
		port = "4222"
	}
	u.Host = guestnet.GatewayHost + ":" + port
	return u.String(), nil
}

// Start runs one placement attempt: pull the creds, stage them for the
// boot-time copy, boot the microVM, supervise the child.
func (b *Microsandbox) Start(ctx context.Context, spec Spec) (Placement, error) {
	logger := b.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if b.WorkloadBinary == "" {
		return nil, fmt.Errorf("microsandbox backend needs the workload binary path")
	}
	image := b.Image
	if image == "" {
		image = DefaultImage
	}
	msb := b.MSB
	if msb == "" {
		msb = "msb"
	}

	creds, err := pullCredsWithRetry(ctx, b.Creds, spec.Tenant, spec.Workload)
	if err != nil {
		return nil, fmt.Errorf("pull creds for %s/%s: %w", spec.Tenant, spec.Workload, err)
	}
	stage, err := os.MkdirTemp(b.StageDir, "chron-"+spec.Tenant+"-")
	if err != nil {
		return nil, fmt.Errorf("stage dir: %w", err)
	}
	stagedCreds := filepath.Join(stage, "service.creds")
	if err := os.WriteFile(stagedCreds, creds, 0o600); err != nil {
		_ = os.RemoveAll(stage)
		return nil, fmt.Errorf("stage creds: %w", err)
	}
	stagedKey := ""
	if spec.Kind == contract.WorkloadKindIndexSemantic && b.Embedding.Configured() && b.Embedding.APIKey != "" {
		stagedKey = filepath.Join(stage, "embedding.key")
		if err := os.WriteFile(stagedKey, []byte(b.Embedding.APIKey), 0o600); err != nil {
			_ = os.RemoveAll(stage)
			return nil, fmt.Errorf("stage embedding key: %w", err)
		}
	}

	gURL, err := guestURL(b.HostURL)
	if err != nil {
		_ = os.RemoveAll(stage)
		return nil, err
	}
	name := sandboxName(spec.Tenant, spec.Workload)

	// A crashed predecessor's sandbox must never block its successor:
	// converge toward absence first, best effort.
	rmCtx, rmCancel := context.WithTimeout(ctx, 10*time.Second)
	_ = exec.CommandContext(rmCtx, msb, "rm", name).Run()
	rmCancel()

	runCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(runCtx, msb, msbRunArgs(image, name, b.WorkloadBinary, stagedCreds, stagedKey, gURL, spec, b.Embedding)...)
	if err := cmd.Start(); err != nil {
		cancel()
		_ = os.RemoveAll(stage)
		return nil, fmt.Errorf("boot sandbox %s: %w", name, err)
	}
	logger.Info("microsandbox: placement booting", "sandbox", name, "kind", spec.Kind)

	p := &msbPlacement{
		msb:    msb,
		name:   name,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go func() {
		_ = cmd.Wait()
		// The staged creds live exactly as long as the placement — the
		// named at-rest exposure, cleaned the moment it ends.
		_ = os.RemoveAll(stage)
		close(p.done)
	}()
	return p, nil
}

type msbPlacement struct {
	msb    string
	name   string
	cancel context.CancelFunc
	done   chan struct{}
}

// Stop converges toward absence: a graceful stop, then the child dies
// with its context, then the sandbox record goes. Every step tolerates
// having already happened.
func (p *msbPlacement) Stop() {
	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = exec.CommandContext(stopCtx, p.msb, "stop", p.name).Run()
	p.cancel()
	<-p.done
	rmCtx, rmCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rmCancel()
	_ = exec.CommandContext(rmCtx, p.msb, "rm", p.name).Run()
}

func (p *msbPlacement) Done() <-chan struct{} { return p.done }

// The Microsandbox backend shares the record-verified pull's client-side
// fold-lag retry with backend zero.
func pullCredsWithRetry(ctx context.Context, pull func(context.Context, string, string) ([]byte, error), tenant, workload string) ([]byte, error) {
	if pull == nil {
		return nil, fmt.Errorf("no creds pull wired")
	}
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for {
		creds, err := pull(ctx, tenant, workload)
		if err == nil {
			return creds, nil
		}
		lastErr = err
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, lastErr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

var _ Backend = (*Microsandbox)(nil)
