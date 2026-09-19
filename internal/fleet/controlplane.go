package fleet

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/control"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/identity/github"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/version"
	"github.com/impire-io/chronicle/internal/workloads"
)

// PlacementTimeout bounds how long a mint — or the boot-time replay of the
// tenants already on disk — waits for a tenant's node to answer. The wait
// covers the hosted boot race (chronicle-hq/02-DESIGN/09-hosted-
// environment.md): control's replay dispatches before the host's executor
// has registered, the auction finds no bidder, and the level scan
// re-auctions once it has. A tenant without a node answers no verbs, so
// the mint's promise is not kept until the node is.
const PlacementTimeout = 45 * time.Second

// ControlPlaneConfig configures the control plane standing over a
// substrate that already serves: `chronicle-control` on the hosted
// workload host, and the same wiring inside `up` once its embedded server
// is up.
type ControlPlaneConfig struct {
	// Dir is the bootstrap dir: custody, per-tenant issuance material, and
	// the bridge profile hand-out. Empty means devdir.Default().
	Dir string
	// URL is the client URL tenants connect to. Control stamps it into
	// META provisioning and dials it for the verify-by-connect read, so for
	// the hosted form it is the public name — never a private address.
	URL string
	// GithubClientID configures the browser identity bridge (decision
	// 0026) — the install's GitHub App. Empty means no bridge.
	GithubClientID string
	// Logger; nil means slog.Default.
	Logger *slog.Logger
}

// ControlPlane is one running control plane: control's verbs and bridge,
// and one chronicle-workloads instance, each on its own connection.
type ControlPlane struct {
	ctrl  micro.Service
	wl    *workloads.Service
	conns []*nats.Conn
}

// StartControlPlane loads the bootstrap dir and composes the control plane
// over cfg.URL — the standalone form the hosted environment runs as a
// systemd unit. The host's executor is a separate process; a boot where
// control comes up first is the normal case, absorbed by PlacementTimeout.
func StartControlPlane(ctx context.Context, cfg ControlPlaneConfig) (*ControlPlane, error) {
	if cfg.Dir == "" {
		cfg.Dir = devdir.Default()
	}
	b, err := mint.LoadOrInitBootstrap(cfg.Dir)
	if err != nil {
		return nil, err
	}
	return startControlPlane(ctx, b, cfg, nil)
}

// startControlPlane is the composition both roots share: the workload
// service first, then — after beforeControl, when given — control with its
// bridge. beforeControl is `up`'s seam: its embedded executor registers on
// the roster the workload service holds, and must be bidding before
// control's boot replay dispatches the tenants on disk.
func startControlPlane(ctx context.Context, b *mint.Bootstrap, cfg ControlPlaneConfig, beforeControl func() error) (*ControlPlane, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("control plane needs the client url tenants connect to")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	cp := &ControlPlane{}
	connect := func(creds []byte, name string) (*nats.Conn, error) {
		nc, err := mint.ConnectCreds(cfg.URL, creds, name)
		if err != nil {
			return nil, fmt.Errorf("connect %s: %w", name, err)
		}
		cp.conns = append(cp.conns, nc)
		return nc, nil
	}

	sysConn, err := connect(b.SysCreds, "chronicle-sys")
	if err != nil {
		cp.Stop()
		return nil, err
	}
	ctrlConn, err := connect(b.ControlCreds, "chronicle-control")
	if err != nil {
		cp.Stop()
		return nil, err
	}
	// Every control-plane component shares the bootstrap control user on
	// its own connection; per-component users are custody a later
	// increment makes real.
	wlConn, err := connect(b.ControlCreds, "chronicle-workloads")
	if err != nil {
		cp.Stop()
		return nil, err
	}
	wl, err := workloads.Start(ctx, wlConn, workloads.Config{Logger: logger})
	if err != nil {
		cp.Stop()
		return nil, fmt.Errorf("start workload service: %w", err)
	}
	cp.wl = wl

	if beforeControl != nil {
		if err := beforeControl(); err != nil {
			cp.Stop()
			return nil, err
		}
	}

	var bridgeCfg *control.BridgeConfig
	if cfg.GithubClientID != "" {
		authConn, err := connect(b.BridgeCreds, "chronicle-bridge")
		if err != nil {
			cp.Stop()
			return nil, err
		}
		bridgeCfg = &control.BridgeConfig{
			Conn:               authConn,
			ResponseSignerSeed: b.AuthAccountSeed,
			XKeySeed:           b.AuthXKeySeed,
			Validator:          &github.Client{ClientID: cfg.GithubClientID},
			Logger:             logger,
		}
		// The bridge profile is the hand-out that makes `chronicle login`
		// possible — public material only (0026: the sentinel is public
		// by design).
		if err := writeBridgeProfile(cfg.Dir, cfg.URL, cfg.GithubClientID, b.SentinelCreds); err != nil {
			cp.Stop()
			return nil, err
		}
	}

	ctrl, err := control.Start(ctrlConn, control.Config{
		Driver:      b.Driver(sysConn, cfg.URL),
		URL:         cfg.URL,
		AccountsDir: b.AccountsDir(),
		Bridge:      bridgeCfg,
		OnTenant: func(name string, serviceCreds []byte) error {
			// The tenant's node exists because the record says so; the
			// executor pulls the creds itself — the record-verified pull.
			dispatchCtx, cancel := context.WithTimeout(ctx, PlacementTimeout)
			defer cancel()
			if _, err := workloads.Dispatch(dispatchCtx, ctrlConn, contract.FleetDispatchRequest{
				Tenant:   name,
				Workload: contract.WorkloadNodeName,
				Kind:     contract.WorkloadKindNode,
			}); err != nil {
				return err
			}
			// Placement is asynchronous, but the mint's promise is not: a
			// minted tenant answers verbs (onboarding § verify by
			// connecting). Wait until the placed node serves.
			return waitForNode(dispatchCtx, cfg.URL, name, serviceCreds)
		},
		Logger: logger,
	})
	if err != nil {
		cp.Stop()
		return nil, err
	}
	cp.ctrl = ctrl
	return cp, nil
}

// Stop tears the control plane down: control's verbs first, then the
// workload service, then the connections. Appends need none of them —
// writers lose nothing, and running placements keep serving.
func (cp *ControlPlane) Stop() {
	if cp.ctrl != nil {
		_ = cp.ctrl.Stop()
		cp.ctrl = nil
	}
	if cp.wl != nil {
		cp.wl.Stop()
		cp.wl = nil
	}
	conns := cp.conns
	cp.conns = nil
	for _, nc := range conns {
		nc.Close()
	}
}

// RunControl is `chronicle-control`: the standalone control plane over an
// operator-run substrate, serving until the context ends. It is the
// hosted environment's control unit (design 09 § the stand-up ceremony);
// `up` composes the same plane over its embedded server.
func RunControl(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle-control", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "data dir holding the bootstrap material")
	url := fs.String("url", "", "the client url tenants connect to (default: the data dir's recorded url)")
	githubClientID := fs.String("github-client-id", "", "GitHub App client id — enables the browser identity bridge (0026)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("chronicle-control takes no positionals")
	}
	target := *url
	if target == "" {
		recorded, err := devdir.ReadClientURL(*dir)
		if err != nil {
			return fmt.Errorf("no --url and no recorded url in %s: %w", *dir, err)
		}
		target = recorded
	}

	cp, err := StartControlPlane(ctx, ControlPlaneConfig{Dir: *dir, URL: target, GithubClientID: *githubClientID})
	if err != nil {
		return err
	}
	defer cp.Stop()

	fmt.Fprintf(out, "chronicle-control %s serving on %s (SIGTERM to stop)\n", version.Version, target)
	if *githubClientID != "" {
		fmt.Fprintf(out, "  bridge: GitHub App %s, profile in %s\n", *githubClientID, *dir)
	}
	<-ctx.Done()
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	return ctx.Err()
}
