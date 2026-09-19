package fleet

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/control"
	"github.com/impire-io/chronicle/internal/identity/github"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/version"
	"github.com/impire-io/chronicle/internal/workloads"
)

// PlacementTimeout bounds how long a mint — or the boot-time replay of the
// tenants in custody — waits for a tenant's node to answer. The wait
// covers the hosted boot race (chronicle-hq/02-DESIGN/09-hosted-
// environment.md): control's replay dispatches before the host's executor
// has registered, the auction finds no bidder, and the level scan
// re-auctions once it has. A tenant without a node answers no verbs, so
// the mint's promise is not kept until the node is.
const PlacementTimeout = 45 * time.Second

// ControlPlaneConfig configures one control instance standing over a
// substrate that already serves: `chronicle-control --url --bundle` on a
// hosted host, and the same wiring inside `up` once its embedded server
// is up. The instance holds one secret — its bundle — and reads everything
// else from the AUTH bucket (design 10).
type ControlPlaneConfig struct {
	// URL is the client URL tenants connect to. Control stamps it into
	// META provisioning and dials it for the verify-by-connect read, so for
	// the hosted form it is the public name — never a private address.
	URL string
	// Bundle is the instance's credentials directory: control.creds under
	// the control-instance template and sys.creds, issued by init for the
	// first instance and by `operator instance add` for every other.
	Bundle string
	// GithubClientID configures the browser identity bridge (decision
	// 0026) — the install's GitHub App. Empty means no bridge.
	GithubClientID string
	// BridgeProfile is where the bridge's hand-out for `chronicle login`
	// is written when the bridge is on; empty means bridge.json beside
	// the bundle.
	BridgeProfile string
	// Logger; nil means slog.Default.
	Logger *slog.Logger
}

// ControlPlane is one running control instance: control's verbs and its
// bridge. The workload service is its own binary (decision 0029) and the
// executors their own units; this plane composes none of them.
type ControlPlane struct {
	// Instance is the name the bundle was issued under.
	Instance string

	ctrl  micro.Service
	conns []*nats.Conn
}

// StartControlPlane opens the bundle, connects its two users, opens
// custody, and starts control — the standalone form the hosted
// environment runs as a unit. A boot where control comes up before the
// host's executor is the normal case, absorbed by PlacementTimeout.
func StartControlPlane(ctx context.Context, cfg ControlPlaneConfig) (*ControlPlane, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("control plane needs the client url tenants connect to")
	}
	if cfg.Bundle == "" {
		return nil, fmt.Errorf("control plane needs its bundle")
	}
	bundle, err := mint.ReadBundle(cfg.Bundle)
	if err != nil {
		return nil, err
	}
	if !bundle.IsControlInstance() {
		return nil, fmt.Errorf("%s is not a control instance's bundle (no sys.creds): issue one with `chronicle operator instance add <name> --template control-instance`", cfg.Bundle)
	}
	instance, err := mint.InstanceOf(bundle.ControlCreds)
	if err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	cp := &ControlPlane{Instance: instance}
	connect := func(creds []byte, name string) (*nats.Conn, error) {
		nc, err := mint.ConnectCreds(cfg.URL, creds, name)
		if err != nil {
			return nil, fmt.Errorf("connect %s: %w", name, err)
		}
		cp.conns = append(cp.conns, nc)
		return nc, nil
	}
	sysConn, err := connect(bundle.SysCreds, instance+"-sys")
	if err != nil {
		cp.Stop()
		return nil, err
	}
	ctrlConn, err := connect(bundle.ControlCreds, instance)
	if err != nil {
		cp.Stop()
		return nil, err
	}
	custody, err := mint.OpenCustody(ctx, ctrlConn)
	if err != nil {
		cp.Stop()
		return nil, err
	}
	driver, err := mint.NewJWTDriver(ctx, custody, sysConn, cfg.URL)
	if err != nil {
		cp.Stop()
		return nil, err
	}
	profile := cfg.BridgeProfile
	if profile == "" {
		profile = filepath.Join(cfg.Bundle, bridgeProfileFile)
	}
	ctrl, err := startControl(ctx, controlInputs{
		url:            cfg.URL,
		ctrlConn:       ctrlConn,
		custody:        custody,
		driver:         driver,
		githubClientID: cfg.GithubClientID,
		bridgeProfile:  profile,
		logger:         logger,
		connect:        connect,
	})
	if err != nil {
		cp.Stop()
		return nil, err
	}
	cp.ctrl = ctrl
	return cp, nil
}

// bridgeProfileFile is the bridge's hand-out for `chronicle login`.
const bridgeProfileFile = "bridge.json"

// controlInputs is what starting control needs, whichever root composes
// it: the instance's control connection and its driver over custody, the
// bridge's configuration, and a connect the owner tracks for teardown.
type controlInputs struct {
	url            string
	ctrlConn       *nats.Conn
	custody        *mint.Custody
	driver         *mint.JWTDriver
	githubClientID string
	bridgeProfile  string
	logger         *slog.Logger
	connect        func(creds []byte, name string) (*nats.Conn, error)
}

// startControl is the composition both roots share: the bridge from
// custody when a GitHub App is configured, then control — whose OnTenant
// dispatches a node workload for every tenant, at mint and at the boot
// replay, and waits for the placed node to answer.
func startControl(ctx context.Context, in controlInputs) (micro.Service, error) {
	var bridgeCfg *control.BridgeConfig
	if in.githubClientID != "" {
		authRec, _, err := in.custody.Auth(ctx)
		if err != nil {
			return nil, err
		}
		authAcct, _, err := in.custody.Account(ctx, "AUTH")
		if err != nil {
			return nil, err
		}
		authConn, err := in.connect([]byte(authRec.BridgeCreds), "chronicle-bridge")
		if err != nil {
			return nil, err
		}
		bridgeCfg = &control.BridgeConfig{
			Conn:               authConn,
			ResponseSignerSeed: []byte(authAcct.Seed),
			XKeySeed:           []byte(authRec.XKeySeed),
			Validator:          &github.Client{ClientID: in.githubClientID},
			Logger:             in.logger,
		}
		// The bridge profile is the hand-out that makes `chronicle login`
		// possible — public material only (0026: the sentinel is public
		// by design).
		if err := writeBridgeProfile(in.bridgeProfile, in.url, in.githubClientID, []byte(authRec.SentinelCreds)); err != nil {
			return nil, err
		}
	}
	return control.Start(in.ctrlConn, control.Config{
		Driver: in.driver,
		URL:    in.url,
		Bridge: bridgeCfg,
		OnTenant: func(name string, serviceCreds []byte) error {
			// The tenant's node exists because the record says so; the
			// executor pulls the creds itself — the record-verified pull.
			dispatchCtx, cancel := context.WithTimeout(ctx, PlacementTimeout)
			defer cancel()
			if _, err := workloads.Dispatch(dispatchCtx, in.ctrlConn, contract.FleetDispatchRequest{
				Tenant:   name,
				Workload: contract.WorkloadNodeName,
				Kind:     contract.WorkloadKindNode,
			}); err != nil {
				return err
			}
			// Placement is asynchronous, but the mint's promise is not: a
			// minted tenant answers verbs (onboarding § verify by
			// connecting). Wait until the placed node serves.
			return waitForNode(dispatchCtx, in.url, name, serviceCreds)
		},
		Logger: in.logger,
	})
}

// Stop tears the instance down: control's verbs first, then its
// connections. Appends need none of them — writers lose nothing, running
// placements keep serving, and the other instances keep answering.
func (cp *ControlPlane) Stop() {
	if cp.ctrl != nil {
		_ = cp.ctrl.Stop()
		cp.ctrl = nil
	}
	conns := cp.conns
	cp.conns = nil
	for _, nc := range conns {
		nc.Close()
	}
}

// RunControl is `chronicle-control --url --bundle`: one control instance
// over an operator-run substrate, serving until the context ends. It is
// the hosted environment's control unit (design 09 § the stand-up
// ceremony); `up` composes the same control over its embedded server.
func RunControl(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle-control", flag.ContinueOnError)
	fs.SetOutput(out)
	url := fs.String("url", "", "the client url tenants connect to (required)")
	bundle := fs.String("bundle", "", "this instance's bundle directory: control.creds and sys.creds (required)")
	githubClientID := fs.String("github-client-id", "", "GitHub App client id — enables the browser identity bridge (0026)")
	profile := fs.String("bridge-profile", "", "where the bridge writes the hand-out for `chronicle login` (default: bridge.json beside the bundle)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("chronicle-control takes no positionals")
	}
	if *url == "" || *bundle == "" {
		return fmt.Errorf("chronicle-control needs --url and --bundle")
	}

	cp, err := StartControlPlane(ctx, ControlPlaneConfig{URL: *url, Bundle: *bundle, GithubClientID: *githubClientID, BridgeProfile: *profile})
	if err != nil {
		return err
	}
	defer cp.Stop()

	fmt.Fprintf(out, "chronicle-control %s: instance %s serving on %s (SIGTERM to stop)\n", version.Version, cp.Instance, *url)
	if *githubClientID != "" {
		fmt.Fprintf(out, "  bridge: GitHub App %s\n", *githubClientID)
	}
	<-ctx.Done()
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	return ctx.Err()
}
