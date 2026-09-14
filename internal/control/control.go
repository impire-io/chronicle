// Package control runs chronicle-control: the cross-account control plane
// that holds the minting material and turns "create tenant" into a minted
// account, a provisioned META, a seeded identity registry, and a first
// admin's credentials (chronicle-hq/02-DESIGN/01-onboarding.md,
// 02-DESIGN/04-fleet.md). It is the only component that crosses accounts;
// no tenant workload ever holds another tenant's credentials.
package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/version"
)

// tenantName rules match log names: lowercase tokens; the name becomes the
// account name and a directory.
var tenantName = regexp.MustCompile(`^[a-z0-9-]+$`)

// Config wires one control service.
type Config struct {
	// Driver mints accounts on the configured substrate — exactly one per
	// install.
	Driver mint.Driver
	// URL is the client URL tenants connect to; META provisioning and the
	// verify-by-connect read dial it.
	URL string
	// AccountsDir is where per-tenant issuance material lands: the account
	// signing keys live with the issuance path, the service creds with the
	// fleet. Admin creds are handed back, never kept.
	AccountsDir string
	// OnTenant, when set, is told about every tenant that exists — at mint
	// and for each already on disk at start — so the composition root can
	// run a node for it. The error fails the mint: a tenant without a node
	// would answer no verbs.
	OnTenant func(name string, serviceCreds []byte) error
	// Logger; nil means slog.Default.
	Logger *slog.Logger
}

// Start replays the accounts dir through OnTenant (the restart path) and
// registers the chronicle-control micro service. Stopping the returned
// service is the caller's job.
func Start(nc *nats.Conn, cfg Config) (micro.Service, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	c := &control{cfg: cfg, js: js}

	svc, err := micro.AddService(nc, micro.Config{
		Name:        "chronicle-control",
		Version:     version.Version,
		Description: "chronicle control plane: minting, invite redemption",
	})
	if err != nil {
		return nil, fmt.Errorf("register service: %w", err)
	}
	if err := svc.AddEndpoint("tenant-mint", micro.HandlerFunc(c.handleMint),
		micro.WithEndpointSubject(client.TenantMintSubject)); err != nil {
		_ = svc.Stop()
		return nil, fmt.Errorf("add tenant-mint endpoint: %w", err)
	}
	if err := svc.AddEndpoint("fleet-creds", micro.HandlerFunc(c.handleFleetCreds),
		micro.WithEndpointSubject(contract.FleetCredsSubject)); err != nil {
		_ = svc.Stop()
		return nil, fmt.Errorf("add fleet-creds endpoint: %w", err)
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		_ = svc.Stop()
		return nil, fmt.Errorf("flush endpoint subscriptions: %w", err)
	}

	// The restart replay runs with the endpoints already serving: placing a
	// tenant's node goes through the dispatch surface and the creds pull,
	// and the creds pull lands right here — a replay before registration
	// would wait on an endpoint that cannot appear until the replay ends.
	if cfg.OnTenant != nil {
		entries, err := os.ReadDir(cfg.AccountsDir)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			_ = svc.Stop()
			return nil, fmt.Errorf("read accounts dir: %w", err)
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			creds, err := os.ReadFile(filepath.Join(cfg.AccountsDir, e.Name(), "service.creds"))
			if err != nil {
				_ = svc.Stop()
				return nil, fmt.Errorf("tenant %s: read service creds: %w", e.Name(), err)
			}
			if err := cfg.OnTenant(e.Name(), creds); err != nil {
				_ = svc.Stop()
				return nil, fmt.Errorf("tenant %s: %w", e.Name(), err)
			}
		}
	}
	return svc, nil
}

type control struct {
	cfg Config
	// js is the control account's JetStream view — the fleet log and
	// STATE_FLEET live there, and the creds pull verifies against them.
	js jetstream.JetStream
}

func (c *control) handleMint(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var r client.TenantMintRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if !tenantName.MatchString(r.Name) {
		_ = req.Error("bad-tenant-name", fmt.Sprintf("tenant name %q: must match [a-z0-9-]+", r.Name), nil)
		return
	}
	admin := r.Admin
	if admin == "" {
		admin = "admin"
	}

	dir := filepath.Join(c.cfg.AccountsDir, r.Name)
	if _, err := os.Stat(dir); err == nil {
		_ = req.Error("tenant-exists", fmt.Sprintf("tenant %q already exists", r.Name), nil)
		return
	}

	resp, err := c.mintTenant(ctx, r.Name, admin, dir)
	if err != nil {
		c.cfg.Logger.Error("mint tenant", "tenant", r.Name, "err", err)
		_ = req.Error("mint-failed", err.Error(), nil)
		return
	}
	reply, err := json.Marshal(resp)
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

// mintTenant is the one flow of the onboarding design, driver-agnostic
// above the seam: mint the account, establish the keys, provision META and
// seed the registry, verify by connecting, mint the first principal.
func (c *control) mintTenant(ctx context.Context, name, admin, dir string) (client.TenantMintResponse, error) {
	var zero client.TenantMintResponse

	acct, err := c.cfg.Driver.MintAccount(ctx, name)
	if err != nil {
		return zero, fmt.Errorf("mint account: %w", err)
	}

	svcCreds, err := mint.IssueServiceUser(acct, "chronicle-node")
	if err != nil {
		return zero, fmt.Errorf("issue service user: %w", err)
	}
	adminCreds, err := mint.IssueMember(acct, admin)
	if err != nil {
		return zero, fmt.Errorf("issue admin: %w", err)
	}

	if err := c.provisionMeta(ctx, svcCreds.File, admin, adminCreds.PublicKey); err != nil {
		return zero, fmt.Errorf("provision META: %w", err)
	}

	// Persist the issuance material: the signing keys live with the
	// issuance path, the service creds with the fleet. The admin creds go
	// back to the caller — the only copy; the registry keeps the public
	// key, never the secret.
	if err := c.persist(dir, acct, svcCreds.File); err != nil {
		return zero, fmt.Errorf("persist tenant material: %w", err)
	}

	if c.cfg.OnTenant != nil {
		if err := c.cfg.OnTenant(name, svcCreds.File); err != nil {
			return zero, fmt.Errorf("start tenant workloads: %w", err)
		}
	}

	return client.TenantMintResponse{
		Account:    acct.PublicKey,
		Admin:      admin,
		AdminCreds: adminCreds.File,
	}, nil
}

func (c *control) provisionMeta(ctx context.Context, svcCreds []byte, admin, adminPub string) error {
	nc, err := mint.ConnectCreds(c.cfg.URL, svcCreds, "chronicle-control-provision")
	if err != nil {
		return fmt.Errorf("connect as service user: %w", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	meta, err := js.CreateKeyValue(ctx, contract.MetaBucketConfig())
	if err != nil {
		return fmt.Errorf("create META bucket: %w", err)
	}
	principal, err := json.Marshal(contract.Principal{ID: admin})
	if err != nil {
		return err
	}
	if _, err := meta.Put(ctx, contract.MetaPrincipal(admin), principal); err != nil {
		return fmt.Errorf("seed principal: %w", err)
	}
	membership, err := json.Marshal(contract.Membership{PublicKey: adminPub, Role: contract.RoleAdmin})
	if err != nil {
		return err
	}
	if _, err := meta.Put(ctx, contract.MetaMember(admin), membership); err != nil {
		return fmt.Errorf("seed membership: %w", err)
	}
	return nil
}

func (c *control) persist(dir string, acct *mint.Account, svcCreds []byte) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	files := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{"account.pub", []byte(acct.PublicKey), 0o644},
		{"signing.nk", acct.SigningSeed, 0o600},
		{"scoped.nk", acct.ScopedSeed, 0o600},
		{"service.creds", svcCreds, 0o600},
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f.name), f.data, f.mode); err != nil {
			return fmt.Errorf("write %s: %w", f.name, err)
		}
	}
	return nil
}
