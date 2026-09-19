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
	"regexp"
	"sync"
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

// principalName holds the same line for principal IDs: the ID becomes a
// registry key segment (identity.member.<id> must stay one KV token) and a
// default .creds filename — KV itself would accept dots and slashes, and
// both would leak structure into places that trust the shape.
var principalName = regexp.MustCompile(`^[a-z0-9-]+$`)

// Config wires one control service.
type Config struct {
	// Driver mints accounts on the configured substrate — exactly one per
	// install.
	Driver mint.Driver
	// URL is the client URL tenants connect to; META provisioning and the
	// verify-by-connect read dial it.
	URL string
	// OnTenant, when set, is told about every tenant that exists — at mint
	// and for each in custody at start — so the composition root can run a
	// node for it. The error fails the mint: a tenant without a node would
	// answer no verbs.
	OnTenant func(name string, serviceCreds []byte) error
	// ReconcileEvery is how often this instance compares custody's account
	// JWTs with the resolver's and pushes the differences (design 10 § the
	// bucket) — the repair of a push that never landed. Zero means
	// DefaultReconcileEvery.
	ReconcileEvery time.Duration
	// Bridge, when set, serves the browser identity bridge (decision
	// 0026) beside the control endpoints. Nil means no bridge — an
	// install without a GitHub App simply has none.
	Bridge *BridgeConfig
	// Logger; nil means slog.Default.
	Logger *slog.Logger
}

// DefaultReconcileEvery keeps the resolver honest without chatter.
const DefaultReconcileEvery = 5 * time.Minute

// Start reconciles the resolver against custody, replays custody's tenants
// through OnTenant (the restart path), registers the chronicle-control
// micro service, and keeps reconciling on a timer. Stopping the returned
// service is the caller's job; it stops the timer too.
func Start(nc *nats.Conn, cfg Config) (micro.Service, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Driver == nil {
		return nil, fmt.Errorf("control needs a driver")
	}
	if cfg.ReconcileEvery == 0 {
		cfg.ReconcileEvery = DefaultReconcileEvery
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
		micro.WithEndpointSubject(contract.FleetCredsSubject("*"))); err != nil {
		_ = svc.Stop()
		return nil, fmt.Errorf("add fleet-creds endpoint: %w", err)
	}
	memberEndpoints := []struct {
		name    string
		subject string
		handler micro.HandlerFunc
	}{
		{"member-add", client.MemberAddSubject, c.handleMemberAdd},
		{"member-revoke", client.MemberRevokeSubject, c.handleMemberRevoke},
		{"member-rekey", client.MemberRekeySubject, c.handleMemberRekey},
	}
	for _, e := range memberEndpoints {
		if err := svc.AddEndpoint(e.name, e.handler, micro.WithEndpointSubject(e.subject)); err != nil {
			_ = svc.Stop()
			return nil, fmt.Errorf("add %s endpoint: %w", e.name, err)
		}
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		_ = svc.Stop()
		return nil, fmt.Errorf("flush endpoint subscriptions: %w", err)
	}
	if cfg.Bridge != nil {
		bcfg := *cfg.Bridge
		if bcfg.Logger == nil {
			bcfg.Logger = cfg.Logger
		}
		if err := c.startBridge(bcfg); err != nil {
			_ = svc.Stop()
			return nil, err
		}
	}

	// The boot reconcile: whatever custody says an account's JWT is, the
	// resolver serves — repairing any push a dead instance never made.
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer bootCancel()
	c.reconcile(bootCtx)

	// The restart replay runs with the endpoints already serving: placing a
	// tenant's node goes through the dispatch surface and the creds pull,
	// and the creds pull lands right here — a replay before registration
	// would wait on an endpoint that cannot appear until the replay ends.
	if cfg.OnTenant != nil {
		names, err := cfg.Driver.Tenants(bootCtx)
		if err != nil {
			_ = svc.Stop()
			return nil, fmt.Errorf("list tenants: %w", err)
		}
		for _, name := range names {
			acct, err := cfg.Driver.Tenant(bootCtx, name)
			if err != nil {
				_ = svc.Stop()
				return nil, fmt.Errorf("tenant %s: %w", name, err)
			}
			if err := cfg.OnTenant(name, acct.ServiceCreds); err != nil {
				_ = svc.Stop()
				return nil, fmt.Errorf("tenant %s: %w", name, err)
			}
		}
	}

	c.done = make(chan struct{})
	go c.reconcileLoop()
	return &stopping{Service: svc, done: c.done}, nil
}

// stopping is the returned service: stopping it ends the reconcile loop
// before the endpoints go.
type stopping struct {
	micro.Service
	done chan struct{}
	once sync.Once
}

func (s *stopping) Stop() error {
	s.once.Do(func() { close(s.done) })
	return s.Service.Stop()
}

func (c *control) reconcileLoop() {
	t := time.NewTicker(c.cfg.ReconcileEvery)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			c.reconcile(ctx)
			cancel()
		}
	}
}

func (c *control) reconcile(ctx context.Context) {
	pushed, err := c.cfg.Driver.Reconcile(ctx)
	if err != nil {
		c.cfg.Logger.Warn("control: reconcile", "err", err)
	}
	if len(pushed) > 0 {
		c.cfg.Logger.Info("control: reconciled the resolver from custody", "accounts", pushed)
	}
}

type control struct {
	cfg Config
	// js is the control account's JetStream view — the fleet log and
	// STATE_FLEET live there, and the creds pull verifies against them.
	js jetstream.JetStream
	// done ends the reconcile loop when the service stops. Claims
	// mutations need no lock here: the driver lands each one in custody
	// by compare-and-set, which guards across instances, not just within
	// this one.
	done chan struct{}
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
	if !principalName.MatchString(admin) {
		_ = req.Error("bad-principal-name", fmt.Sprintf("principal %q: must match [a-z0-9-]+", admin), nil)
		return
	}

	resp, err := c.mintTenant(ctx, r.Name, admin)
	if err != nil {
		if errors.Is(err, mint.ErrTenantExists) {
			_ = req.Error("tenant-exists", fmt.Sprintf("tenant %q already exists", r.Name), nil)
			return
		}
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
func (c *control) mintTenant(ctx context.Context, name, admin string) (client.TenantMintResponse, error) {
	var zero client.TenantMintResponse

	// The driver records the tenant — keys, service creds, canonical JWT
	// — in custody before it pushes; a taken name comes back as
	// ErrTenantExists from the compare-and-set, whichever instance took it.
	acct, err := c.cfg.Driver.MintAccount(ctx, name)
	if err != nil {
		return zero, fmt.Errorf("mint account: %w", err)
	}

	// The admin creds go back to the caller — the only copy; the registry
	// keeps the public key, never the secret.
	adminCreds, err := mint.IssueMember(acct, admin)
	if err != nil {
		return zero, fmt.Errorf("issue admin: %w", err)
	}

	if err := c.provisionMeta(ctx, acct.ServiceCreds, admin, adminCreds.PublicKey); err != nil {
		return zero, fmt.Errorf("provision META: %w", err)
	}

	if c.cfg.OnTenant != nil {
		if err := c.cfg.OnTenant(name, acct.ServiceCreds); err != nil {
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
