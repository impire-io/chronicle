package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
	"github.com/nats-io/nkeys"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/mint"
)

// The membership verbs: the post-mint half of the onboarding design that
// chronicle-15 found missing. Minting wrote identity once and nothing
// could touch it after — a leaked credential could only be outrun by
// destroying the install. These verbs exercise the same doors mint
// opened: the scoped key issues, the registry records, the driver
// re-pushes the account JWT.

// roles a membership can hold — the registry's vocabulary, enforced by the
// API surface at request time, never by key material (decision 0007).
var memberRoles = []string{contract.RoleAdmin, contract.RoleWriter, contract.RoleReader}

// tenantMaterial is the issuance custody one tenant keeps on disk.
type tenantMaterial struct {
	dir          string
	accountPub   string
	scopedSeed   []byte
	serviceCreds []byte
}

// loadTenant reads accounts/<name>; a missing directory is the caller's
// no-such-tenant, anything else is an install problem.
func (c *control) loadTenant(name string) (tenantMaterial, error) {
	dir := filepath.Join(c.cfg.AccountsDir, name)
	pub, err := os.ReadFile(filepath.Join(dir, "account.pub"))
	if errors.Is(err, os.ErrNotExist) {
		return tenantMaterial{}, errNoSuchTenant
	}
	if err != nil {
		return tenantMaterial{}, fmt.Errorf("read account.pub: %w", err)
	}
	scoped, err := os.ReadFile(filepath.Join(dir, "scoped.nk"))
	if err != nil {
		return tenantMaterial{}, fmt.Errorf("read scoped.nk: %w", err)
	}
	svc, err := os.ReadFile(filepath.Join(dir, "service.creds"))
	if err != nil {
		return tenantMaterial{}, fmt.Errorf("read service.creds: %w", err)
	}
	return tenantMaterial{dir: dir, accountPub: string(pub), scopedSeed: scoped, serviceCreds: svc}, nil
}

var errNoSuchTenant = errors.New("no such tenant")

// meta opens the tenant's registry the way provisionMeta left it: through
// the tenant's own service user — control never holds a cross-tenant key.
func (c *control) meta(ctx context.Context, tm tenantMaterial) (jetstream.KeyValue, func(), error) {
	nc, err := mint.ConnectCreds(c.cfg.URL, tm.serviceCreds, "chronicle-control-members")
	if err != nil {
		return nil, nil, fmt.Errorf("connect as service user: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	meta, err := js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("open META: %w", err)
	}
	return meta, nc.Close, nil
}

func (c *control) handleMemberAdd(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var r client.MemberAddRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if !tenantName.MatchString(r.Tenant) {
		_ = req.Error("bad-tenant-name", fmt.Sprintf("tenant name %q: must match [a-z0-9-]+", r.Tenant), nil)
		return
	}
	if !principalName.MatchString(r.Principal) {
		_ = req.Error("bad-principal-name", fmt.Sprintf("principal %q: must match [a-z0-9-]+", r.Principal), nil)
		return
	}
	role := r.Role
	if role == "" {
		role = contract.RoleWriter
	}
	valid := false
	for _, want := range memberRoles {
		if role == want {
			valid = true
		}
	}
	if !valid {
		_ = req.Error("bad-role", fmt.Sprintf("role %q: one of %v", role, memberRoles), nil)
		return
	}

	tm, err := c.loadTenant(r.Tenant)
	if errors.Is(err, errNoSuchTenant) {
		_ = req.Error("no-such-tenant", fmt.Sprintf("tenant %q does not exist", r.Tenant), nil)
		return
	}
	if err != nil {
		c.cfg.Logger.Error("member add", "tenant", r.Tenant, "err", err)
		_ = req.Error("add-failed", err.Error(), nil)
		return
	}

	resp, err := c.addMember(ctx, tm, r.Tenant, r.Principal, role)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			_ = req.Error("member-exists", fmt.Sprintf("principal %q is already a member of %q", r.Principal, r.Tenant), nil)
			return
		}
		c.cfg.Logger.Error("member add", "tenant", r.Tenant, "principal", r.Principal, "err", err)
		_ = req.Error("add-failed", err.Error(), nil)
		return
	}
	respond(req, resp)
}

func (c *control) addMember(ctx context.Context, tm tenantMaterial, tenant, principal, role string) (client.MemberAddResponse, error) {
	var zero client.MemberAddResponse
	acct := &mint.Account{Name: tenant, PublicKey: tm.accountPub, ScopedSeed: tm.scopedSeed}
	creds, err := mint.IssueMember(acct, principal)
	if err != nil {
		return zero, fmt.Errorf("issue member: %w", err)
	}

	// Verify by connecting before anything is recorded: a scoped key that
	// no longer matches the account — a crashed rekey, a hand-edited dir —
	// must fail loudly here, not mint dead credentials.
	probe, err := mint.ConnectCreds(c.cfg.URL, creds.File, "chronicle-member-probe")
	if err != nil {
		return zero, fmt.Errorf("verify member by connecting: %w", err)
	}
	probe.Close()

	meta, done, err := c.meta(ctx, tm)
	if err != nil {
		return zero, err
	}
	defer done()

	// The principal record is the durable identity — it may survive from
	// an earlier, since-revoked membership; keep it. The membership Create
	// is the dedup gate: two adds of one principal cannot both land.
	principalRec, err := json.Marshal(contract.Principal{ID: principal})
	if err != nil {
		return zero, err
	}
	if _, err := meta.Create(ctx, contract.MetaPrincipal(principal), principalRec); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return zero, fmt.Errorf("record principal: %w", err)
	}
	membership, err := json.Marshal(contract.Membership{PublicKey: creds.PublicKey, Role: role})
	if err != nil {
		return zero, err
	}
	if _, err := meta.Create(ctx, contract.MetaMember(principal), membership); err != nil {
		return zero, fmt.Errorf("record membership: %w", err)
	}
	return client.MemberAddResponse{Principal: principal, Role: role, Creds: creds.File}, nil
}

func (c *control) handleMemberRevoke(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var r client.MemberRevokeRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if !tenantName.MatchString(r.Tenant) {
		_ = req.Error("bad-tenant-name", fmt.Sprintf("tenant name %q: must match [a-z0-9-]+", r.Tenant), nil)
		return
	}
	if !principalName.MatchString(r.Principal) {
		_ = req.Error("bad-principal-name", fmt.Sprintf("principal %q: must match [a-z0-9-]+", r.Principal), nil)
		return
	}
	tm, err := c.loadTenant(r.Tenant)
	if errors.Is(err, errNoSuchTenant) {
		_ = req.Error("no-such-tenant", fmt.Sprintf("tenant %q does not exist", r.Tenant), nil)
		return
	}
	if err != nil {
		c.cfg.Logger.Error("member revoke", "tenant", r.Tenant, "err", err)
		_ = req.Error("revoke-failed", err.Error(), nil)
		return
	}

	meta, done, err := c.meta(ctx, tm)
	if err != nil {
		c.cfg.Logger.Error("member revoke", "tenant", r.Tenant, "err", err)
		_ = req.Error("revoke-failed", err.Error(), nil)
		return
	}
	defer done()

	entry, err := meta.Get(ctx, contract.MetaMember(r.Principal))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		_ = req.Error("not-a-member", fmt.Sprintf("principal %q is not a member of %q", r.Principal, r.Tenant), nil)
		return
	}
	if err != nil {
		c.cfg.Logger.Error("member revoke", "tenant", r.Tenant, "err", err)
		_ = req.Error("revoke-failed", err.Error(), nil)
		return
	}
	var m contract.Membership
	if err := json.Unmarshal(entry.Value(), &m); err != nil {
		_ = req.Error("revoke-failed", fmt.Sprintf("decode membership: %v", err), nil)
		return
	}

	// Wire first, registry second: a half-failure leaves a dead credential
	// with a stale record — re-runnable — never a live credential the
	// registry no longer remembers.
	c.claimsMu.Lock()
	err = c.cfg.Driver.RevokeUser(ctx, tm.accountPub, m.PublicKey)
	c.claimsMu.Unlock()
	if err != nil {
		c.cfg.Logger.Error("member revoke", "tenant", r.Tenant, "principal", r.Principal, "err", err)
		_ = req.Error("revoke-failed", err.Error(), nil)
		return
	}
	if err := meta.Delete(ctx, contract.MetaMember(r.Principal)); err != nil {
		c.cfg.Logger.Error("member revoke: registry", "tenant", r.Tenant, "principal", r.Principal, "err", err)
		_ = req.Error("revoke-failed", fmt.Sprintf("credential revoked, but the registry entry remains: %v", err), nil)
		return
	}
	respond(req, client.MemberRevokeResponse{Principal: r.Principal, PublicKey: m.PublicKey})
}

func (c *control) handleMemberRekey(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var r client.MemberRekeyRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if !tenantName.MatchString(r.Tenant) {
		_ = req.Error("bad-tenant-name", fmt.Sprintf("tenant name %q: must match [a-z0-9-]+", r.Tenant), nil)
		return
	}
	tm, err := c.loadTenant(r.Tenant)
	if errors.Is(err, errNoSuchTenant) {
		_ = req.Error("no-such-tenant", fmt.Sprintf("tenant %q does not exist", r.Tenant), nil)
		return
	}
	if err != nil {
		c.cfg.Logger.Error("member rekey", "tenant", r.Tenant, "err", err)
		_ = req.Error("rekey-failed", err.Error(), nil)
		return
	}

	resp, err := c.rekeyMembers(ctx, tm, r.Tenant)
	if err != nil {
		c.cfg.Logger.Error("member rekey", "tenant", r.Tenant, "err", err)
		_ = req.Error("rekey-failed", err.Error(), nil)
		return
	}
	respond(req, resp)
}

func (c *control) rekeyMembers(ctx context.Context, tm tenantMaterial, tenant string) (client.MemberRekeyResponse, error) {
	var zero client.MemberRekeyResponse

	meta, done, err := c.meta(ctx, tm)
	if err != nil {
		return zero, err
	}
	defer done()

	// The registry is the inventory of who must come back after the swap.
	type member struct {
		id   string
		role string
	}
	var members []member
	lister, err := meta.ListKeysFiltered(ctx, contract.MetaMember("*"))
	if err != nil {
		return zero, fmt.Errorf("list memberships: %w", err)
	}
	prefix := contract.MetaMember("")
	for key := range lister.Keys() {
		entry, err := meta.Get(ctx, key)
		if err != nil {
			return zero, fmt.Errorf("read %s: %w", key, err)
		}
		var m contract.Membership
		if err := json.Unmarshal(entry.Value(), &m); err != nil {
			return zero, fmt.Errorf("decode %s: %w", key, err)
		}
		members = append(members, member{id: strings.TrimPrefix(key, prefix), role: m.Role})
	}

	scopedKP, err := nkeys.CreateAccount()
	if err != nil {
		return zero, fmt.Errorf("create scoped key: %w", err)
	}
	scopedPub, err := scopedKP.PublicKey()
	if err != nil {
		return zero, fmt.Errorf("scoped public key: %w", err)
	}
	scopedSeed, err := scopedKP.Seed()
	if err != nil {
		return zero, fmt.Errorf("scoped seed: %w", err)
	}

	// Custody before the wire: once the push lands, the old seed issues
	// nothing — the new one must already be what the install remembers. A
	// crash between the two is repaired by running the rekey again.
	if err := os.WriteFile(filepath.Join(tm.dir, "scoped.nk"), scopedSeed, 0o600); err != nil {
		return zero, fmt.Errorf("persist scoped.nk: %w", err)
	}
	c.claimsMu.Lock()
	err = c.cfg.Driver.RotateScopedSigner(ctx, tm.accountPub, scopedPub)
	c.claimsMu.Unlock()
	if err != nil {
		return zero, fmt.Errorf("rotate scoped signer: %w", err)
	}

	acct := &mint.Account{Name: tenant, PublicKey: tm.accountPub, ScopedSeed: scopedSeed}
	resp := client.MemberRekeyResponse{}
	for _, m := range members {
		creds, err := mint.IssueMember(acct, m.id)
		if err != nil {
			return zero, fmt.Errorf("re-issue %s: %w", m.id, err)
		}
		membership, err := json.Marshal(contract.Membership{PublicKey: creds.PublicKey, Role: m.role})
		if err != nil {
			return zero, err
		}
		if _, err := meta.Put(ctx, contract.MetaMember(m.id), membership); err != nil {
			return zero, fmt.Errorf("update membership %s: %w", m.id, err)
		}
		resp.Members = append(resp.Members, client.MemberAddResponse{Principal: m.id, Role: m.role, Creds: creds.File})
	}

	// Verify by connecting with one re-issued credential: the push settled
	// before it returned, so a refusal here is a real break, not a race.
	if len(resp.Members) > 0 {
		probe, err := mint.ConnectCreds(c.cfg.URL, resp.Members[0].Creds, "chronicle-rekey-probe")
		if err != nil {
			return zero, fmt.Errorf("verify re-issued member by connecting: %w", err)
		}
		probe.Close()
	}
	return resp, nil
}

func respond(req micro.Request, v any) {
	reply, err := json.Marshal(v)
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}
