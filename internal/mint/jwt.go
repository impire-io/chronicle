package mint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/impire-io/chronicle/contract"
)

// claimsUpdateSubject is the full resolver's runtime account-creation door.
const claimsUpdateSubject = "$SYS.REQ.CLAIMS.UPDATE"

// accountLookupSubject is the read half of the same door: the resolver
// answers with the account's current JWT. Custody keeps the canonical copy
// (design 10); the lookup is how Reconcile learns the resolver disagrees.
const accountLookupSubject = "$SYS.REQ.ACCOUNT.%s.CLAIMS.LOOKUP"

// JWTDriver mints accounts on a self-hosted operator-mode NATS install:
// chronicle's signing key trusted in the operator JWT, full resolver,
// system-account credentials (onboarding design § setup) — reading every
// key it signs with from the AUTH bucket (design 10), so any number of
// control instances hold one each over the same custody.
type JWTDriver struct {
	// Custody is the bucket: the operator signing key, and per tenant the
	// keys, the service creds, and the canonical account JWT.
	Custody *Custody
	// SysConn is a connection authenticated as a system-account user; the
	// capability to manage every account on the substrate. It never
	// approaches a tenant. Nil means no pushes and no reconcile — a
	// driver that can only read, which some tests are.
	SysConn *nats.Conn
	// URL is the client URL the verify-by-connecting step dials.
	URL string
	// ControlAccountPub names the bridge's exporter: every minted tenant
	// imports the fleet-report service from it, stamped with the tenant's
	// own name (06-scheduler.md § the dispatch surface). Empty mints no
	// import — a substrate without the fleet machinery still mints.
	ControlAccountPub string
	// Limits are the explicit JetStream limits every minted account gets —
	// a fresh account has JetStream off until its JWT says otherwise. Zero
	// means unlimited on the dev substrate.
	Limits jwt.JetStreamLimits
}

// NewJWTDriver builds the driver over an opened custody. The control
// account's public key comes from custody when it is there (the bridge
// import) and stays empty otherwise.
func NewJWTDriver(ctx context.Context, c *Custody, sysConn *nats.Conn, url string) (*JWTDriver, error) {
	d := &JWTDriver{Custody: c, SysConn: sysConn, URL: url}
	acct, _, err := c.Account(ctx, "CONTROL")
	switch {
	case err == nil:
		d.ControlAccountPub = acct.PublicKey
	case errors.Is(err, ErrNoRecord):
	default:
		return nil, err
	}
	return d, nil
}

// casAttempts bounds the re-read-and-recompute loop of a mutation. Eight
// instances racing one tenant is the tested case; five retries is far past
// it and still finite.
const casAttempts = 5

// MintAccount signs the account JWT, records the tenant in custody, pushes
// the JWT to the claims-update subject, and verifies by connecting before
// reporting success. Custody first, then the push: the bucket is
// canonical and the resolver follows it (a push that never lands is
// repaired by Reconcile; a mint that cannot land at all takes its record
// back).
func (d *JWTDriver) MintAccount(ctx context.Context, name string) (*Account, error) {
	akp, err := nkeys.CreateAccount()
	if err != nil {
		return nil, fmt.Errorf("create account key: %w", err)
	}
	apub, err := akp.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("account public key: %w", err)
	}
	signingKP, err := nkeys.CreateAccount()
	if err != nil {
		return nil, fmt.Errorf("create signing key: %w", err)
	}
	signingPub, err := signingKP.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("signing public key: %w", err)
	}
	scopedKP, err := nkeys.CreateAccount()
	if err != nil {
		return nil, fmt.Errorf("create scoped key: %w", err)
	}
	scopedPub, err := scopedKP.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("scoped public key: %w", err)
	}

	ac := jwt.NewAccountClaims(apub)
	ac.Name = name
	limits := d.Limits
	if (jwt.JetStreamLimits{}) == limits {
		limits = jwt.JetStreamLimits{
			MemoryStorage: -1, DiskStorage: -1, Streams: -1, Consumer: -1,
		}
	}
	ac.Limits.JetStreamLimits = limits
	ac.SigningKeys.Add(signingPub)
	ac.SigningKeys.AddScopedSigner(memberScope(scopedPub))
	if d.ControlAccountPub != "" {
		// The bridge import: the tenant publishes the local subject, the
		// server maps it to the stamped form — unforgeable, because this
		// JWT is chronicle's to sign and the tenant never holds the pen.
		ac.Imports.Add(&jwt.Import{
			Name:         "chronicle-fleet-bridge",
			Type:         jwt.Service,
			Account:      d.ControlAccountPub,
			Subject:      jwt.Subject(contract.FleetBridgeSubjectFor(name)),
			LocalSubject: jwt.RenamingSubject(contract.FleetBridgeLocalSubject),
		})
	}
	token, err := d.sign(ctx, ac)
	if err != nil {
		return nil, err
	}

	signingSeed, err := signingKP.Seed()
	if err != nil {
		return nil, fmt.Errorf("signing seed: %w", err)
	}
	scopedSeed, err := scopedKP.Seed()
	if err != nil {
		return nil, fmt.Errorf("scoped seed: %w", err)
	}
	acct := &Account{
		Name:        name,
		PublicKey:   apub,
		SigningSeed: signingSeed,
		ScopedSeed:  scopedSeed,
	}
	svc, err := IssueServiceUser(acct, "chronicle-node")
	if err != nil {
		return nil, fmt.Errorf("issue service user: %w", err)
	}
	acct.ServiceCreds = svc.File

	// Custody before the wire.
	rec := tenantRecord(acct, token)
	if _, err := d.Custody.PutTenant(ctx, rec, 0); err != nil {
		if errors.Is(err, ErrRevisionMismatch) {
			return nil, fmt.Errorf("%w: %s", ErrTenantExists, name)
		}
		return nil, err
	}
	if err := d.push(ctx, token); err != nil {
		_ = d.Custody.DeleteTenant(ctx, name)
		return nil, fmt.Errorf("push account %s: %w", name, err)
	}

	// Verify by connecting: the same could-not-succeed-if-broken read the
	// quality posture demands. A throwaway user, dialed and discarded.
	probe, err := IssueServiceUser(acct, "mint-probe")
	if err != nil {
		return nil, fmt.Errorf("issue mint probe: %w", err)
	}
	nc, err := ConnectCreds(d.URL, probe.File, "chronicle-mint-probe")
	if err != nil {
		return nil, fmt.Errorf("verify account %s by connecting: %w", name, err)
	}
	nc.Close()
	return acct, nil
}

// Tenant reads one tenant from custody.
func (d *JWTDriver) Tenant(ctx context.Context, name string) (*Account, error) {
	rec, _, err := d.Custody.Tenant(ctx, name)
	if err != nil {
		if errors.Is(err, ErrNoRecord) {
			return nil, fmt.Errorf("%w: %s", ErrNoSuchTenant, name)
		}
		return nil, err
	}
	return accountOf(rec), nil
}

// Tenants lists custody's tenants.
func (d *JWTDriver) Tenants(ctx context.Context) ([]string, error) {
	return d.Custody.Tenants(ctx)
}

// RevokeUser adds the user to the account's revocation list, lands the
// re-signed JWT in custody by compare-and-set, and pushes it: new
// connections are refused and live ones are actively closed. The registry
// entry is the caller's to retire — the wire dies here.
func (d *JWTDriver) RevokeUser(ctx context.Context, tenant, userPub string) error {
	_, err := d.mutate(ctx, tenant, func(ac *jwt.AccountClaims, _ *TenantRecord) error {
		ac.Revoke(userPub)
		return nil
	})
	return err
}

// RotateScopedSigner swaps the member-issuing scoped key: every scoped
// signer is dropped and a fresh one takes their place in one push, so
// eviction of the old key's users and acceptance of the new key's flip
// together; the new seed lands in custody in the same compare-and-set as
// the JWT. The plain signing key — the service user's issuer — survives.
func (d *JWTDriver) RotateScopedSigner(ctx context.Context, tenant string) ([]byte, error) {
	var newSeed []byte
	_, err := d.mutate(ctx, tenant, func(ac *jwt.AccountClaims, rec *TenantRecord) error {
		kp, err := nkeys.CreateAccount()
		if err != nil {
			return fmt.Errorf("create scoped key: %w", err)
		}
		pub, err := kp.PublicKey()
		if err != nil {
			return fmt.Errorf("scoped public key: %w", err)
		}
		seed, err := kp.Seed()
		if err != nil {
			return fmt.Errorf("scoped seed: %w", err)
		}
		for existing, scope := range ac.SigningKeys {
			if scope != nil {
				ac.SigningKeys.Remove(existing)
			}
		}
		ac.SigningKeys.AddScopedSigner(memberScope(pub))
		rec.ScopedSeed = string(seed)
		newSeed = seed
		return nil
	})
	return newSeed, err
}

// mutate is every claims mutation's shape: read the tenant at a revision,
// let edit change the claims (and the record beside them), sign, land the
// new record by compare-and-set at that revision, and only then push. A
// lost race re-reads and recomputes on top of the winner — nothing is ever
// overwritten, and no instance need know another exists.
func (d *JWTDriver) mutate(ctx context.Context, tenant string, edit func(*jwt.AccountClaims, *TenantRecord) error) (string, error) {
	for attempt := 0; attempt < casAttempts; attempt++ {
		rec, rev, err := d.Custody.Tenant(ctx, tenant)
		if err != nil {
			if errors.Is(err, ErrNoRecord) {
				return "", fmt.Errorf("%w: %s", ErrNoSuchTenant, tenant)
			}
			return "", err
		}
		ac, err := jwt.DecodeAccountClaims(rec.JWT)
		if err != nil {
			return "", fmt.Errorf("decode account claims of %s: %w", tenant, err)
		}
		if err := edit(ac, &rec); err != nil {
			return "", err
		}
		token, err := d.sign(ctx, ac)
		if err != nil {
			return "", err
		}
		rec.JWT = token
		if _, err := d.Custody.PutTenant(ctx, rec, rev); err != nil {
			if errors.Is(err, ErrRevisionMismatch) {
				continue
			}
			return "", err
		}
		if err := d.push(ctx, token); err != nil {
			return "", fmt.Errorf("push account %s: %w", tenant, err)
		}
		return token, nil
	}
	return "", fmt.Errorf("account %s: %d writers raced this mutation and it never landed; retry", tenant, casAttempts)
}

// Reconcile compares every account's canonical JWT in custody with the
// resolver's and pushes the bucket's where they differ. Run at boot and on
// a slow timer by every instance: a push that never landed — an instance
// that won the compare-and-set and died — is repaired by the next boot.
func (d *JWTDriver) Reconcile(ctx context.Context) ([]string, error) {
	if d.SysConn == nil {
		return nil, nil
	}
	type acct struct{ name, pub, token string }
	var accts []acct
	for _, name := range []string{"SYS", "CONTROL", "AUTH"} {
		rec, _, err := d.Custody.Account(ctx, name)
		if errors.Is(err, ErrNoRecord) {
			continue
		}
		if err != nil {
			return nil, err
		}
		accts = append(accts, acct{name, rec.PublicKey, rec.JWT})
	}
	names, err := d.Custody.Tenants(ctx)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		rec, _, err := d.Custody.Tenant(ctx, name)
		if err != nil {
			return nil, err
		}
		accts = append(accts, acct{name, rec.PublicKey, rec.JWT})
	}
	var pushed []string
	for _, a := range accts {
		if a.token == "" {
			continue
		}
		served, err := d.lookupAccountJWT(ctx, a.pub)
		if err != nil {
			return pushed, fmt.Errorf("reconcile %s: %w", a.name, err)
		}
		if served == a.token {
			continue
		}
		if err := d.push(ctx, a.token); err != nil {
			return pushed, fmt.Errorf("reconcile %s: %w", a.name, err)
		}
		pushed = append(pushed, a.name)
	}
	return pushed, nil
}

// sign encodes account claims under the operator signing key custody
// currently names — read at every use, so a rotation takes effect on the
// next signature.
func (d *JWTDriver) sign(ctx context.Context, ac *jwt.AccountClaims) (string, error) {
	op, _, err := d.Custody.Operator(ctx)
	if err != nil {
		return "", fmt.Errorf("operator signing key: %w", err)
	}
	oskp, err := nkeys.FromSeed([]byte(op.SigningSeed))
	if err != nil {
		return "", fmt.Errorf("operator signing seed: %w", err)
	}
	token, err := ac.Encode(oskp)
	if err != nil {
		return "", fmt.Errorf("encode account jwt: %w", err)
	}
	return token, nil
}

func tenantRecord(acct *Account, token string) TenantRecord {
	return TenantRecord{
		Name:         acct.Name,
		PublicKey:    acct.PublicKey,
		SigningSeed:  string(acct.SigningSeed),
		ScopedSeed:   string(acct.ScopedSeed),
		ServiceCreds: string(acct.ServiceCreds),
		JWT:          token,
	}
}

func accountOf(rec TenantRecord) *Account {
	return &Account{
		Name:         rec.Name,
		PublicKey:    rec.PublicKey,
		SigningSeed:  []byte(rec.SigningSeed),
		ScopedSeed:   []byte(rec.ScopedSeed),
		ServiceCreds: []byte(rec.ServiceCreds),
	}
}

// memberScope is the scoped signer both mint and rotation install: one
// construction, so the member baseline cannot drift between the two.
func memberScope(scopedPub string) *jwt.UserScope {
	scope := jwt.NewUserScope()
	scope.Key = scopedPub
	scope.Role = "member"
	scope.Description = "the member baseline: granted once, at account creation"
	scope.Template = MemberBaseline()
	return scope
}

// lookupAccountJWT reads the account's current JWT from the resolver —
// the raw token, so it compares byte-for-byte with custody's. An empty
// token means the resolver does not know the account.
func (d *JWTDriver) lookupAccountJWT(ctx context.Context, accountPub string) (string, error) {
	msg, err := d.SysConn.RequestWithContext(ctx, fmt.Sprintf(accountLookupSubject, accountPub), nil)
	if err != nil {
		return "", fmt.Errorf("claims lookup: %w", err)
	}
	return string(msg.Data), nil
}

func (d *JWTDriver) push(ctx context.Context, accountJWT string) error {
	if d.SysConn == nil {
		return fmt.Errorf("this driver holds no system connection and cannot push")
	}
	return pushAccount(ctx, d.SysConn, accountJWT)
}

// MemberBaseline is the scoped key's permission template: every member can
// append, replay, and read state; no member can create or delete streams,
// purge, or write KV — the 0003 ownership boundary held at the wire. The
// template replaces whatever the user JWT claims.
func MemberBaseline() jwt.UserPermissionLimits {
	return jwt.UserPermissionLimits{
		Permissions: jwt.Permissions{
			Pub: jwt.Permission{Allow: jwt.StringList{
				contract.Root + ".>",
				// Whoami: a member may ask the server who it is — the
				// bridge dial reads its principal from the answer, and
				// the reply names only the caller's own identity.
				"$SYS.REQ.USER.INFO",
				// The read side of the JetStream API: consumers for replay
				// and watch, stream and bucket handles, message gets.
				"$JS.API.CONSUMER.>",
				"$JS.API.STREAM.INFO.>",
				"$JS.API.STREAM.NAMES",
				"$JS.API.STREAM.MSG.GET.>",
				"$JS.API.DIRECT.GET.>",
			}},
			Sub: jwt.Permission{Allow: jwt.StringList{
				contract.Root + ".>",
				"_INBOX.>",
			}},
		},
		Limits: jwt.Limits{
			NatsLimits: jwt.NatsLimits{Subs: -1, Data: -1, Payload: -1},
		},
	}
}

// ConnectCreds dials with in-memory creds content — no file on disk needed.
func ConnectCreds(url string, creds []byte, name string) (*nats.Conn, error) {
	token, err := jwt.ParseDecoratedJWT(creds)
	if err != nil {
		return nil, fmt.Errorf("parse creds jwt: %w", err)
	}
	kp, err := jwt.ParseDecoratedUserNKey(creds)
	if err != nil {
		return nil, fmt.Errorf("parse creds nkey: %w", err)
	}
	return nats.Connect(url,
		nats.Name(name),
		nats.UserJWT(
			func() (string, error) { return token, nil },
			func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) },
		),
		nats.Timeout(5*time.Second),
	)
}
