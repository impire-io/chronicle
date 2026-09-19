// Package mint is the minting seam of the onboarding design
// (chronicle-hq/02-DESIGN/01-onboarding.md): a chronicle install is
// configured with exactly one driver, and everything above the seam sees
// "an account was created, here is its signing key." The skeleton carries
// the jwt driver; the synadia driver arrives with a consumer that needs it.
package mint

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// Account is what a driver hands back: the minted tenant account and the
// issuance material. Users are minted offline from it, the same way under
// every driver.
type Account struct {
	Name      string
	PublicKey string
	// SigningSeed issues users with account-default permissions — the
	// tenant service user. It is chronicle's issuance credential for this
	// tenant; compromise is contained to one tenant.
	SigningSeed []byte
	// ScopedSeed is the member-baseline scoped key: every member gets the
	// same wire-level rights, and no key is ever touched again.
	ScopedSeed []byte
	// ServiceCreds is the tenant's service user — issued at mint under the
	// signing key, held by the fleet for the tenant's node and indexers.
	ServiceCreds []byte
}

// Driver mints tenant accounts on one substrate and keeps their issuance
// material in shared custody (design 10). MintAccount creates the account
// with explicit JetStream limits, establishes the keys, records the tenant,
// and verifies by connecting — a successful push does not prove trust.
//
// The mutation verbs are the onboarding design's revocation row held at
// the seam: account-claims surgery is driver-specific (the jwt driver edits
// the canonical JWT in custody by compare-and-set and re-pushes it; a vendor
// driver would call the vendor's API), while everything above the seam
// speaks in tenant names and public keys. Every instance of control holds a
// driver over the same custody, so none of these verbs assumes it is alone.
type Driver interface {
	MintAccount(ctx context.Context, name string) (*Account, error)
	// Tenant reads one tenant's material; ErrNoSuchTenant when absent.
	Tenant(ctx context.Context, name string) (*Account, error)
	// Tenants lists every tenant in custody.
	Tenants(ctx context.Context) ([]string, error)
	// RevokeUser invalidates one user credential: new connections are
	// refused and live ones are evicted.
	RevokeUser(ctx context.Context, tenant, userPub string) error
	// RotateScopedSigner replaces the tenant's member-issuing scoped key,
	// evicting every user the old key signed, and returns the new seed so
	// the caller re-issues members. The plain signing key — and the
	// service user it signed — stays untouched.
	RotateScopedSigner(ctx context.Context, tenant string) ([]byte, error)
	// Reconcile pushes every account whose canonical JWT in custody differs
	// from what the resolver serves — the boot-time and periodic repair of
	// a push that never landed. It returns the accounts it pushed.
	Reconcile(ctx context.Context) ([]string, error)
}

// ErrNoSuchTenant says custody holds no tenant of that name.
var ErrNoSuchTenant = fmt.Errorf("no such tenant")

// ErrTenantExists says a mint found the name taken.
var ErrTenantExists = fmt.Errorf("tenant already exists")

// Creds is an issued user: the decorated .creds content and the user's
// public key (the membership registry records it).
type Creds struct {
	PublicKey string
	File      []byte
}

// IssueServiceUser mints the tenant's service user offline: issued under
// the account signing key, unscoped, so it carries account-default rights.
// Only chronicle's own workloads hold it.
func IssueServiceUser(acct *Account, name string) (Creds, error) {
	return issueUser(acct.PublicKey, acct.SigningSeed, name, false)
}

// IssueMember mints a member's user offline under the member-baseline
// scoped key. The user JWT is permission-empty — the scope's template
// replaces whatever a user JWT would claim — and its name carries the
// principal ID, so every connection already says who it is.
func IssueMember(acct *Account, principalID string) (Creds, error) {
	return issueUser(acct.PublicKey, acct.ScopedSeed, principalID, true)
}

func issueUser(accountPub string, issuerSeed []byte, name string, scoped bool) (Creds, error) {
	ukp, err := nkeys.CreateUser()
	if err != nil {
		return Creds{}, fmt.Errorf("create user key: %w", err)
	}
	upub, err := ukp.PublicKey()
	if err != nil {
		return Creds{}, fmt.Errorf("user public key: %w", err)
	}
	uc := jwt.NewUserClaims(upub)
	uc.Name = name
	// The issuer is a signing key, not the account key: the JWT must name
	// the account it belongs to.
	uc.IssuerAccount = accountPub
	if scoped {
		// A user minted under a scoped key must be permission-empty or it
		// fails validation; the scope's template replaces, never merges.
		uc.UserPermissionLimits = jwt.UserPermissionLimits{}
	}
	ikp, err := nkeys.FromSeed(issuerSeed)
	if err != nil {
		return Creds{}, fmt.Errorf("issuer seed: %w", err)
	}
	token, err := uc.Encode(ikp)
	if err != nil {
		return Creds{}, fmt.Errorf("encode user jwt: %w", err)
	}
	seed, err := ukp.Seed()
	if err != nil {
		return Creds{}, fmt.Errorf("user seed: %w", err)
	}
	file, err := jwt.FormatUserConfig(token, seed)
	if err != nil {
		return Creds{}, fmt.Errorf("format creds: %w", err)
	}
	return Creds{PublicKey: upub, File: file}, nil
}

// IssueBridgeUser signs a callout placement (decision 0026): the
// server-generated user nkey placed into the tenant under the
// member-baseline scoped key, the principal ID as its name so every
// connection says who it is, expiring — the bridge's users expire instead
// of being revoked, and the TTL is the revocation bound. Only the JWT
// travels; the server holds the key.
func IssueBridgeUser(accountPub string, scopedSeed []byte, principalID, userNkey string, ttl time.Duration) (string, error) {
	uc := jwt.NewUserClaims(userNkey)
	uc.Name = principalID
	uc.IssuerAccount = accountPub
	// Scoped-key users must be permission-empty; the scope's template
	// replaces, never merges.
	uc.UserPermissionLimits = jwt.UserPermissionLimits{}
	uc.Expires = time.Now().Add(ttl).Unix()
	ikp, err := nkeys.FromSeed(scopedSeed)
	if err != nil {
		return "", fmt.Errorf("scoped seed: %w", err)
	}
	token, err := uc.Encode(ikp)
	if err != nil {
		return "", fmt.Errorf("encode bridge user jwt: %w", err)
	}
	return token, nil
}
