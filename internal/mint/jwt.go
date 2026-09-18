package mint

import (
	"context"
	"encoding/json"
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
// answers with the account's current JWT. Mutations must start from it —
// chronicle keeps no copy of the account JWT, and rebuilding claims from
// the mint template would silently drop everything added since.
const accountLookupSubject = "$SYS.REQ.ACCOUNT.%s.CLAIMS.LOOKUP"

// JWTDriver mints accounts on a self-hosted operator-mode NATS install:
// chronicle's signing key trusted in the operator JWT, full resolver,
// system-account credentials (onboarding design § setup).
type JWTDriver struct {
	// OperatorSigningSeed is the day-zero artifact: it must exist in the
	// operator JWT before the fleet boots.
	OperatorSigningSeed []byte
	// SysConn is a connection authenticated as a system-account user; the
	// capability to manage every account on the substrate. It never
	// approaches a tenant.
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

// MintAccount signs the account JWT, pushes it to the claims-update
// subject, and verifies by connecting before reporting success.
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

	if err := d.resign(ctx, ac); err != nil {
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

// RevokeUser adds the user to the account's revocation list and re-pushes:
// new connections are refused and live ones are actively closed. The
// registry entry is the caller's to retire — the wire dies here.
func (d *JWTDriver) RevokeUser(ctx context.Context, accountPub, userPub string) error {
	ac, err := d.lookupAccountClaims(ctx, accountPub)
	if err != nil {
		return err
	}
	ac.Revoke(userPub)
	return d.resign(ctx, ac)
}

// RotateScopedSigner swaps the member-issuing scoped key: every scoped
// signer is dropped and newScopedPub takes their place in one push, so
// eviction of the old key's users and acceptance of the new key's flip
// together. The plain signing key — the service user's issuer — survives.
func (d *JWTDriver) RotateScopedSigner(ctx context.Context, accountPub, newScopedPub string) error {
	ac, err := d.lookupAccountClaims(ctx, accountPub)
	if err != nil {
		return err
	}
	for pub, scope := range ac.SigningKeys {
		if scope != nil {
			ac.SigningKeys.Remove(pub)
		}
	}
	ac.SigningKeys.AddScopedSigner(memberScope(newScopedPub))
	return d.resign(ctx, ac)
}

// lookupAccountClaims reads the account's current JWT from the resolver.
// The reply is the raw JWT — an empty payload means the resolver does not
// know the account (a different shape than the update door's JSON).
func (d *JWTDriver) lookupAccountClaims(ctx context.Context, accountPub string) (*jwt.AccountClaims, error) {
	msg, err := d.SysConn.RequestWithContext(ctx, fmt.Sprintf(accountLookupSubject, accountPub), nil)
	if err != nil {
		return nil, fmt.Errorf("claims lookup: %w", err)
	}
	if len(msg.Data) == 0 {
		return nil, fmt.Errorf("claims lookup: account %s not known to the resolver", accountPub)
	}
	ac, err := jwt.DecodeAccountClaims(string(msg.Data))
	if err != nil {
		return nil, fmt.Errorf("decode account claims: %w", err)
	}
	return ac, nil
}

func (d *JWTDriver) resign(ctx context.Context, ac *jwt.AccountClaims) error {
	oskp, err := nkeys.FromSeed(d.OperatorSigningSeed)
	if err != nil {
		return fmt.Errorf("operator signing seed: %w", err)
	}
	token, err := ac.Encode(oskp)
	if err != nil {
		return fmt.Errorf("re-encode account jwt: %w", err)
	}
	if err := d.push(ctx, token); err != nil {
		return fmt.Errorf("push account %s: %w", ac.Name, err)
	}
	return nil
}

func (d *JWTDriver) push(ctx context.Context, accountJWT string) error {
	msg, err := d.SysConn.RequestWithContext(ctx, claimsUpdateSubject, []byte(accountJWT))
	if err != nil {
		return fmt.Errorf("claims update: %w", err)
	}
	var resp struct {
		Error *struct {
			Code        int    `json:"code"`
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return fmt.Errorf("claims update response: %w", err)
	}
	if resp.Error != nil {
		return fmt.Errorf("claims update refused: %d %s", resp.Error.Code, resp.Error.Description)
	}
	return nil
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
