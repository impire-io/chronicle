package mint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// The environment's half of first boot (chronicle-hq/02-DESIGN/10-custody.md
// § first boot, decisions 0031 and 0032): chronicle-ops births the operator
// identity and its signing key and the SYS and CONTROL accounts as bare
// keys, renders the node configs with those bare accounts preloaded, and
// hands the service three seeds. The service's seal does the rest — and
// holds nothing of its own afterwards.

// Material is what the environment hands the service at seal: the
// operator signing seed and the two account seeds. The operator identity
// never travels.
type Material struct {
	OperatorSigningSeed []byte
	SystemAccountSeed   []byte
	ControlAccountSeed  []byte
}

// ReadMaterial loads the three seed files.
func ReadMaterial(signingSeed, sysSeed, controlSeed string) (Material, error) {
	read := func(what, path string, create func() (nkeys.KeyPair, error)) ([]byte, error) {
		if path == "" {
			return nil, fmt.Errorf("seal needs the %s seed file", what)
		}
		seed, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read the %s seed: %w", what, err)
		}
		kp, err := nkeys.FromSeed(seed)
		if err != nil {
			return nil, fmt.Errorf("%s seed %s: %w", what, path, err)
		}
		want, _ := create()
		if wantPub, _ := want.PublicKey(); wantPub != "" {
			gotPub, _ := kp.PublicKey()
			if gotPub[0] != wantPub[0] {
				return nil, fmt.Errorf("%s seed %s is a %c key, not a %c key", what, path, gotPub[0], wantPub[0])
			}
		}
		return seed, nil
	}
	var m Material
	var err error
	if m.OperatorSigningSeed, err = read("operator signing", signingSeed, nkeys.CreateOperator); err != nil {
		return m, err
	}
	if m.SystemAccountSeed, err = read("SYS account", sysSeed, nkeys.CreateAccount); err != nil {
		return m, err
	}
	if m.ControlAccountSeed, err = read("CONTROL account", controlSeed, nkeys.CreateAccount); err != nil {
		return m, err
	}
	return m, nil
}

// GenerateMaterial is the environment's birth in one call — what `nsc`
// does by hand: an operator identity whose JWT trusts one signing key, and
// the SYS and CONTROL accounts as bare keys with bare JWTs. It returns the
// seeds the service is handed and the public material a node config is
// rendered from. The dev shape's own generator (`up`) stamps the shapes at
// birth instead; this one leaves them for seal, which is the point.
func GenerateMaterial() (Material, *Bootstrap, error) {
	okp, opub, err := newKey(nkeys.CreateOperator)
	if err != nil {
		return Material{}, nil, fmt.Errorf("operator key: %w", err)
	}
	oskp, ospub, err := newKey(nkeys.CreateOperator)
	if err != nil {
		return Material{}, nil, fmt.Errorf("operator signing key: %w", err)
	}
	sakp, sapub, err := newKey(nkeys.CreateAccount)
	if err != nil {
		return Material{}, nil, fmt.Errorf("system account key: %w", err)
	}
	cakp, capub, err := newKey(nkeys.CreateAccount)
	if err != nil {
		return Material{}, nil, fmt.Errorf("control account key: %w", err)
	}
	oc := jwt.NewOperatorClaims(opub)
	oc.Name = "chronicle"
	oc.SigningKeys.Add(ospub)
	oc.SystemAccount = sapub
	operatorJWT, err := oc.Encode(okp)
	if err != nil {
		return Material{}, nil, fmt.Errorf("encode operator jwt: %w", err)
	}
	sac := jwt.NewAccountClaims(sapub)
	sac.Name = "SYS"
	sysJWT, err := sac.Encode(oskp)
	if err != nil {
		return Material{}, nil, fmt.Errorf("encode system account jwt: %w", err)
	}
	cac := jwt.NewAccountClaims(capub)
	cac.Name = "CONTROL"
	controlJWT, err := cac.Encode(oskp)
	if err != nil {
		return Material{}, nil, fmt.Errorf("encode control account jwt: %w", err)
	}
	oSeed, _ := okp.Seed()
	osSeed, _ := oskp.Seed()
	saSeed, _ := sakp.Seed()
	caSeed, _ := cakp.Seed()
	m := Material{OperatorSigningSeed: osSeed, SystemAccountSeed: saSeed, ControlAccountSeed: caSeed}
	b := &Bootstrap{
		OperatorJWT:       operatorJWT,
		OperatorSeed:      oSeed,
		SystemAccountPub:  sapub,
		SystemAccountJWT:  sysJWT,
		ControlAccountPub: capub,
		ControlAccountJWT: controlJWT,
	}
	return m, b, nil
}

// SealFromMaterial is `chronicle operator seal --url`: the service's one
// birth ceremony, handed the environment's seeds against a cluster whose
// resolver preloads the bare accounts. It issues the first control
// instance's users from the seeds and connects with them; stamps the
// account shapes the service defines — CONTROL's JetStream limits and the
// bridge export — by re-signing and pushing; births the AUTH account
// (until the fold, decision 0030 point 6) and pushes it; creates the
// bucket, writes every working key, reads each back, and records the
// first instance on the accounts. The bundle comes back for the caller
// to write where the host reads it. Idempotent: against a bucket that
// already exists it verifies by public key that the material is the
// bucket's, and writes nothing.
func SealFromMaterial(ctx context.Context, url string, m Material, opts SealOptions) (SealReport, Bundle, *Custody, error) {
	var rep SealReport
	oskp, err := nkeys.FromSeed(m.OperatorSigningSeed)
	if err != nil {
		return rep, Bundle{}, nil, fmt.Errorf("operator signing seed: %w", err)
	}
	opPub, err := oskp.PublicKey()
	if err != nil {
		return rep, Bundle{}, nil, err
	}
	sysPub, err := PublicKeyOfSeed(m.SystemAccountSeed)
	if err != nil {
		return rep, Bundle{}, nil, fmt.Errorf("system account seed: %w", err)
	}
	ctrlPub, err := PublicKeyOfSeed(m.ControlAccountSeed)
	if err != nil {
		return rep, Bundle{}, nil, fmt.Errorf("control account seed: %w", err)
	}
	b := &Bootstrap{
		OperatorSigningSeed: m.OperatorSigningSeed,
		SystemAccountPub:    sysPub,
		SystemAccountSeed:   m.SystemAccountSeed,
		ControlAccountPub:   ctrlPub,
		ControlAccountSeed:  m.ControlAccountSeed,
	}

	// The first instance, issued from the seeds: the connections the
	// ceremony runs over and the bundle the first host takes.
	ctrlUser, err := b.IssueControlUser(firstInstance, ControlInstanceTemplate())
	if err != nil {
		return rep, Bundle{}, nil, err
	}
	sysUser, err := b.IssueSystemUser(firstInstance)
	if err != nil {
		return rep, Bundle{}, nil, err
	}
	bundle := Bundle{ControlCreds: ctrlUser.File, SysCreds: sysUser.File}
	sysConn, err := ConnectCreds(url, bundle.SysCreds, "chronicle-seal-sys")
	if err != nil {
		return rep, Bundle{}, nil, fmt.Errorf("connect %s as the first instance's system user (does the resolver preload the bare SYS account?): %w", url, err)
	}
	defer sysConn.Close()
	ctrlConn, err := ConnectCreds(url, bundle.ControlCreds, "chronicle-seal")
	if err != nil {
		return rep, Bundle{}, nil, fmt.Errorf("connect %s as the first instance's control user (does the resolver preload the bare CONTROL account?): %w", url, err)
	}
	defer ctrlConn.Close()

	// The accounts as the environment preloaded them, then CONTROL
	// stamped with the service's shape and pushed — before anything else,
	// because a bare CONTROL account has no JetStream and the bucket
	// cannot even be looked for in it.
	b.SystemAccountJWT, err = lookupAccountJWT(ctx, sysConn, sysPub)
	if err != nil {
		return rep, Bundle{}, nil, fmt.Errorf("the resolver does not serve the SYS account %s (the environment preloads the bare accounts before seal): %w", sysPub, err)
	}
	b.ControlAccountJWT, err = lookupAccountJWT(ctx, sysConn, ctrlPub)
	if err != nil {
		return rep, Bundle{}, nil, fmt.Errorf("the resolver does not serve the CONTROL account %s (the environment preloads the bare accounts before seal): %w", ctrlPub, err)
	}
	stamped, changed, err := stampControlShape(b.ControlAccountJWT, oskp)
	if err != nil {
		return rep, Bundle{}, nil, err
	}
	if changed {
		if err := pushAccount(ctx, sysConn, stamped); err != nil {
			return rep, Bundle{}, nil, fmt.Errorf("push the stamped CONTROL account: %w", err)
		}
		b.ControlAccountJWT = stamped
	}

	// A bucket that exists is verified, never re-sealed. The stamp lands
	// in the node's account a beat after the push answers.
	var c *Custody
	for attempt := 0; ; attempt++ {
		c, err = OpenCustody(ctx, ctrlConn)
		if err == nil || errors.Is(err, ErrNotSealed) || attempt >= 20 {
			break
		}
		select {
		case <-ctx.Done():
			return rep, Bundle{}, nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	switch {
	case err == nil:
		rep, err = verifyByPublicKeys(ctx, c, opPub, map[string]string{"SYS": sysPub, "CONTROL": ctrlPub})
		return rep, Bundle{}, c, err
	case errors.Is(err, ErrNotSealed):
	default:
		return rep, Bundle{}, nil, err
	}
	auth, err := newAuthMaterial(oskp)
	if err != nil {
		return rep, Bundle{}, nil, err
	}
	auth.applyTo(b)
	if err := pushAccount(ctx, sysConn, b.AuthAccountJWT); err != nil {
		return rep, Bundle{}, nil, fmt.Errorf("push the AUTH account: %w", err)
	}

	c, err = CreateCustody(ctx, ctrlConn, opts.Replicas)
	if err != nil {
		return rep, Bundle{}, nil, err
	}
	ctrlUsers := map[string]string{firstInstance: ctrlUser.PublicKey}
	sysUsers := map[string]string{firstInstance: sysUser.PublicKey}
	rep, err = sealEntries(ctx, c, b, ctrlUsers, sysUsers)
	if err != nil {
		return rep, Bundle{}, nil, err
	}
	rep.Instance = firstInstance
	return rep, bundle, c, nil
}

// verifyByPublicKeys is the idempotent second seal: the bucket names this
// material's operator signing key and accounts, or it is another root's.
func verifyByPublicKeys(ctx context.Context, c *Custody, opPub string, accounts map[string]string) (SealReport, error) {
	var rep SealReport
	op, _, err := c.Operator(ctx)
	if err != nil {
		return rep, err
	}
	if op.PublicKey == opPub {
		rep.Matched = append(rep.Matched, keyOperator)
	} else {
		rep.Conflict = append(rep.Conflict, keyOperator)
	}
	for _, name := range []string{"SYS", "CONTROL", "AUTH"} {
		rec, _, err := c.Account(ctx, name)
		if err != nil {
			return rep, err
		}
		want, known := accounts[name]
		if !known || rec.PublicKey == want {
			rep.Matched = append(rep.Matched, keyAccountPrefix+name)
		} else {
			rep.Conflict = append(rep.Conflict, keyAccountPrefix+name)
		}
	}
	if len(rep.Conflict) > 0 {
		return rep, fmt.Errorf("seal refused: the bucket already holds different material for %v — this is not the material that sealed it", rep.Conflict)
	}
	return rep, nil
}

// ExportJSON is the dated export of every current entry — the
// disaster-recovery root the environment keeps (design 10 § what the
// environment keeps).
func ExportJSON(ctx context.Context, c *Custody) ([]byte, error) {
	entries, err := c.Entries(ctx)
	if err != nil {
		return nil, err
	}
	data, err := json.MarshalIndent(struct {
		ExportedAt string                     `json:"exported_at"`
		Entries    map[string]json.RawMessage `json:"entries"`
	}{time.Now().UTC().Format(time.RFC3339), entries}, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode export: %w", err)
	}
	return data, nil
}

// lookupAccountJWT reads an account's current JWT from the resolver over
// a system-account connection — the raw token. An empty payload means
// the resolver does not know the account.
func lookupAccountJWT(ctx context.Context, sysConn *nats.Conn, accountPub string) (string, error) {
	msg, err := sysConn.RequestWithContext(ctx, fmt.Sprintf(accountLookupSubject, accountPub), nil)
	if err != nil {
		return "", fmt.Errorf("claims lookup: %w", err)
	}
	if len(msg.Data) == 0 {
		return "", fmt.Errorf("claims lookup: the resolver knows no account %s", accountPub)
	}
	return string(msg.Data), nil
}

// pushAccount lands an account JWT in the resolver over a system-account
// connection.
func pushAccount(ctx context.Context, sysConn *nats.Conn, accountJWT string) error {
	msg, err := sysConn.RequestWithContext(ctx, claimsUpdateSubject, []byte(accountJWT))
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
