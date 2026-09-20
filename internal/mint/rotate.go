package mint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/impire-io/chronicle/internal/devdir"
)

// Rotation, clustered (chronicle-hq/02-DESIGN/10-custody.md § rotation,
// decision 0032): two steps with both keys trusted throughout. The
// environment adds the new signing key to the operator JWT and rolls the
// nodes; the service lands the new seed in the bucket and re-signs every
// account by compare-and-set then push; the environment removes the old
// key and rolls again. Instances keep serving: a mint during the roll
// signs under the key the bucket names, and every node trusts it.

// RotationReport says what the service's step re-signed.
type RotationReport struct {
	NewPublicKey string
	Resigned     []string
}

// RotateOverBucket is the service's step, `chronicle operator
// rotate-signing-key --url --new-signing-seed`: proves the nodes trust the
// new key by re-signing SYS under it and pushing — a refusal is put back
// and reported, and the bucket is untouched — then lands the new seed in
// `operator` by compare-and-set and re-signs CONTROL and every
// tenant, each by compare-and-set at the revision it was read at, then
// pushed. Mints that interleave sign under whichever key the bucket names
// at that moment.
func RotateOverBucket(ctx context.Context, d *JWTDriver, newSeed []byte) (RotationReport, error) {
	var rep RotationReport
	if d.SysConn == nil {
		return rep, fmt.Errorf("rotation needs a system connection to push")
	}
	newKP, err := nkeys.FromSeed(newSeed)
	if err != nil {
		return rep, fmt.Errorf("new signing seed: %w", err)
	}
	newPub, err := newKP.PublicKey()
	if err != nil {
		return rep, err
	}
	if newPub[0] != 'O' {
		return rep, fmt.Errorf("the new signing seed is a %c key, not an operator key", newPub[0])
	}
	rep.NewPublicKey = newPub

	// The trust check comes first and asks the nodes: the resolver stores
	// whatever is pushed and refuses an untrusted issuer only when the
	// account is next loaded, so a push under a key the nodes do not
	// trust would poison the account, not fail. Every node that answers
	// must list the key.
	untrusting, answered, err := nodesTrusting(ctx, d.SysConn, newPub)
	if err != nil {
		return rep, err
	}
	if answered == 0 {
		return rep, fmt.Errorf("no node answered the trust check")
	}
	if len(untrusting) > 0 {
		return rep, fmt.Errorf("the nodes do not trust %s yet (%s): add it to the operator JWT and roll the nodes first", newPub, strings.Join(untrusting, ", "))
	}
	if err := d.resignAccount(ctx, "SYS", newKP); err != nil {
		return rep, err
	}
	rep.Resigned = append(rep.Resigned, "SYS")

	// The bucket names the new key from here: every signature after this
	// line, a mint's included, is the new key's.
	for attempt := 0; attempt < casAttempts; attempt++ {
		op, rev, err := d.Custody.Operator(ctx)
		if err != nil {
			return rep, err
		}
		op.PublicKey, op.SigningSeed = newPub, string(newSeed)
		if _, err := d.Custody.PutOperator(ctx, op, rev); err != nil {
			if errors.Is(err, ErrRevisionMismatch) {
				continue
			}
			return rep, fmt.Errorf("operator: %w", err)
		}
		break
	}
	if err := d.resignAccount(ctx, "CONTROL", newKP); err != nil {
		return rep, err
	}
	rep.Resigned = append(rep.Resigned, "CONTROL")
	tenants, err := d.Custody.Tenants(ctx)
	if err != nil {
		return rep, err
	}
	for _, name := range tenants {
		if _, err := d.mutate(ctx, name, func(*jwt.AccountClaims, *TenantRecord) error { return nil }); err != nil {
			return rep, err
		}
		rep.Resigned = append(rep.Resigned, "tenant "+name)
	}
	return rep, nil
}

// nodesTrusting asks every node which operator keys it trusts — the
// server's VARZ, scattered over the system account — and names the nodes
// that do not list pub. The window is short: a node that does not answer
// in it is not counted, and the caller requires at least one.
func nodesTrusting(ctx context.Context, sysConn *nats.Conn, pub string) (untrusting []string, answered int, err error) {
	inbox := nats.NewInbox()
	sub, err := sysConn.SubscribeSync(inbox)
	if err != nil {
		return nil, 0, fmt.Errorf("trust check: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := sysConn.PublishRequest(fmt.Sprintf(serverPingSubject, "VARZ"), inbox, nil); err != nil {
		return nil, 0, fmt.Errorf("trust check: %w", err)
	}
	deadline := time.Now().Add(trustCheckWindow)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 || ctx.Err() != nil {
			return untrusting, answered, nil
		}
		// A scatter has no end marker: the window is the wait, and a
		// silent subscription ends it.
		msg, err := sub.NextMsg(remaining)
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				return untrusting, answered, nil
			}
			return nil, answered, fmt.Errorf("trust check: %w", err)
		}
		var resp struct {
			Server struct {
				Name string `json:"name"`
			} `json:"server"`
			Data struct {
				Operators []*jwt.OperatorClaims `json:"trusted_operators_claim"`
			} `json:"data"`
		}
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			continue
		}
		answered++
		trusts := false
		for _, oc := range resp.Data.Operators {
			if oc.Subject == pub || oc.SigningKeys.Contains(pub) {
				trusts = true
			}
		}
		if !trusts {
			untrusting = append(untrusting, resp.Server.Name)
		}
	}
}

// serverPingSubject scatters a monitoring request to every node.
const serverPingSubject = "$SYS.REQ.SERVER.PING.%s"

// trustCheckWindow is how long the trust check listens for nodes.
const trustCheckWindow = 1500 * time.Millisecond

// resignAccount re-signs a bootstrap account under signer, lands it by
// compare-and-set, and pushes; a refused push restores the record.
func (d *JWTDriver) resignAccount(ctx context.Context, name string, signer nkeys.KeyPair) error {
	for attempt := 0; attempt < casAttempts; attempt++ {
		rec, rev, err := d.Custody.Account(ctx, name)
		if err != nil {
			return err
		}
		ac, err := jwt.DecodeAccountClaims(rec.JWT)
		if err != nil {
			return fmt.Errorf("decode account claims of %s: %w", name, err)
		}
		previous := rec.JWT
		token, err := ac.Encode(signer)
		if err != nil {
			return fmt.Errorf("re-sign %s: %w", name, err)
		}
		rec.JWT = token
		landed, err := d.Custody.PutAccount(ctx, rec, rev)
		if err != nil {
			if errors.Is(err, ErrRevisionMismatch) {
				continue
			}
			return err
		}
		if err := d.push(ctx, token); err != nil {
			rec.JWT = previous
			_, _ = d.Custody.PutAccount(ctx, rec, landed)
			return fmt.Errorf("push account %s: %w", name, err)
		}
		return nil
	}
	return fmt.Errorf("account %s: %d writers raced this mutation and it never landed; retry", name, casAttempts)
}

// RotateOperatorSigningKey is the dev shape of the ceremony,
// `chronicle operator rotate-signing-key --dir`: the dev dir plays the
// environment around the service's step, over its own embedded server —
// a new signing key; the operator JWT re-issued under the identity key
// with both keys; the server up; the service's step; the server down; the
// operator JWT with the new key alone; then the verification boot, every
// credential the install holds connecting. `up` must be stopped: the
// ceremony owns the embedded server for its duration.
func RotateOperatorSigningKey(dir string) (string, error) {
	r, err := LoadRoot(dir)
	if err != nil {
		return "", err
	}
	b := r.B
	if len(b.OperatorSeed) == 0 {
		return "", fmt.Errorf("%s has no %s: the install predates operator-identity custody, and its operator JWT can never be re-issued — rotation needs a fresh dev dir", dir, fOperatorNK)
	}
	if r.Manifest.Sealed == "" {
		return "", fmt.Errorf("%s is not sealed: run `chronicle up` once before rotating", dir)
	}
	sysCreds, ctrlCreds, err := ceremonyCreds(r)
	if err != nil {
		return "", err
	}
	if err := requireStopped(dir, sysCreds); err != nil {
		return "", err
	}
	okp, err := nkeys.FromSeed(b.OperatorSeed)
	if err != nil {
		return "", fmt.Errorf("operator seed: %w", err)
	}
	oc, err := jwt.DecodeOperatorClaims(b.OperatorJWT)
	if err != nil {
		return "", fmt.Errorf("decode operator jwt: %w", err)
	}
	if opub, _ := okp.PublicKey(); oc.Subject != opub {
		return "", fmt.Errorf("%s does not match the operator JWT: seed is for %s, operator is %s", fOperatorNK, opub, oc.Subject)
	}
	newKP, newPub, err := newKey(nkeys.CreateOperator)
	if err != nil {
		return "", fmt.Errorf("new operator signing key: %w", err)
	}
	newSeed, err := newKP.Seed()
	if err != nil {
		return "", err
	}
	writeOperatorJWT := func(keys jwt.StringList) error {
		oc.SigningKeys = keys
		token, err := oc.Encode(okp)
		if err != nil {
			return fmt.Errorf("re-encode operator jwt: %w", err)
		}
		b.OperatorJWT = token
		return os.WriteFile(filepath.Join(dir, fOperatorJWT), []byte(token), plainFileMode)
	}

	// Step one, the environment's: both keys trusted, the server up.
	if err := writeOperatorJWT(append(oc.SigningKeys, newPub)); err != nil {
		return "", err
	}
	key := ""
	if n, ok := r.Manifest.Nodes["embedded"]; ok {
		key = n.JetStreamKey
	}
	if err := withEmbeddedServer(b, key, func(url string) error {
		sysConn, err := ConnectCreds(url, sysCreds, "rotate-sys")
		if err != nil {
			return err
		}
		defer sysConn.Close()
		ctrlConn, err := ConnectCreds(url, ctrlCreds, "rotate")
		if err != nil {
			return err
		}
		defer ctrlConn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		c, err := OpenCustody(ctx, ctrlConn)
		if err != nil {
			return err
		}
		d, err := NewJWTDriver(ctx, c, sysConn, url)
		if err != nil {
			return err
		}
		// Step two, the service's.
		if _, err := RotateOverBucket(ctx, d, newSeed); err != nil {
			return err
		}
		// The dev dir plays the environment: its preloads follow the
		// bucket, as a rendered config would after the ceremony.
		return r.refreshAccountFiles(ctx, c)
	}); err != nil {
		return "", err
	}

	// Step three, the environment's: the old key gone, then the
	// verification boot — every credential the install holds connects.
	if err := writeOperatorJWT(jwt.StringList{newPub}); err != nil {
		return "", err
	}
	if err := withEmbeddedServer(b, key, func(url string) error {
		return verifyCredentials(url, sysCreds, ctrlCreds)
	}); err != nil {
		return "", fmt.Errorf("verify rotated install: %w", err)
	}
	return newPub, nil
}

// withEmbeddedServer runs fn against the dev dir's server, then shuts it
// down fully so the caller may boot the directory again at once.
func withEmbeddedServer(b *Bootstrap, key string, fn func(url string) error) error {
	srv, err := b.StartServerWithKey(-1, key)
	if err != nil {
		return err
	}
	defer func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	}()
	return fn(srv.ClientURL())
}

// refreshAccountFiles rewrites the dev dir's public account JWTs from the
// bucket after a rotation — the preloads a dev boot renders from.
func (r *Root) refreshAccountFiles(ctx context.Context, c *Custody) error {
	files := map[string]string{"SYS": fSysAcctJWT, "CONTROL": fCtrlAcctJWT}
	for name, file := range files {
		rec, _, err := c.Account(ctx, name)
		if errors.Is(err, ErrNoRecord) {
			continue
		}
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(r.Dir, file), []byte(rec.JWT), plainFileMode); err != nil {
			return fmt.Errorf("write %s: %w", file, err)
		}
		switch name {
		case "SYS":
			r.B.SystemAccountJWT = rec.JWT
		case "CONTROL":
			r.B.ControlAccountJWT = rec.JWT
		}
	}
	return nil
}

// verifyCredentials connects with every credential the install holds —
// the ceremony's own and each tenant's service user. The could-not-
// succeed-if-broken read: a bad rotation cannot pass it.
func verifyCredentials(url string, sysCreds, ctrlCreds []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	creds := map[string][]byte{"sys": sysCreds, "control": ctrlCreds}
	ctrlConn, err := ConnectCreds(url, ctrlCreds, "rotate-verify-custody")
	if err != nil {
		return fmt.Errorf("control cannot connect after rotation: %w", err)
	}
	defer ctrlConn.Close()
	c, err := OpenCustody(ctx, ctrlConn)
	if err != nil {
		return err
	}
	names, err := c.Tenants(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		rec, _, err := c.Tenant(ctx, name)
		if err != nil {
			return err
		}
		creds["tenant "+name] = []byte(rec.ServiceCreds)
	}
	for name, cr := range creds {
		nc, err := ConnectCreds(url, cr, "rotate-verify")
		if err != nil {
			return fmt.Errorf("%s cannot connect after rotation: %w", name, err)
		}
		nc.Close()
	}
	return nil
}

// ceremonyCreds is what the dev ceremony dials with: the root's
// control-instance bundle — the only users a root ever holds.
func ceremonyCreds(r *Root) (sys, ctrl []byte, err error) {
	_, bundle, err := r.ControlBundle("")
	if err != nil {
		return nil, nil, err
	}
	return bundle.SysCreds, bundle.ControlCreds, nil
}

// requireStopped refuses the dev ceremony while `up` answers: the
// ceremony boots the dev dir's server itself, and two servers on one
// store is a corruption. A stale client.url after a clean stop is normal
// — the dial is the test. An authorization refusal still proves something
// is listening.
func requireStopped(dir string, sysCreds []byte) error {
	url, err := devdir.ReadClientURL(dir)
	if err != nil {
		return nil
	}
	token, jwtErr := jwt.ParseDecoratedJWT(sysCreds)
	kp, kpErr := jwt.ParseDecoratedUserNKey(sysCreds)
	if jwtErr != nil || kpErr != nil {
		return fmt.Errorf("parse sys creds: %w", errors.Join(jwtErr, kpErr))
	}
	nc, err := nats.Connect(url,
		nats.Name("chronicle-rotate-guard"),
		nats.UserJWT(
			func() (string, error) { return token, nil },
			func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) },
		),
		nats.Timeout(2*time.Second),
		nats.RetryOnFailedConnect(false),
		nats.MaxReconnects(0),
	)
	if err == nil {
		nc.Close()
	} else if !errors.Is(err, nats.ErrAuthorization) {
		return nil
	}
	return fmt.Errorf("the fleet at %s is still running (%s answers); stop it before rotating", dir, url)
}
