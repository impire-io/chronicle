package mint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/devdir"
)

// Bootstrap is the throwaway operator-mode substrate of the onboarding
// design's jwt ceremony: generated operator, full resolver, chronicle's
// signing key baked in from the start — so the self-hosted first run needs
// no NATS expertise at all. It is generated into a directory once and
// loaded ever after; the directory is the install's key custody.
type Bootstrap struct {
	Dir string

	OperatorJWT string
	// OperatorSeed is the operator IDENTITY seed — used only to re-issue
	// the operator JWT when the signing key rotates. Empty on installs
	// bootstrapped before it was persisted; those cannot rotate.
	OperatorSeed        []byte
	OperatorSigningSeed []byte

	SystemAccountPub string
	SystemAccountJWT string
	// SystemAccountSeed and ControlAccountSeed issue the users of the two
	// accounts — every one an instance under a role's template (design
	// 10, 0032); no user is minted at birth. Empty once seal moved them
	// into the bucket, and on installs bootstrapped before they were
	// persisted; those cannot issue instances and need a fresh init.
	SystemAccountSeed []byte

	ControlAccountPub  string
	ControlAccountJWT  string
	ControlAccountSeed []byte

	// The AUTH account of decision 0026 — the callout bridge's trigger
	// account (authbootstrap.go). SentinelCreds are public by design;
	// the seeds are custody like every other.
	AuthAccountPub  string
	AuthAccountJWT  string
	AuthAccountSeed []byte
	AuthXKeySeed    []byte
	BridgeCreds     []byte
	SentinelCreds   []byte
}

// bootstrap file names inside the data dir. The two the CLI reads are
// declared in internal/devdir; the rest are custody, private to this
// package.
const (
	fOperatorJWT  = "operator.jwt"
	fOperatorNK   = "operator.nk"
	fOperatorSK   = "operator-signing.nk"
	fSysAcctJWT   = "sys-account.jwt"
	fSysAcctPub   = "sys-account.pub"
	fSysAcctNK    = "sys-account.nk"
	fCtrlAcctJWT  = "control-account.jwt"
	fCtrlAcctPub  = "control-account.pub"
	fCtrlAcctNK   = "control-account.nk"
	resolverDir   = "resolver"
	jetstreamDir  = "jetstream"
	accountsDir   = "accounts"
	fClientURL    = devdir.ClientURLFile
	keyFileMode   = 0o600
	plainFileMode = 0o644
)

// LoadOrInitBootstrap loads the bootstrap material from dir, generating it
// on first run.
func LoadOrInitBootstrap(dir string) (*Bootstrap, error) {
	if _, err := os.Stat(filepath.Join(dir, fOperatorJWT)); err == nil {
		return loadBootstrap(dir)
	}
	return initBootstrap(dir)
}

func loadBootstrap(dir string) (*Bootstrap, error) {
	b := &Bootstrap{Dir: dir}
	var missing []string
	// The public material every root keeps: the operator JWT and the
	// account JWTs and public keys the servers are rendered from.
	read := func(name string) []byte {
		p, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			missing = append(missing, name)
		}
		return p
	}
	// The working keys: present until seal shreds them (design 10 § first
	// boot), absent ever after — the bucket holds them then, and only the
	// ceremonies that run before seal need them here.
	optional := func(name string) []byte {
		p, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil
		}
		return p
	}
	b.OperatorJWT = string(read(fOperatorJWT))
	b.SystemAccountJWT = string(read(fSysAcctJWT))
	b.SystemAccountPub = string(read(fSysAcctPub))
	b.ControlAccountJWT = string(read(fCtrlAcctJWT))
	b.ControlAccountPub = string(read(fCtrlAcctPub))
	if len(missing) > 0 {
		return nil, fmt.Errorf("bootstrap dir %s is incomplete (no %s); move it aside to regenerate", dir, missing[0])
	}
	// The operator identity seed arrived after the first installs shipped:
	// absent is a valid state — only signing-key rotation needs it, and
	// rotation refuses with guidance when it is missing.
	b.OperatorSeed = optional(fOperatorNK)
	b.OperatorSigningSeed = optional(fOperatorSK)
	b.SystemAccountSeed = optional(fSysAcctNK)
	b.ControlAccountSeed = optional(fCtrlAcctNK)
	if err := b.ensureControlJetStream(); err != nil {
		return nil, err
	}
	if err := b.ensureAuthAccount(); err != nil {
		return nil, err
	}
	return b, nil
}

// HasWorkingKeys says the root still holds the seeds seal moves into the
// bucket: true before seal, false after it shreds them.
func (b *Bootstrap) HasWorkingKeys() bool {
	return len(b.OperatorSigningSeed) > 0 && len(b.SystemAccountSeed) > 0 && len(b.ControlAccountSeed) > 0
}

// OperatorSigningKeys are the signing keys the operator JWT trusts — the
// public half, readable on a sealed root that holds no seed.
func (b *Bootstrap) OperatorSigningKeys() ([]string, error) {
	oc, err := jwt.DecodeOperatorClaims(b.OperatorJWT)
	if err != nil {
		return nil, fmt.Errorf("decode operator jwt: %w", err)
	}
	return oc.SigningKeys, nil
}

// workingSecretFiles is what seal shreds from the root once the bucket
// holds it: the seeds the bucket now keeps, and the AUTH account's users
// until the fold moves them. What survives is the operator identity, the
// node keys, the bundles, and public material.
var workingSecretFiles = []string{
	fOperatorSK, fSysAcctNK, fCtrlAcctNK,
	fAuthAcctNK, fAuthXKeyNK, fBridgeCreds, fSentinelCreds,
}

// shredWorkingKeys overwrites and removes the working secrets from dir.
// Absent files are already shredded. The in-memory Bootstrap keeps what
// it loaded: the process that sealed still has the material it sealed.
func shredWorkingKeys(dir string) error {
	for _, name := range workingSecretFiles {
		if err := shredFile(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return nil
}

func shredFile(path string) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("shred %s: %w", path, err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("shred %s: %w", path, err)
	}
	zeros := make([]byte, info.Size())
	_, werr := f.Write(zeros)
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		return fmt.Errorf("shred %s: %w", path, err)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("shred %s: %w", path, err)
	}
	return nil
}

// controlJetStreamLimits mirrors the tenant default: the fleet log is an
// ordinary chronicle log and gets an ordinary account.
var controlJetStreamLimits = jwt.JetStreamLimits{
	MemoryStorage: -1, DiskStorage: -1, Streams: -1, Consumer: -1,
}

// bridgeExport is the control account's half of the tenant-stamped
// bridge (06-scheduler.md § the dispatch surface): a service export every
// tenant JWT imports with its own stamp. Unguarded on this substrate —
// only chronicle's signing key can mint an importer; a token-gated export
// is the synadia driver's concern when it exists.
func bridgeExport() *jwt.Export {
	return &jwt.Export{
		Name:    "chronicle-fleet-bridge",
		Type:    jwt.Service,
		Subject: jwt.Subject(contract.FleetBridgeExport),
	}
}

// ensureControlJetStream refreshes a control-account JWT minted before the
// fleet log or the bridge existed (decision 0014): account claims are
// signed by the operator signing key, which the bootstrap keeps, so the
// upgrade needs no account seed. The resolver preloads the refreshed JWT
// at every server start.
func (b *Bootstrap) ensureControlJetStream() error {
	stamped, changed, err := stampControlShapeWith(b.ControlAccountJWT, func() (nkeys.KeyPair, error) {
		if len(b.OperatorSigningSeed) == 0 {
			return nil, fmt.Errorf("the control account JWT in %s predates the fleet log and the root holds no signing seed to refresh it; a fresh dev dir is the remedy", b.Dir)
		}
		return nkeys.FromSeed(b.OperatorSigningSeed)
	})
	if err != nil || !changed {
		return err
	}
	b.ControlAccountJWT = stamped
	if err := os.WriteFile(filepath.Join(b.Dir, fCtrlAcctJWT), []byte(stamped), plainFileMode); err != nil {
		return fmt.Errorf("write refreshed control account jwt: %w", err)
	}
	return nil
}

// stampControlShape is the service's shape on the CONTROL account — the
// fleet log's JetStream limits and the bridge export — applied to whatever
// JWT the account carries and re-signed under the operator signing key.
// A JWT that already carries the shape comes back unchanged: the
// environment's bare account is stamped once, at seal.
func stampControlShape(token string, oskp nkeys.KeyPair) (string, bool, error) {
	return stampControlShapeWith(token, func() (nkeys.KeyPair, error) { return oskp, nil })
}

func stampControlShapeWith(token string, signer func() (nkeys.KeyPair, error)) (string, bool, error) {
	claims, err := jwt.DecodeAccountClaims(token)
	if err != nil {
		return "", false, fmt.Errorf("decode control account jwt: %w", err)
	}
	hasExport := false
	for _, e := range claims.Exports {
		if string(e.Subject) == contract.FleetBridgeExport {
			hasExport = true
		}
	}
	if claims.Limits.JetStreamLimits != (jwt.JetStreamLimits{}) && hasExport {
		return token, false, nil
	}
	claims.Limits.JetStreamLimits = controlJetStreamLimits
	if !hasExport {
		claims.Exports.Add(bridgeExport())
	}
	oskp, err := signer()
	if err != nil {
		return "", false, err
	}
	stamped, err := claims.Encode(oskp)
	if err != nil {
		return "", false, fmt.Errorf("re-encode control account jwt: %w", err)
	}
	return stamped, true, nil
}

func initBootstrap(dir string) (*Bootstrap, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create bootstrap dir: %w", err)
	}

	okp, opub, err := newKey(nkeys.CreateOperator)
	if err != nil {
		return nil, fmt.Errorf("operator key: %w", err)
	}
	oskp, ospub, err := newKey(nkeys.CreateOperator)
	if err != nil {
		return nil, fmt.Errorf("operator signing key: %w", err)
	}

	sakp, sapub, err := newKey(nkeys.CreateAccount)
	if err != nil {
		return nil, fmt.Errorf("system account key: %w", err)
	}
	cakp, capub, err := newKey(nkeys.CreateAccount)
	if err != nil {
		return nil, fmt.Errorf("control account key: %w", err)
	}

	oc := jwt.NewOperatorClaims(opub)
	oc.Name = "chronicle"
	oc.SigningKeys.Add(ospub)
	oc.SystemAccount = sapub
	operatorJWT, err := oc.Encode(okp)
	if err != nil {
		return nil, fmt.Errorf("encode operator jwt: %w", err)
	}

	sac := jwt.NewAccountClaims(sapub)
	sac.Name = "SYS"
	sysJWT, err := sac.Encode(oskp)
	if err != nil {
		return nil, fmt.Errorf("encode system account jwt: %w", err)
	}

	cac := jwt.NewAccountClaims(capub)
	cac.Name = "CONTROL"
	// The control plane has its own data plane — the fleet log (decision
	// 0014) — so the control account gets JetStream like any tenant.
	cac.Limits.JetStreamLimits = controlJetStreamLimits
	cac.Exports.Add(bridgeExport())
	controlJWT, err := cac.Encode(oskp)
	if err != nil {
		return nil, fmt.Errorf("encode control account jwt: %w", err)
	}

	oSeed, err := okp.Seed()
	if err != nil {
		return nil, fmt.Errorf("operator seed: %w", err)
	}
	osSeed, err := oskp.Seed()
	if err != nil {
		return nil, fmt.Errorf("operator signing seed: %w", err)
	}
	saSeed, err := sakp.Seed()
	if err != nil {
		return nil, fmt.Errorf("system account seed: %w", err)
	}
	caSeed, err := cakp.Seed()
	if err != nil {
		return nil, fmt.Errorf("control account seed: %w", err)
	}

	files := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{fOperatorJWT, []byte(operatorJWT), plainFileMode},
		{fOperatorNK, oSeed, keyFileMode},
		{fOperatorSK, osSeed, keyFileMode},
		{fSysAcctJWT, []byte(sysJWT), plainFileMode},
		{fSysAcctPub, []byte(sapub), plainFileMode},
		{fSysAcctNK, saSeed, keyFileMode},
		{fCtrlAcctJWT, []byte(controlJWT), plainFileMode},
		{fCtrlAcctPub, []byte(capub), plainFileMode},
		{fCtrlAcctNK, caSeed, keyFileMode},
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f.name), f.data, f.mode); err != nil {
			return nil, fmt.Errorf("write %s: %w", f.name, err)
		}
	}

	b := &Bootstrap{
		Dir:                 dir,
		OperatorJWT:         operatorJWT,
		OperatorSeed:        oSeed,
		OperatorSigningSeed: osSeed,
		SystemAccountPub:    sapub,
		SystemAccountJWT:    sysJWT,
		SystemAccountSeed:   saSeed,
		ControlAccountPub:   capub,
		ControlAccountJWT:   controlJWT,
		ControlAccountSeed:  caSeed,
	}
	if err := b.ensureAuthAccount(); err != nil {
		return nil, err
	}
	return b, nil
}

// issueDirect mints a user signed by the account key itself — the
// bootstrap accounts need no signing-key ceremony. The public key comes
// back beside the creds: the AUTH account lists its bridge user by it.
func issueDirect(akp nkeys.KeyPair, apub, name string) ([]byte, string, error) {
	return issueDirectWith(akp, apub, name, jwt.UserPermissionLimits{})
}

// issueDirectWith is issueDirect with a permission template — the fence of
// design 10: every SYS and CONTROL user carries its role's template.
func issueDirectWith(akp nkeys.KeyPair, apub, name string, limits jwt.UserPermissionLimits) ([]byte, string, error) {
	ukp, upub, err := newKey(nkeys.CreateUser)
	if err != nil {
		return nil, "", err
	}
	uc := jwt.NewUserClaims(upub)
	uc.Name = name
	uc.Permissions = limits.Permissions
	if limits.NatsLimits != (jwt.NatsLimits{}) {
		// A template that states limits replaces the defaults; one that
		// states none keeps NewUserClaims' no-limit defaults rather than
		// zeroing them (a zero jwt.NatsLimits means zero subscriptions).
		uc.Limits = limits.Limits
	}
	token, err := uc.Encode(akp)
	if err != nil {
		return nil, "", err
	}
	seed, err := ukp.Seed()
	if err != nil {
		return nil, "", err
	}
	_ = apub
	creds, err := jwt.FormatUserConfig(token, seed)
	return creds, upub, err
}

func newKey(create func() (nkeys.KeyPair, error)) (nkeys.KeyPair, string, error) {
	kp, err := create()
	if err != nil {
		return nil, "", err
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return nil, "", err
	}
	return kp, pub, nil
}

// ServerOptions builds the embedded operator-mode server for this
// bootstrap: trusted operator, dir (full) resolver preloaded with the two
// bootstrap accounts, JetStream on. Port -1 picks a free port.
func (b *Bootstrap) ServerOptions(port int) (*server.Options, error) {
	opClaims, err := jwt.DecodeOperatorClaims(b.OperatorJWT)
	if err != nil {
		return nil, fmt.Errorf("decode operator jwt: %w", err)
	}
	res, err := server.NewDirAccResolver(b.ensureDir(resolverDir), 0, 2*time.Minute, server.NoDelete)
	if err != nil {
		return nil, fmt.Errorf("dir resolver: %w", err)
	}
	if err := res.Store(b.SystemAccountPub, b.SystemAccountJWT); err != nil {
		return nil, fmt.Errorf("preload system account: %w", err)
	}
	if err := res.Store(b.ControlAccountPub, b.ControlAccountJWT); err != nil {
		return nil, fmt.Errorf("preload control account: %w", err)
	}
	if err := res.Store(b.AuthAccountPub, b.AuthAccountJWT); err != nil {
		return nil, fmt.Errorf("preload auth account: %w", err)
	}
	return &server.Options{
		Host:             "127.0.0.1",
		Port:             port,
		JetStream:        true,
		StoreDir:         b.ensureDir(jetstreamDir),
		SystemAccount:    b.SystemAccountPub,
		TrustedOperators: []*jwt.OperatorClaims{opClaims},
		AccountResolver:  res,
	}, nil
}

// StartServer runs the embedded server until ready, its store in the clear.
func (b *Bootstrap) StartServer(port int) (*server.Server, error) {
	return b.StartServerWithKey(port, "")
}

// StartServerWithKey runs the embedded server with its JetStream store
// encrypted at rest under key (design 10 § at rest); an empty key leaves
// the store in the clear. `up` passes the node key its root holds.
func (b *Bootstrap) StartServerWithKey(port int, key string) (*server.Server, error) {
	opts, err := b.ServerOptions(port)
	if err != nil {
		return nil, err
	}
	if key != "" {
		opts.JetStreamKey = key
		opts.JetStreamCipher = server.ChaCha
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("new nats server: %w", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		return nil, fmt.Errorf("nats server not ready in time")
	}
	return srv, nil
}

// AccountsDir is where the install keeps per-tenant issuance material.
func (b *Bootstrap) AccountsDir() string { return b.ensureDir(accountsDir) }

// WriteClientURL records the running server's client URL for the CLI.
func (b *Bootstrap) WriteClientURL(url string) error {
	return os.WriteFile(filepath.Join(b.Dir, fClientURL), []byte(url), plainFileMode)
}

func (b *Bootstrap) ensureDir(name string) string {
	p := filepath.Join(b.Dir, name)
	_ = os.MkdirAll(p, 0o700)
	return p
}

// ErrNoAccountSeeds says the install predates design 10's persisted account
// seeds: it cannot issue instances or fleet users, and its remedy is a
// fresh init — the same rule operator.nk set for rotation.
var ErrNoAccountSeeds = fmt.Errorf("this install keeps no SYS/CONTROL account seeds (bootstrapped before design 10); a fresh `chronicle operator init` is the remedy")

// IssueControlUser issues a user of the CONTROL account under the given
// permission template — one of the four roles' — from the root's own
// seed, before seal.
func (b *Bootstrap) IssueControlUser(name string, limits jwt.UserPermissionLimits) (Creds, error) {
	return issueAccountUser(b.ControlAccountSeed, b.ControlAccountPub, name, limits)
}

// IssueSystemUser issues a user of the SYS account: every control instance
// holds one for claims pushes and account lookups.
func (b *Bootstrap) IssueSystemUser(name string) (Creds, error) {
	return issueAccountUser(b.SystemAccountSeed, b.SystemAccountPub, name, jwt.UserPermissionLimits{})
}
