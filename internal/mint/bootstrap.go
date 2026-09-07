package mint

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

// Bootstrap is the throwaway operator-mode substrate of the onboarding
// design's jwt ceremony: generated operator, full resolver, chronicle's
// signing key baked in from the start — so the self-hosted first run needs
// no NATS expertise at all. It is generated into a directory once and
// loaded ever after; the directory is the install's key custody.
type Bootstrap struct {
	Dir string

	OperatorJWT         string
	OperatorSigningSeed []byte

	SystemAccountPub string
	SystemAccountJWT string
	SysCreds         []byte

	ControlAccountPub string
	ControlAccountJWT string
	ControlCreds      []byte
}

// bootstrap file names inside the data dir.
const (
	fOperatorJWT  = "operator.jwt"
	fOperatorSK   = "operator-signing.nk"
	fSysAcctJWT   = "sys-account.jwt"
	fSysAcctPub   = "sys-account.pub"
	fSysCreds     = "sys.creds"
	fCtrlAcctJWT  = "control-account.jwt"
	fCtrlAcctPub  = "control-account.pub"
	fCtrlCreds    = "control.creds"
	resolverDir   = "resolver"
	jetstreamDir  = "jetstream"
	accountsDir   = "accounts"
	fClientURL    = "client.url"
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
	read := func(name string) []byte {
		p, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			b = nil
		}
		return p
	}
	opJWT := read(fOperatorJWT)
	opSK := read(fOperatorSK)
	sysJWT := read(fSysAcctJWT)
	sysPub := read(fSysAcctPub)
	sysCreds := read(fSysCreds)
	ctrlJWT := read(fCtrlAcctJWT)
	ctrlPub := read(fCtrlAcctPub)
	ctrlCreds := read(fCtrlCreds)
	if b == nil {
		return nil, fmt.Errorf("bootstrap dir %s is incomplete; move it aside to regenerate", dir)
	}
	b.OperatorJWT = string(opJWT)
	b.OperatorSigningSeed = opSK
	b.SystemAccountJWT = string(sysJWT)
	b.SystemAccountPub = string(sysPub)
	b.SysCreds = sysCreds
	b.ControlAccountJWT = string(ctrlJWT)
	b.ControlAccountPub = string(ctrlPub)
	b.ControlCreds = ctrlCreds
	return b, nil
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
	controlJWT, err := cac.Encode(oskp)
	if err != nil {
		return nil, fmt.Errorf("encode control account jwt: %w", err)
	}

	sysCreds, err := issueDirect(sakp, sapub, "sys")
	if err != nil {
		return nil, fmt.Errorf("system user: %w", err)
	}
	ctrlCreds, err := issueDirect(cakp, capub, "control")
	if err != nil {
		return nil, fmt.Errorf("control user: %w", err)
	}

	osSeed, err := oskp.Seed()
	if err != nil {
		return nil, fmt.Errorf("operator signing seed: %w", err)
	}

	files := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{fOperatorJWT, []byte(operatorJWT), plainFileMode},
		{fOperatorSK, osSeed, keyFileMode},
		{fSysAcctJWT, []byte(sysJWT), plainFileMode},
		{fSysAcctPub, []byte(sapub), plainFileMode},
		{fSysCreds, sysCreds, keyFileMode},
		{fCtrlAcctJWT, []byte(controlJWT), plainFileMode},
		{fCtrlAcctPub, []byte(capub), plainFileMode},
		{fCtrlCreds, ctrlCreds, keyFileMode},
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f.name), f.data, f.mode); err != nil {
			return nil, fmt.Errorf("write %s: %w", f.name, err)
		}
	}

	return &Bootstrap{
		Dir:                 dir,
		OperatorJWT:         operatorJWT,
		OperatorSigningSeed: osSeed,
		SystemAccountPub:    sapub,
		SystemAccountJWT:    sysJWT,
		SysCreds:            sysCreds,
		ControlAccountPub:   capub,
		ControlAccountJWT:   controlJWT,
		ControlCreds:        ctrlCreds,
	}, nil
}

// issueDirect mints a user signed by the account key itself — the two
// bootstrap accounts need no signing-key ceremony.
func issueDirect(akp nkeys.KeyPair, apub, name string) ([]byte, error) {
	ukp, upub, err := newKey(nkeys.CreateUser)
	if err != nil {
		return nil, err
	}
	uc := jwt.NewUserClaims(upub)
	uc.Name = name
	token, err := uc.Encode(akp)
	if err != nil {
		return nil, err
	}
	seed, err := ukp.Seed()
	if err != nil {
		return nil, err
	}
	_ = apub
	return jwt.FormatUserConfig(token, seed)
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

// StartServer runs the embedded server until ready.
func (b *Bootstrap) StartServer(port int) (*server.Server, error) {
	opts, err := b.ServerOptions(port)
	if err != nil {
		return nil, err
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

// ReadClientURL reads the recorded client URL; the CLI's way back to a
// running `chronicle up`.
func ReadClientURL(dir string) (string, error) {
	p, err := os.ReadFile(filepath.Join(dir, fClientURL))
	if err != nil {
		return "", fmt.Errorf("read client url (is `chronicle up` running?): %w", err)
	}
	return string(p), nil
}

// ControlCredsPath is where the CLI finds the control-plane credentials.
func ControlCredsPath(dir string) string { return filepath.Join(dir, fCtrlCreds) }

func (b *Bootstrap) ensureDir(name string) string {
	p := filepath.Join(b.Dir, name)
	_ = os.MkdirAll(p, 0o700)
	return p
}
