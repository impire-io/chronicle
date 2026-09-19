package mint

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// The offline root (chronicle-hq/02-DESIGN/10-custody.md § first boot, § what
// stays offline): the material an install is born from. `init` generates
// it, `emit-cluster-config` renders the servers from it, `seal` moves the
// working keys into the AUTH bucket once the cluster serves, and what the
// root keeps afterwards is the operator identity, one JetStream key per
// node, the instance bundles it issued, and dated exports of the bucket.
// Until seal, the root is also the bootstrap dir the rest of the code
// reads — one directory, two roles.

const (
	fRootManifest = "root.json"
	dBundles      = "bundles"
	dExports      = "exports"
	fBundleCtrl   = "control.creds"
	fBundleSys    = "sys.creds"
	// firstInstance is the bundle init issues: the control instance that
	// seals and serves first.
	firstInstance = "instance-1"
)

// RootManifest is root.json: the per-node JetStream keys, the bundles and
// exports the root has produced, and when it was sealed.
type RootManifest struct {
	Version int                    `json:"version"`
	Nodes   map[string]NodeSecrets `json:"nodes"`
	Bundles []string               `json:"bundles"`
	Exports []string               `json:"exports"`
	Sealed  string                 `json:"sealed,omitempty"`
}

// NodeSecrets is one NATS node's material that never enters its config:
// the key its JetStream store is encrypted with, delivered to the node's
// unit environment as NATS_JETSTREAM_KEY.
type NodeSecrets struct {
	JetStreamKey string `json:"jetstream_key"`
}

// Root is an opened offline root.
type Root struct {
	Dir      string
	B        *Bootstrap
	Manifest RootManifest
}

// InitRoot generates a root at dir — or opens the one already there — and
// makes sure it holds a manifest and the first instance's bundle. It is
// `chronicle operator init`; idempotent, like every ceremony here.
func InitRoot(dir string) (*Root, error) {
	b, err := LoadOrInitBootstrap(dir)
	if err != nil {
		return nil, err
	}
	r := &Root{Dir: dir, B: b}
	if err := r.loadManifest(); err != nil {
		return nil, err
	}
	if _, err := os.Stat(r.BundleDir(firstInstance)); errors.Is(err, os.ErrNotExist) {
		if _, err := r.IssueBundle(firstInstance); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// LoadRoot opens an existing root; it does not generate one.
func LoadRoot(dir string) (*Root, error) {
	if _, err := os.Stat(filepath.Join(dir, fOperatorJWT)); err != nil {
		return nil, fmt.Errorf("no root at %s: %w", dir, err)
	}
	b, err := loadBootstrap(dir)
	if err != nil {
		return nil, err
	}
	r := &Root{Dir: dir, B: b}
	if err := r.loadManifest(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Root) loadManifest() error {
	path := filepath.Join(r.Dir, fRootManifest)
	data, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		r.Manifest = RootManifest{Version: 1, Nodes: map[string]NodeSecrets{}}
		return r.saveManifest()
	case err != nil:
		return fmt.Errorf("read %s: %w", fRootManifest, err)
	}
	if err := json.Unmarshal(data, &r.Manifest); err != nil {
		return fmt.Errorf("decode %s: %w", fRootManifest, err)
	}
	if r.Manifest.Nodes == nil {
		r.Manifest.Nodes = map[string]NodeSecrets{}
	}
	return nil
}

func (r *Root) saveManifest() error {
	data, err := json.MarshalIndent(r.Manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", fRootManifest, err)
	}
	return os.WriteFile(filepath.Join(r.Dir, fRootManifest), data, keyFileMode)
}

// NodeKey is the JetStream encryption key for one named node — generated
// the first time the node is emitted, stable ever after, kept only here
// and in the node's unit environment.
func (r *Root) NodeKey(name string) (string, error) {
	if n, ok := r.Manifest.Nodes[name]; ok && n.JetStreamKey != "" {
		return n.JetStreamKey, nil
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("node key: %w", err)
	}
	key := hex.EncodeToString(raw)
	r.Manifest.Nodes[name] = NodeSecrets{JetStreamKey: key}
	if err := r.saveManifest(); err != nil {
		return "", err
	}
	return key, nil
}

// BundleDir is where an instance's bundle lives under the root.
func (r *Root) BundleDir(name string) string { return filepath.Join(r.Dir, dBundles, name) }

// IssueBundle issues one control instance's credentials — a CONTROL user
// under the control-instance template and a SYS user — and writes them as
// a bundle directory (mode 0700). Before seal this is the only way to
// issue an instance; after it, `operator instance add` does the same over
// the live bucket.
func (r *Root) IssueBundle(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("an instance needs a name")
	}
	ctrl, err := r.B.IssueControlUser(name+"-control", ControlInstanceTemplate())
	if err != nil {
		return "", fmt.Errorf("issue control user for %s: %w", name, err)
	}
	sys, err := r.B.IssueSystemUser(name + "-sys")
	if err != nil {
		return "", fmt.Errorf("issue system user for %s: %w", name, err)
	}
	dir := r.BundleDir(name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create bundle dir: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, fBundleCtrl), ctrl.File, keyFileMode); err != nil {
		return "", fmt.Errorf("write bundle: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, fBundleSys), sys.File, keyFileMode); err != nil {
		return "", fmt.Errorf("write bundle: %w", err)
	}
	for _, b := range r.Manifest.Bundles {
		if b == name {
			return dir, nil
		}
	}
	r.Manifest.Bundles = append(r.Manifest.Bundles, name)
	return dir, r.saveManifest()
}

// Bundle reads an instance's bundle: the two credentials files.
type Bundle struct {
	ControlCreds []byte
	SysCreds     []byte
}

// ReadBundle loads a bundle directory.
func ReadBundle(dir string) (Bundle, error) {
	ctrl, err := os.ReadFile(filepath.Join(dir, fBundleCtrl))
	if err != nil {
		return Bundle{}, fmt.Errorf("bundle %s: %w", dir, err)
	}
	sys, err := os.ReadFile(filepath.Join(dir, fBundleSys))
	if err != nil {
		return Bundle{}, fmt.Errorf("bundle %s: %w", dir, err)
	}
	return Bundle{ControlCreds: ctrl, SysCreds: sys}, nil
}

// SealOptions shapes the ceremony.
type SealOptions struct {
	// Replicas of the bucket — three on the hosted trio, one on a single
	// embedded server. Zero means one.
	Replicas int
}

// SealReport says what the ceremony did.
type SealReport struct {
	Written  []string // entries created
	Matched  []string // entries already present and identical
	Export   string   // the dated export written under the root
	Conflict []string // entries present and DIFFERENT — the ceremony refuses
}

// Seal moves the root's working keys into the AUTH bucket over an
// authenticated control connection: create the bucket, write every entry
// the root holds, read each back, write the first dated export. Idempotent
// — a second run finds every entry and reports it matched. An entry that
// exists and differs is a conflict: the ceremony refuses rather than
// overwrite what another root sealed.
func (r *Root) Seal(ctx context.Context, nc *nats.Conn, opts SealOptions) (SealReport, error) {
	var rep SealReport
	b := r.B
	if len(b.SystemAccountSeed) == 0 || len(b.ControlAccountSeed) == 0 {
		return rep, ErrNoAccountSeeds
	}
	c, err := CreateCustody(ctx, nc, opts.Replicas)
	if err != nil {
		return rep, err
	}

	opPub, err := PublicKeyOfSeed(b.OperatorSigningSeed)
	if err != nil {
		return rep, fmt.Errorf("operator signing seed: %w", err)
	}
	entries := []sealEntry{
		{
			key:  keyOperator,
			want: OperatorRecord{PublicKey: opPub, SigningSeed: string(b.OperatorSigningSeed)},
			write: func() (uint64, error) {
				return c.PutOperator(ctx, OperatorRecord{PublicKey: opPub, SigningSeed: string(b.OperatorSigningSeed)}, 0)
			},
			read: func() (any, uint64, error) { return c.Operator(ctx) },
		},
		accountEntry(ctx, c, AccountRecord{Name: "SYS", PublicKey: b.SystemAccountPub, Seed: string(b.SystemAccountSeed), JWT: b.SystemAccountJWT}),
		accountEntry(ctx, c, AccountRecord{Name: "CONTROL", PublicKey: b.ControlAccountPub, Seed: string(b.ControlAccountSeed), JWT: b.ControlAccountJWT}),
		accountEntry(ctx, c, AccountRecord{Name: "AUTH", PublicKey: b.AuthAccountPub, Seed: string(b.AuthAccountSeed), JWT: b.AuthAccountJWT}),
		{
			key:  keyAuth,
			want: AuthRecord{XKeySeed: string(b.AuthXKeySeed), BridgeCreds: string(b.BridgeCreds), SentinelCreds: string(b.SentinelCreds)},
			write: func() (uint64, error) {
				return c.PutAuth(ctx, AuthRecord{XKeySeed: string(b.AuthXKeySeed), BridgeCreds: string(b.BridgeCreds), SentinelCreds: string(b.SentinelCreds)}, 0)
			},
			read: func() (any, uint64, error) { return c.Auth(ctx) },
		},
	}
	for _, e := range entries {
		got, _, err := e.read()
		switch {
		case errors.Is(err, ErrNoRecord):
			if _, err := e.write(); err != nil {
				return rep, err
			}
			back, _, err := e.read()
			if err != nil {
				return rep, fmt.Errorf("read back %s: %w", e.key, err)
			}
			if !sameJSON(back, e.want) {
				return rep, fmt.Errorf("read back %s: the bucket does not hold what was written", e.key)
			}
			rep.Written = append(rep.Written, e.key)
		case err != nil:
			return rep, err
		case sameJSON(got, e.want):
			rep.Matched = append(rep.Matched, e.key)
		default:
			rep.Conflict = append(rep.Conflict, e.key)
		}
	}
	if len(rep.Conflict) > 0 {
		return rep, fmt.Errorf("seal refused: the bucket already holds different material for %v — this root is not the one that sealed it", rep.Conflict)
	}

	path, err := r.Export(ctx, c)
	if err != nil {
		return rep, err
	}
	rep.Export = path
	if r.Manifest.Sealed == "" {
		r.Manifest.Sealed = time.Now().UTC().Format(time.RFC3339)
		if err := r.saveManifest(); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// sealEntry is one record the ceremony writes, reads back, or finds.
type sealEntry struct {
	key   string
	write func() (uint64, error)
	read  func() (any, uint64, error)
	want  any
}

func accountEntry(ctx context.Context, c *Custody, rec AccountRecord) sealEntry {
	return sealEntry{
		key:   keyAccountPrefix + rec.Name,
		want:  rec,
		write: func() (uint64, error) { return c.PutAccount(ctx, rec, 0) },
		read:  func() (any, uint64, error) { return c.Account(ctx, rec.Name) },
	}
}

// PublicKeyOfSeed is the public half of an nkey seed.
func PublicKeyOfSeed(seed []byte) (string, error) {
	kp, err := nkeys.FromSeed(seed)
	if err != nil {
		return "", err
	}
	return kp.PublicKey()
}

// Export writes every current entry of the bucket to a dated file under
// the root's exports dir (mode 0600) and records it in the manifest. The
// disaster-recovery root for the day the cluster and every snapshot are
// gone.
func (r *Root) Export(ctx context.Context, c *Custody) (string, error) {
	entries, err := c.Entries(ctx)
	if err != nil {
		return "", err
	}
	dir := filepath.Join(r.Dir, dExports)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create exports dir: %w", err)
	}
	// One file per export, even within a second: a suffix when the
	// stamp is taken.
	stamp := time.Now().UTC().Format("20060102T150405Z")
	name := stamp + ".json"
	for i := 2; ; i++ {
		if _, err := os.Stat(filepath.Join(dir, name)); errors.Is(err, os.ErrNotExist) {
			break
		}
		name = fmt.Sprintf("%s-%d.json", stamp, i)
	}
	data, err := json.MarshalIndent(struct {
		ExportedAt string                     `json:"exported_at"`
		Entries    map[string]json.RawMessage `json:"entries"`
	}{time.Now().UTC().Format(time.RFC3339), entries}, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode export: %w", err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, keyFileMode); err != nil {
		return "", fmt.Errorf("write export: %w", err)
	}
	r.Manifest.Exports = append(r.Manifest.Exports, name)
	return path, r.saveManifest()
}

func sameJSON(a, b any) bool {
	x, errA := json.Marshal(a)
	y, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(x) == string(y)
}
