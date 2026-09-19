package mint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
)

// The AUTH bucket (chronicle-hq/02-DESIGN/10-custody.md § the bucket): the
// working keys every control instance shares, and beside each account's
// keys its canonical JWT. The bucket is canonical and the resolver is a
// projection of it: every mutation is a compare-and-set here at the
// revision it was read at, and only then a push. Two instances mutating
// one tenant cannot lose an update — the guard is the server's, the loser
// re-reads and recomputes on top of the winner.

// OperatorRecord is the `operator` entry: the operator signing key. The
// operator identity is never here — it stays in the offline root.
type OperatorRecord struct {
	PublicKey   string `json:"public_key"`
	SigningSeed string `json:"signing_seed"`
}

// AccountRecord is an `account.<NAME>` entry — SYS, CONTROL, and until the
// fold AUTH: the account seed, from which its users are issued, the
// account's current JWT, and the users issued from it by instance name.
// The users travel with the account so `operator instance remove` finds
// what to revoke without the host's bundle — losing the host is how the
// bundle is lost.
type AccountRecord struct {
	Name      string `json:"name"`
	PublicKey string `json:"public_key"`
	Seed      string `json:"seed"`
	JWT       string `json:"jwt"`
	// Users maps an instance name to the public key of the user this
	// account issued for it (design 10 § instances).
	Users map[string]string `json:"users,omitempty"`
}

// AuthRecord is the `auth` entry: what the callout bridge answers with. The
// sentinel is public by design and lives here only so an instance can hand
// it out.
type AuthRecord struct {
	XKeySeed      string `json:"xkey_seed"`
	BridgeCreds   string `json:"bridge_creds"`
	SentinelCreds string `json:"sentinel_creds"`
}

// TenantRecord is a `tenant.<name>` entry: everything the issuance path
// holds for one tenant, and the account JWT the resolver is expected to
// carry.
type TenantRecord struct {
	Name         string `json:"name"`
	PublicKey    string `json:"public_key"`
	SigningSeed  string `json:"signing_seed"`
	ScopedSeed   string `json:"scoped_seed"`
	ServiceCreds string `json:"service_creds"`
	JWT          string `json:"jwt"`
}

const (
	keyOperator      = "operator"
	keyAuth          = "auth"
	keyAccountPrefix = "account."
	keyTenantPrefix  = "tenant."
	// custodyHistory keeps an audit trail of every re-sign, and the undo
	// for a bad one.
	custodyHistory = 16
)

var (
	// ErrNotSealed says the bucket does not exist on this substrate yet.
	ErrNotSealed = errors.New("custody: the AUTH bucket does not exist — run `chronicle operator seal`")
	// ErrNoRecord says the entry does not exist.
	ErrNoRecord = errors.New("custody: no such record")
	// ErrRevisionMismatch says another writer landed first: re-read,
	// recompute on top of it, write again.
	ErrRevisionMismatch = errors.New("custody: revision mismatch — re-read and recompute")
)

// Custody is the bucket as the code sees it.
type Custody struct {
	kv jetstream.KeyValue
}

// OpenCustody opens the bucket over an authenticated control connection;
// ErrNotSealed when it does not exist.
func OpenCustody(ctx context.Context, nc *nats.Conn) (*Custody, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("custody: jetstream: %w", err)
	}
	kv, err := js.KeyValue(ctx, contract.AuthBucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, ErrNotSealed
		}
		return nil, fmt.Errorf("custody: open %s: %w", contract.AuthBucket, err)
	}
	return &Custody{kv: kv}, nil
}

// CreateCustody creates the bucket — replicas as the substrate allows,
// history kept — or opens it when it already exists. The seal ceremony's
// first act.
func CreateCustody(ctx context.Context, nc *nats.Conn, replicas int) (*Custody, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("custody: jetstream: %w", err)
	}
	if replicas <= 0 {
		replicas = 1
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      contract.AuthBucket,
		Description: "chronicle custody: the working keys every control instance shares (design 10)",
		History:     custodyHistory,
		Replicas:    replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("custody: create %s: %w", contract.AuthBucket, err)
	}
	return &Custody{kv: kv}, nil
}

// Operator reads the operator signing key.
func (c *Custody) Operator(ctx context.Context) (OperatorRecord, uint64, error) {
	return get[OperatorRecord](ctx, c.kv, keyOperator)
}

// PutOperator writes it: expectedRev 0 creates, otherwise compare-and-set.
func (c *Custody) PutOperator(ctx context.Context, rec OperatorRecord, expectedRev uint64) (uint64, error) {
	return put(ctx, c.kv, keyOperator, rec, expectedRev)
}

// Account reads a bootstrap account by name (SYS, CONTROL, AUTH).
func (c *Custody) Account(ctx context.Context, name string) (AccountRecord, uint64, error) {
	return get[AccountRecord](ctx, c.kv, keyAccountPrefix+name)
}

// PutAccount writes one: expectedRev 0 creates, otherwise compare-and-set.
func (c *Custody) PutAccount(ctx context.Context, rec AccountRecord, expectedRev uint64) (uint64, error) {
	if rec.Name == "" {
		return 0, fmt.Errorf("custody: account record needs a name")
	}
	return put(ctx, c.kv, keyAccountPrefix+rec.Name, rec, expectedRev)
}

// Auth reads the callout material.
func (c *Custody) Auth(ctx context.Context) (AuthRecord, uint64, error) {
	return get[AuthRecord](ctx, c.kv, keyAuth)
}

// PutAuth writes it: expectedRev 0 creates, otherwise compare-and-set.
func (c *Custody) PutAuth(ctx context.Context, rec AuthRecord, expectedRev uint64) (uint64, error) {
	return put(ctx, c.kv, keyAuth, rec, expectedRev)
}

// Tenant reads one tenant's issuance material and canonical JWT.
func (c *Custody) Tenant(ctx context.Context, name string) (TenantRecord, uint64, error) {
	return get[TenantRecord](ctx, c.kv, keyTenantPrefix+name)
}

// PutTenant writes one: expectedRev 0 creates (the mint), otherwise
// compare-and-set (revoke, rekey, rotation).
func (c *Custody) PutTenant(ctx context.Context, rec TenantRecord, expectedRev uint64) (uint64, error) {
	if rec.Name == "" {
		return 0, fmt.Errorf("custody: tenant record needs a name")
	}
	return put(ctx, c.kv, keyTenantPrefix+rec.Name, rec, expectedRev)
}

// DeleteTenant removes a tenant's entry — a mint that could not push, or
// a future tenant destroy. Absent is not an error.
func (c *Custody) DeleteTenant(ctx context.Context, name string) error {
	if err := c.kv.Delete(ctx, keyTenantPrefix+name); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("custody: delete tenant %s: %w", name, err)
	}
	return nil
}

// Tenants lists every tenant the bucket holds — control's boot replay and
// the reconcile walk it.
func (c *Custody) Tenants(ctx context.Context) ([]string, error) {
	keys, err := c.keys(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, k := range keys {
		if strings.HasPrefix(k, keyTenantPrefix) {
			names = append(names, strings.TrimPrefix(k, keyTenantPrefix))
		}
	}
	return names, nil
}

// Entries is every current value by key — the export's input.
func (c *Custody) Entries(ctx context.Context) (map[string]json.RawMessage, error) {
	keys, err := c.keys(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]json.RawMessage, len(keys))
	for _, k := range keys {
		e, err := c.kv.Get(ctx, k)
		if err != nil {
			return nil, fmt.Errorf("custody: read %s: %w", k, err)
		}
		out[k] = json.RawMessage(e.Value())
	}
	return out, nil
}

func (c *Custody) keys(ctx context.Context) ([]string, error) {
	keys, err := c.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("custody: list keys: %w", err)
	}
	return keys, nil
}

func get[T any](ctx context.Context, kv jetstream.KeyValue, key string) (T, uint64, error) {
	var zero T
	e, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return zero, 0, fmt.Errorf("%w: %s", ErrNoRecord, key)
		}
		return zero, 0, fmt.Errorf("custody: read %s: %w", key, err)
	}
	var rec T
	if err := json.Unmarshal(e.Value(), &rec); err != nil {
		return zero, 0, fmt.Errorf("custody: decode %s: %w", key, err)
	}
	return rec, e.Revision(), nil
}

// put is the one write path: create at revision zero, compare-and-set
// otherwise. A lost race is ErrRevisionMismatch, never a silent overwrite.
func put[T any](ctx context.Context, kv jetstream.KeyValue, key string, rec T, expectedRev uint64) (uint64, error) {
	value, err := json.Marshal(rec)
	if err != nil {
		return 0, fmt.Errorf("custody: encode %s: %w", key, err)
	}
	var rev uint64
	if expectedRev == 0 {
		rev, err = kv.Create(ctx, key, value)
	} else {
		rev, err = kv.Update(ctx, key, value, expectedRev)
	}
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return 0, fmt.Errorf("%w: %s", ErrRevisionMismatch, key)
		}
		return 0, fmt.Errorf("custody: write %s: %w", key, err)
	}
	return rev, nil
}
