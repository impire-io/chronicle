package foldcore

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
)

// Verifier answers whether an operation's author is verified (decision
// 0057): the attestation verifies against an attester key the account's
// META lists — identity.attester.<pub>, written by the node, which no
// member can write. Keys are remembered once seen; a key not yet seen is
// read from META, so a node that just recorded its key is trusted on the
// first operation it signs, and a key META does not hold never is.
type Verifier struct {
	meta jetstream.KeyValue

	mu   sync.Mutex
	keys map[string]bool
}

// NewVerifier reads attester keys from the account's META bucket.
func NewVerifier(meta jetstream.KeyValue) *Verifier {
	return &Verifier{meta: meta, keys: map[string]bool{}}
}

// Verify reports whether the op's author is verified: an attestation
// present, signed by a key META lists, and valid over the record.
func (v *Verifier) Verify(ctx context.Context, op contract.Op) bool {
	if op.Attester == "" || op.Signature == "" {
		return false
	}
	if !v.known(ctx, op.Attester) {
		return false
	}
	return op.VerifyAttestation()
}

func (v *Verifier) known(ctx context.Context, pub string) bool {
	v.mu.Lock()
	ok, seen := v.keys[pub]
	v.mu.Unlock()
	if seen && ok {
		return true
	}
	_, err := v.meta.Get(ctx, contract.MetaAttester(pub))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false
	}
	if err != nil {
		// A read that failed decides nothing: the op is judged unverified
		// now, and the next op asks again.
		return false
	}
	v.mu.Lock()
	v.keys[pub] = true
	v.mu.Unlock()
	return true
}

// StoreRequiresVerified reads whether a store requires verified authors —
// its configuration's declaration, set at creation. A store with no
// configuration requires nothing.
func StoreRequiresVerified(ctx context.Context, meta jetstream.KeyValue, store string) (bool, error) {
	entry, err := meta.Get(ctx, contract.MetaStoreConfig(store))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var cfg contract.StoreConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		return false, err
	}
	return cfg.VerifiedAuthors, nil
}
