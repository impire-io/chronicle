package contract

import (
	"encoding/json"
	"strings"
)

// META keys live under reserved prefixes; chronicle refuses keys outside
// them (03-meta-and-state.md). The key grammar is declared here; the
// *content* of the identity slice is the onboarding design's.

// MetaKeyAllowed reports whether a key lives under a reserved prefix.
func MetaKeyAllowed(key string) bool {
	for _, p := range []string{"log.", "index.", "identity."} {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// MetaLogConfig is log.<log>.config — stream overrides, status, description.
func MetaLogConfig(log string) string { return "log." + log + ".config" }

// MetaLogConfigPrefix lists every log: the authoritative inventory the node
// boots from.
const MetaLogConfigPrefix = "log."

// MetaLogType is log.<log>.type.<type> — the type record, the unit of
// definition (decision 0021). Type names follow the log-name grammar, so
// they can never collide with the dotted op-type keys this shape replaced.
func MetaLogType(log, typeName string) string { return "log." + log + ".type." + typeName }

// MetaIndex is index.<log>.<index> — an index declaration. The index
// exists because this key does: declared through INDEX.DECLARE, realized
// by the supervisor placing a workload, retired by deleting the key
// (05-indexes.md).
func MetaIndex(log, index string) string { return "index." + log + "." + index }

// MetaIndexPrefix lists every declaration — the supervisor's watch prefix.
const MetaIndexPrefix = "index."

// IndexDeclaration is the value at index.<log>.<index>: the kind, and
// the kind-owned config the 0012 deferral grew into with its first real
// consumers (0015, 0016). Config is absent for search — no-knobs stands.
type IndexDeclaration struct {
	Kind   string          `json:"kind"`
	Config json.RawMessage `json:"config,omitempty"`
}

// IndexKindSearch is the first index kind (decision 0012): full-text over
// thing state through an embedded engine.
const IndexKindSearch = "search"

// KnownIndexKind reports whether the kind is in this build's vocabulary.
// INDEX.DECLARE refuses kinds outside it (write-side strictness), while a
// supervisor reading a newer build's declaration ignores it with a warning
// (read-side tolerance) — the same split effects got in 0011.
func KnownIndexKind(kind string) bool {
	switch kind {
	case IndexKindSearch, IndexKindGraph, IndexKindSemantic:
		return true
	}
	return false
}

// MetaPrincipal is identity.principal.<id>.
func MetaPrincipal(id string) string { return "identity.principal." + id }

// MetaMember is identity.member.<id> — the membership of one principal.
func MetaMember(id string) string { return "identity.member." + id }

// MetaInvite is identity.invite.<digest>.
func MetaInvite(digest string) string { return "identity.invite." + digest }

// LogConfig is the value at log.<log>.config.
type LogConfig struct {
	Status      string `json:"status"`
	Description string `json:"description,omitempty"`
	// MaxBytes is the per-log byte-budget override; zero means the decided
	// default.
	MaxBytes int64 `json:"max_bytes,omitempty"`
	// History is the log's 0019 declaration: HistoryCompactable (the
	// default when unset) or HistoryPreserved. Set at creation, immutable
	// until a config-edit verb exists.
	History string `json:"history,omitempty"`
}

// LogStatusActive is the one status the skeleton knows.
const LogStatusActive = "active"

// The history vocabulary (decision 0019). Write-side strict at LOG.CREATE,
// read-side tolerant: an unknown stored value reads as compactable — the
// stream's own AllowRollup setting stays the guarantee either way.
const (
	// HistoryCompactable: node-driven rollup acts wherever the effect gate
	// (0011) allows. The default.
	HistoryCompactable = "compactable"
	// HistoryPreserved: the log's trail is the product — the node never
	// compacts it, and its stream is created with AllowRollup false so no
	// writer can replace a subject's history.
	HistoryPreserved = "preserved"
)

// NormalizeHistory maps the unset declaration to its meaning.
func NormalizeHistory(history string) string {
	if history == "" {
		return HistoryCompactable
	}
	return history
}

// TypeRecord is the value at log.<log>.type.<type> — one record, all
// facets, set in one act and revisioned whole (decision 0021). Revisions
// are recorded, never rewritten in place: each revision is a new KV put,
// and the bucket's history keeps the old ones readable.
type TypeRecord struct {
	Revision uint64 `json:"revision"`
	// Schema is the thing's shape: pre-flight validates snapshot state
	// against it, projections mark state that fails it. Read-side only.
	Schema json.RawMessage `json:"schema"`
	// History is the type's compaction declaration — the 0019 shape at
	// type level (decision 0022 § 4): the soft tier, honored by the
	// node's roll-up gate, never server-enforced.
	History string `json:"history,omitempty"`
	// Aspects maps segment names to the types that may live under a
	// thing of this type (decision 0022): {segment → type}. Declared
	// means possible, not present; the latest declaration wins.
	Aspects map[string]string `json:"aspects,omitempty"`
	// Operations is the op vocabulary, keyed by op-type string (natural
	// dots kept). An operation is defined inside exactly one type and
	// writes to exactly one subject when invoked (0021 § 2).
	Operations map[string]OpDef `json:"operations,omitempty"`
}

// OpDef is one operation's definition inside its one type: the payload's
// JSON Schema and the effect a write of this operation has on the
// subject's state (decision 0011, unchanged in substance).
type OpDef struct {
	Schema json.RawMessage `json:"schema"`
	Effect string          `json:"effect,omitempty"`
}

// The effect vocabulary (decision 0011). It grows additively; the fold
// treats an unknown value as EffectNone with a warning, while SCHEMA.SET
// refuses values outside the node's vocabulary — write-side strictness,
// read-side tolerance.
const (
	// EffectNone: the op is recorded, validated, and replayable, but moves
	// no state — it lives in history. The default. Its presence in a tail
	// also vetoes node-driven compaction (the op's meaning would be lost).
	EffectNone = "none"
	// EffectMerge: the payload applies to the thing's state as an RFC 7386
	// JSON Merge Patch — named fields overwrite, null deletes.
	EffectMerge = "merge"
)

// NormalizeEffect maps the unset effect to its meaning.
func NormalizeEffect(effect string) string {
	if effect == "" {
		return EffectNone
	}
	return effect
}

// KnownEffect reports whether the value is in this build's vocabulary.
func KnownEffect(effect string) bool {
	switch NormalizeEffect(effect) {
	case EffectNone, EffectMerge:
		return true
	}
	return false
}

// Principal is the value at identity.principal.<id>: a human or agent known
// to chronicle. Possession of issued credentials is authentication.
type Principal struct {
	ID    string `json:"id"`
	Name  string `json:"name,omitempty"`
	Email string `json:"email,omitempty"`
}

// Membership is the value at identity.member.<id>: the principal's NATS user
// public key and role. A role is data, enforced by chronicle's API surface
// at request time.
type Membership struct {
	PublicKey string `json:"public_key"`
	Role      string `json:"role"`
}

// The roles to start (onboarding design § identity).
const (
	RoleAdmin  = "admin"
	RoleWriter = "writer"
	RoleReader = "reader"
)
