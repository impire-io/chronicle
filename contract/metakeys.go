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

// MetaLogType is log.<log>.type.<op.type> — the JSON Schema for the payload
// plus its revision. Op types keep their natural dots (comment.add).
func MetaLogType(log, opType string) string { return "log." + log + ".type." + opType }

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
}

// LogStatusActive is the one status the skeleton knows.
const LogStatusActive = "active"

// TypeSchema is the value at log.<log>.type.<op.type>. Revisions are
// recorded, never rewritten in place: each revision is a new KV put, and the
// bucket's history keeps the old ones readable. The effect declares how the
// op moves state (decision 0011); it rides the same revision as the schema.
type TypeSchema struct {
	Revision uint64          `json:"revision"`
	Schema   json.RawMessage `json:"schema"`
	Effect   string          `json:"effect,omitempty"`
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
