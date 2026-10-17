package contract

import (
	"encoding/json"
	"fmt"
)

// Decision is what one op does to state under the current declarations —
// the fold rules of 0011 under the type records of 0021/0022, stated once
// so every projection, and every SDK performing the exactness recipe,
// judges alike (design 12: the fold rules are the contract's; the pass
// that drives them per subject is foldcore's).
type Decision int

const (
	// Merge — the op is schema-valid and its operation declares effect
	// merge: the payload applies to the thing's state as an RFC 7386
	// merge patch — onto empty state when the subject has none, because
	// a declared merge is a birth (0025 § 3).
	Merge Decision = iota
	// None — the operation declares effect none (the default): the op
	// lives in history and moves no state.
	None
	// UnknownType — the thing's type defines no such operation (or the
	// thing is untyped); readers ignore the op with a warning.
	UnknownType
	// UnknownEffect — the operation declares an effect outside this
	// build's vocabulary: treated as none with a warning (read-side
	// tolerance).
	UnknownEffect
	// BadTypeRecord — the type record is unreadable or a schema in it
	// does not compile; the op moves nothing.
	BadTypeRecord
	// Invalid — the op's payload is not JSON, fails its operation's
	// schema, or cannot merge: marked, never dropped, takes no effect.
	Invalid
	// Undeclared — the thing is an aspect its parent's type does not
	// declare (0022 § 3): the whole subject is marked, taking no effect
	// anywhere state is derived, until a declaration redeems it.
	Undeclared
	// Reset — a snapshot replaced the thing's state with its payload's.
	Reset
	// MalformedSnapshot — a snapshot whose payload is not a snapshot, or
	// whose state fails the type's thing schema: marked, takes no effect.
	MalformedSnapshot
	// Unverified — the store requires verified authors (0057) and the op
	// carries no attestation that verifies against an attester key in
	// META: marked, takes no effect, the history keeps it.
	Unverified
	// PreconditionUnmet — the operation declares a precondition (0059)
	// and the thing's current state does not satisfy it: marked, takes
	// no effect, the history keeps it.
	PreconditionUnmet
	// OffSchema — a declared merge whose result fails the type's thing
	// schema (0059): the schema is the shape every derived state keeps,
	// so the op is marked and the state stands where it was.
	OffSchema
)

// String names the decision as the artifact and the fixtures spell it.
func (d Decision) String() string {
	switch d {
	case Merge:
		return "merge"
	case None:
		return "none"
	case UnknownType:
		return "unknown-type"
	case UnknownEffect:
		return "unknown-effect"
	case BadTypeRecord:
		return "bad-type-record"
	case Invalid:
		return "invalid"
	case Undeclared:
		return "undeclared"
	case Reset:
		return "reset"
	case MalformedSnapshot:
		return "malformed-snapshot"
	case Unverified:
		return "unverified"
	case PreconditionUnmet:
		return "precondition-unmet"
	case OffSchema:
		return "off-schema"
	}
	return fmt.Sprintf("decision(%d)", int(d))
}

// JudgeSnapshot validates a snapshot's state against a resolved type's
// thing schema (0021 § 3): state that fails its shape is marked, never
// dropped. Empty detail means the state stands; a record without a shape
// declared is tolerated read-side.
func JudgeSnapshot(rec *TypeRecord, state json.RawMessage) string {
	if rec == nil || len(rec.Schema) == 0 {
		return ""
	}
	sch, err := CompileSchema(rec.Schema)
	if err != nil {
		return fmt.Sprintf("the type's schema does not compile: %v", err)
	}
	var v any
	if err := json.Unmarshal(state, &v); err != nil {
		return fmt.Sprintf("state is not JSON: %v", err)
	}
	if err := sch.Validate(v); err != nil {
		return fmt.Sprintf("state fails the type's schema: %v", err)
	}
	return ""
}

// JudgeRecord decides one non-snapshot op under one type record, against
// the thing's current state — latest declaration wins (0011 § 3), so the
// same op can judge differently after a declaration changes, and derived
// state is rebuilt by replay when it does. The payload's schema is judged
// first, then the operation's precondition against the state (0059; the
// empty object when the thing has none yet), then the effect. The detail
// carries the specifics a caller may want in its warning or veto; it is
// empty for Merge and None.
func JudgeRecord(rec *TypeRecord, state json.RawMessage, op Op) (Decision, string) {
	def, ok := rec.Operations[op.Type]
	if !ok {
		return UnknownType, fmt.Sprintf("the type defines no operation %s", op.Type)
	}
	sch, err := CompileSchema(def.Schema)
	if err != nil {
		return BadTypeRecord, fmt.Sprintf("compile operation schema: %v", err)
	}
	var v any
	if err := json.Unmarshal(op.Payload, &v); err != nil {
		return Invalid, fmt.Sprintf("payload is not JSON: %v", err)
	}
	if err := sch.Validate(v); err != nil {
		return Invalid, fmt.Sprintf("payload fails its schema: %v", err)
	}
	if detail := JudgePrecondition(def, state); detail != "" {
		return PreconditionUnmet, detail
	}
	switch effect := NormalizeEffect(def.Effect); effect {
	case EffectNone:
		return None, ""
	case EffectMerge:
		return Merge, ""
	default:
		return UnknownEffect, fmt.Sprintf("effect %q is outside this build's vocabulary", effect)
	}
}

// JudgePrecondition validates the thing's current state against the
// operation's precondition (0059). Empty detail means the operation may
// apply: no precondition declared, or the state satisfies it. A thing
// with no state yet is judged as the empty object — a precondition on
// create sees what a create sees.
func JudgePrecondition(def OpDef, state json.RawMessage) string {
	if len(def.Precondition) == 0 {
		return ""
	}
	pre, err := CompileSchema(def.Precondition)
	if err != nil {
		return fmt.Sprintf("the operation's precondition does not compile: %v", err)
	}
	var cur any = map[string]any{}
	if len(state) > 0 {
		if err := json.Unmarshal(state, &cur); err != nil {
			return fmt.Sprintf("the operation's precondition is not met: state is not JSON: %v", err)
		}
	}
	if err := pre.Validate(cur); err != nil {
		return fmt.Sprintf("the operation's precondition is not met: %v", err)
	}
	return ""
}

// FoldOutcome is one fold step's result: what the op did, why, and the
// thing's state afterwards.
type FoldOutcome struct {
	Decision Decision
	Detail   string
	// State is the thing's state after the step — the input state when
	// nothing moved. Moved says whether the step changed it.
	State json.RawMessage
	Moved bool
}

// FoldStep applies one op to a thing's current state under its resolution
// — the per-op rules of 03-meta-and-state.md § the state buckets, complete
// and pure: a marked subject takes no effect; a snapshot resets state to
// its payload's, judged against the thing schema where the thing is typed;
// a declared merge applies onto current state, empty state included, and
// its result is judged against the thing schema as a snapshot's is (0059:
// the schema is the shape every derived state keeps); a precondition the
// state does not satisfy marks the op; none, an unknown type, an unknown
// effect, a bad record or an invalid payload moves nothing; an untyped
// subject moves only by snapshot.
func FoldStep(res Resolution, state json.RawMessage, op Op) FoldOutcome {
	if res.Kind == ResolvedUndeclared {
		return FoldOutcome{Decision: Undeclared, Detail: res.Detail, State: state}
	}
	if op.Type == OpTypeSnapshot {
		snap, err := ParseSnapshot(op.Payload)
		if err != nil {
			return FoldOutcome{Decision: MalformedSnapshot, Detail: err.Error(), State: state}
		}
		if res.Kind == ResolvedTyped {
			if detail := JudgeSnapshot(res.Record, snap.State); detail != "" {
				return FoldOutcome{Decision: MalformedSnapshot, Detail: detail, State: state}
			}
		}
		return FoldOutcome{Decision: Reset, State: snap.State, Moved: true}
	}
	if res.Kind != ResolvedTyped {
		return FoldOutcome{Decision: UnknownType, Detail: "instance is untyped: " + res.Detail, State: state}
	}
	decision, detail := JudgeRecord(res.Record, state, op)
	if decision != Merge {
		return FoldOutcome{Decision: decision, Detail: detail, State: state}
	}
	merged, err := MergePatch(state, op.Payload)
	if err != nil {
		return FoldOutcome{Decision: Invalid, Detail: "merge failed: " + err.Error(), State: state}
	}
	if detail := JudgeSnapshot(res.Record, merged); detail != "" {
		return FoldOutcome{Decision: OffSchema, Detail: "after the operation, " + detail, State: state}
	}
	return FoldOutcome{Decision: Merge, State: merged, Moved: true}
}
