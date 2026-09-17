// Package foldcore is the shared per-op judgment under the current META
// declarations (decisions 0011 and 0021): one place decides what a
// non-snapshot op does to state, and each projection maps the decision to
// its own act — the node's fold warns and writes, rollup vetoes, an
// indexer warns and indexes. A projection carrying its own copy of these
// rules is a drift hazard: a new effect value would have to land in every
// copy at once.
//
// Since 0021 the vocabulary lives in type records, so judgment is in two
// halves: resolve the thing to its type (ResolveThing — the tenant path;
// the fleet log resolves by family and calls JudgeAs), then judge the op
// through the type's operations (JudgeRecord).
package foldcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// Decision is what one op does to state under the current declarations.
type Decision int

const (
	// Merge — the op is schema-valid and its operation declares effect
	// merge: the payload applies to the thing's state as an RFC 7386
	// merge patch.
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
	// Invalid — the op's payload is not JSON or fails its operation's
	// schema: marked, never dropped, takes no effect.
	Invalid
	// Undeclared — the thing is an aspect its parent's type does not
	// declare (0022 § 3): the whole subject is marked, taking no effect
	// anywhere state is derived, until a declaration redeems it.
	Undeclared
)

// Lookup closes over one log's META bucket as a contract.TypeLookup.
func Lookup(ctx context.Context, meta jetstream.KeyValue, log string) contract.TypeLookup {
	return func(name string) (*contract.TypeRecord, bool, error) {
		entry, err := meta.Get(ctx, contract.MetaLogType(log, name))
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("read type record %s: %w", name, err)
		}
		var rec contract.TypeRecord
		if err := json.Unmarshal(entry.Value(), &rec); err != nil {
			return nil, false, fmt.Errorf("decode type record %s: %w", name, err)
		}
		return &rec, true, nil
	}
}

// ResolveThing resolves a thing tail against the log's declared types —
// the pair walk of 0021 § 4 / 0022 § 2 over live META reads.
func ResolveThing(ctx context.Context, meta jetstream.KeyValue, log, thing string) (contract.Resolution, error) {
	return contract.ResolveTail(thing, Lookup(ctx, meta, log))
}

// JudgeThing is the tenant path: resolve the thing, then judge the op
// through its type. An untyped thing's ops judge UnknownType — the
// vocabulary-less floor; an undeclared aspect's judge Undeclared.
func JudgeThing(ctx context.Context, meta jetstream.KeyValue, log, thing string, op contract.Op) (Decision, string) {
	res, err := ResolveThing(ctx, meta, log, thing)
	if err != nil {
		return BadTypeRecord, err.Error()
	}
	switch res.Kind {
	case contract.ResolvedUntyped:
		return UnknownType, fmt.Sprintf("thing is untyped: %s", res.Detail)
	case contract.ResolvedUndeclared:
		return Undeclared, res.Detail
	}
	return JudgeRecord(res.Record, op)
}

// JudgeAs judges an op through one named type record — the fleet log's
// path, where the family names the type and no pair resolution applies.
// A missing record judges UnknownType, like any unknown vocabulary.
func JudgeAs(ctx context.Context, meta jetstream.KeyValue, log, typeName string, op contract.Op) (Decision, string) {
	rec, ok, err := Lookup(ctx, meta, log)(typeName)
	if err != nil {
		return BadTypeRecord, err.Error()
	}
	if !ok {
		return UnknownType, fmt.Sprintf("type %s has no declaration", typeName)
	}
	return JudgeRecord(rec, op)
}

// FoldFingerprint captures the facets of a type record whose change makes
// derived state suspect (0021 § 5): history, aspects, and each effective
// operation's effect. Schema-only revisions — thing or op — change no
// derivation, and an added effect-none operation moves nothing either.
// A nil record is the undeclared baseline, so a first definition carrying
// anything effective reads as changed, and a deletion reads against it.
func FoldFingerprint(rec *contract.TypeRecord) string {
	if rec == nil {
		rec = &contract.TypeRecord{}
	}
	fp := struct {
		History string            `json:"h"`
		Aspects map[string]string `json:"a,omitempty"`
		Effects map[string]string `json:"e,omitempty"`
	}{History: contract.NormalizeHistory(rec.History)}
	if len(rec.Aspects) > 0 {
		fp.Aspects = rec.Aspects
	}
	for op, def := range rec.Operations {
		if contract.NormalizeEffect(def.Effect) != contract.EffectNone {
			if fp.Effects == nil {
				fp.Effects = map[string]string{}
			}
			fp.Effects[op] = contract.NormalizeEffect(def.Effect)
		}
	}
	b, _ := json.Marshal(fp)
	return string(b)
}

// JudgeSnapshot validates a snapshot's state against a resolved type's
// thing schema (0021 § 3): state that fails its shape is marked, never
// dropped. Empty detail means the state stands; a record without a shape
// declared is tolerated read-side.
func JudgeSnapshot(rec *contract.TypeRecord, state json.RawMessage) string {
	if len(rec.Schema) == 0 {
		return ""
	}
	sch, err := client.CompileSchema(rec.Schema)
	if err != nil {
		return fmt.Sprintf("thing schema does not compile: %v", err)
	}
	var v any
	if err := json.Unmarshal(state, &v); err != nil {
		return fmt.Sprintf("state is not JSON: %v", err)
	}
	if err := sch.Validate(v); err != nil {
		return fmt.Sprintf("state fails the thing schema: %v", err)
	}
	return ""
}

// JudgeRecord decides one non-snapshot op under one type record — latest
// declaration wins (0011 § 3), so the same op can judge differently after
// a declaration changes, and derived state is rebuilt by replay when it
// does. The detail carries the specifics a caller may want in its warning
// or veto; it is empty for Merge and None.
func JudgeRecord(rec *contract.TypeRecord, op contract.Op) (Decision, string) {
	def, ok := rec.Operations[op.Type]
	if !ok {
		return UnknownType, fmt.Sprintf("the type defines no operation %s", op.Type)
	}
	sch, err := client.CompileSchema(def.Schema)
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
	switch effect := contract.NormalizeEffect(def.Effect); effect {
	case contract.EffectNone:
		return None, ""
	case contract.EffectMerge:
		return Merge, ""
	default:
		return UnknownEffect, fmt.Sprintf("effect %q is outside this build's vocabulary", effect)
	}
}
