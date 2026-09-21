// Package foldcore is the shared per-op judgment under the current META
// declarations (decisions 0011 and 0021): one place decides what a
// non-snapshot op does to state, and each projection maps the decision to
// its own act — the node's fold warns and writes, rollup vetoes, an
// indexer warns and indexes. A projection carrying its own copy of these
// rules is a drift hazard: a new effect value would have to land in every
// copy at once.
//
// Since 0021 the vocabulary lives in type records, so judgment is in two
// halves: resolve the thing to its type (the pair walk of ResolveThing
// for tenant logs; the fleet resolves by family), then judge the op
// through the type's operations (JudgeRecord). Pass drives both halves
// per op and keeps the per-thing frontier — the one fold implementation
// every materialization shares (0023 § 3).
package foldcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
)

// Decision is the contract's fold decision (contract.Decision), kept
// under its old name here: the rules moved to the contract with design 12
// so an SDK performing the exactness recipe judges with the same code;
// the pass that drives them per subject stays here.
type Decision = contract.Decision

// The decisions, under their old names.
const (
	Merge             = contract.Merge
	None              = contract.None
	UnknownType       = contract.UnknownType
	UnknownEffect     = contract.UnknownEffect
	BadTypeRecord     = contract.BadTypeRecord
	Invalid           = contract.Invalid
	Undeclared        = contract.Undeclared
	Reset             = contract.Reset
	MalformedSnapshot = contract.MalformedSnapshot
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

// LogFingerprint combines every type record's fold fingerprint into the
// log's declaration watermark (0023 § 4): equal watermarks mean a state
// bucket's values were derived under the current declarations, so a
// state-sourced indexer may bootstrap from them.
func LogFingerprint(ctx context.Context, meta jetstream.KeyValue, log string) (string, error) {
	prefix := contract.MetaLogType(log, "")
	lister, err := meta.ListKeysFiltered(ctx, prefix+">")
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return "", nil
		}
		return "", fmt.Errorf("list type records: %w", err)
	}
	var names []string
	for key := range lister.Keys() {
		names = append(names, strings.TrimPrefix(key, prefix))
	}
	sort.Strings(names)
	lookup := Lookup(ctx, meta, log)
	var b strings.Builder
	for _, name := range names {
		rec, ok, err := lookup(name)
		if err != nil {
			return "", err
		}
		if !ok {
			continue
		}
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(FoldFingerprint(rec))
		b.WriteByte('\n')
	}
	return b.String(), nil
}

// JudgeSnapshot is contract.JudgeSnapshot under its old name.
func JudgeSnapshot(rec *contract.TypeRecord, state json.RawMessage) string {
	return contract.JudgeSnapshot(rec, state)
}

// JudgeRecord is contract.JudgeRecord under its old name.
func JudgeRecord(rec *contract.TypeRecord, op contract.Op) (Decision, string) {
	return contract.JudgeRecord(rec, op)
}
