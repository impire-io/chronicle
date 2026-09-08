// Package foldcore is the shared per-op judgment under the current META
// declarations (decision 0011): one place decides what a non-snapshot op
// does to state, and each projection maps the decision to its own act —
// the node's fold warns and writes, rollup vetoes, an indexer warns and
// indexes. A projection carrying its own copy of these rules is a drift
// hazard: a new effect value would have to land in every copy at once.
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
	// Merge — the op is schema-valid and its type declares effect merge:
	// the payload applies to the thing's state as an RFC 7386 merge patch.
	Merge Decision = iota
	// None — the type declares effect none (the default): the op lives in
	// history and moves no state.
	None
	// UnknownType — the op's type has no declaration; readers ignore it
	// with a warning.
	UnknownType
	// UnknownEffect — the type declares an effect outside this build's
	// vocabulary: treated as none with a warning (read-side tolerance).
	UnknownEffect
	// BadTypeRecord — the type record is unreadable or its schema does
	// not compile; the op moves nothing.
	BadTypeRecord
	// Invalid — the op's payload is not JSON or fails its type's schema:
	// marked, never dropped, takes no effect.
	Invalid
)

// Judge decides one non-snapshot op under the current declarations —
// latest declaration wins (0011 § 3), so the same op can judge
// differently after a declaration changes, and derived state is rebuilt
// by replay when it does. The detail carries the specifics a caller may
// want in its warning or veto; it is empty for Merge and None.
func Judge(ctx context.Context, meta jetstream.KeyValue, log string, op contract.Op) (Decision, string) {
	entry, err := meta.Get(ctx, contract.MetaLogType(log, op.Type))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return UnknownType, fmt.Sprintf("type %s has no declaration", op.Type)
	}
	if err != nil {
		return BadTypeRecord, fmt.Sprintf("read type record: %v", err)
	}
	var ts contract.TypeSchema
	if err := json.Unmarshal(entry.Value(), &ts); err != nil {
		return BadTypeRecord, fmt.Sprintf("decode type record: %v", err)
	}
	sch, err := client.CompileSchema(ts.Schema)
	if err != nil {
		return BadTypeRecord, fmt.Sprintf("compile schema: %v", err)
	}
	var v any
	if err := json.Unmarshal(op.Payload, &v); err != nil {
		return Invalid, fmt.Sprintf("payload is not JSON: %v", err)
	}
	if err := sch.Validate(v); err != nil {
		return Invalid, fmt.Sprintf("payload fails its schema: %v", err)
	}
	switch effect := contract.NormalizeEffect(ts.Effect); effect {
	case contract.EffectNone:
		return None, ""
	case contract.EffectMerge:
		return Merge, ""
	default:
		return UnknownEffect, fmt.Sprintf("effect %q is outside this build's vocabulary", effect)
	}
}
