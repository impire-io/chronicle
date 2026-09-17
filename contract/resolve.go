package contract

import (
	"fmt"
	"strings"
)

// Resolution classifies one thing tail against the log's declared types:
// the pair-addressing grammar of decisions 0021 § 4 and 0022 § 2. The
// whole model is read-side vocabulary — resolution never gates a write.
type Resolution struct {
	Kind ResolutionKind
	// TypeName and Record are the resolved (final) type when Kind is
	// ResolvedTyped.
	TypeName string
	Record   *TypeRecord
	// Detail says why a tail is untyped or undeclared.
	Detail string
}

// ResolutionKind is a tail's standing under the current declarations.
type ResolutionKind int

const (
	// ResolvedUntyped — the tail does not read as <type>.<id> pairs, or
	// its first token names no declared type: the vocabulary-less floor.
	// Every op judges unknown, the 0011 § 4 compaction veto stands, and
	// declaring the type later types the thing retroactively.
	ResolvedUntyped ResolutionKind = iota
	// ResolvedTyped — every pair resolved; Record is the final type.
	ResolvedTyped
	// ResolvedUndeclared — a pair's segment is not in its parent type's
	// aspects map (0022 § 3): the subject is marked, taking no effect
	// anywhere state is derived, until a declaration redeems it.
	ResolvedUndeclared
)

// TypeLookup reads one type record by name; ok is false when the type is
// not declared. Callers close over their own META access.
type TypeLookup func(name string) (*TypeRecord, bool, error)

// ResolveTail walks a thing tail as alternating <segment>.<id> pairs,
// left to right: the first pair against the log's types, each further
// pair through the current type's aspects map. Only the error from a
// lookup escapes; every grammar outcome is a Resolution.
func ResolveTail(thing string, lookup TypeLookup) (Resolution, error) {
	toks := strings.Split(thing, ".")
	if len(toks)%2 != 0 {
		return Resolution{Kind: ResolvedUntyped, Detail: "the tail does not read as <type>.<id> pairs"}, nil
	}
	name := toks[0]
	rec, ok, err := lookup(name)
	if err != nil {
		return Resolution{}, err
	}
	if !ok {
		return Resolution{Kind: ResolvedUntyped, Detail: fmt.Sprintf("no type %q is declared", name)}, nil
	}
	for i := 2; i < len(toks); i += 2 {
		seg := toks[i]
		aspectType, declared := rec.Aspects[seg]
		if !declared {
			return Resolution{Kind: ResolvedUndeclared, Detail: fmt.Sprintf("type %q does not declare aspect segment %q", name, seg)}, nil
		}
		rec, ok, err = lookup(aspectType)
		if err != nil {
			return Resolution{}, err
		}
		if !ok {
			return Resolution{Kind: ResolvedUndeclared, Detail: fmt.Sprintf("aspect type %q (segment %q of %q) is not declared", aspectType, seg, name)}, nil
		}
		name = aspectType
	}
	return Resolution{Kind: ResolvedTyped, TypeName: name, Record: rec}, nil
}
