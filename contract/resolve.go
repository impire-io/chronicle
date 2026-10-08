package contract

import (
	"fmt"
	"strings"
)

// Resolution classifies one instance's tail against the store's defined
// types: the path grammar of decisions 0021 § 4 and 0022 § 2, in the words
// of 0044. The whole model is read-side vocabulary — resolution never
// gates a write.
type Resolution struct {
	Kind ResolutionKind
	// TypeName and Record are the resolved (final) type when Kind is
	// ResolvedTyped.
	TypeName string
	Record   *TypeRecord
	// Detail says why a tail is untyped or undeclared.
	Detail string
}

// ResolutionKind is a tail's standing under the current definitions.
type ResolutionKind int

const (
	// ResolvedUntyped — the tail does not read as type/id pairs, or its
	// first token names no defined type: the vocabulary-less floor. Every
	// op judges unknown, the 0011 § 4 compaction veto stands, and defining
	// the type later types the instance retroactively.
	ResolvedUntyped ResolutionKind = iota
	// ResolvedTyped — every pair resolved; Record is the final type.
	ResolvedTyped
	// ResolvedUndeclared — a pair's name is not in its parent type's
	// children map (0022 § 3): the subject is marked, taking no effect
	// anywhere state is derived, until a definition redeems it.
	ResolvedUndeclared
)

// TypeLookup reads one type record by name; ok is false when the type is
// not defined. Callers close over their own META access.
type TypeLookup func(name string) (*TypeRecord, bool, error)

// ResolveInstance walks an instance's tail as alternating name.id pairs,
// left to right: the first pair against the store's types, each further
// pair through the current type's children map. Only the error from a
// lookup escapes; every grammar outcome is a Resolution.
func ResolveInstance(tail string, lookup TypeLookup) (Resolution, error) {
	toks := strings.Split(tail, TailSeparator)
	if len(toks)%2 != 0 {
		return Resolution{Kind: ResolvedUntyped, Detail: "the path does not read as type/id pairs"}, nil
	}
	typeName := toks[0]
	rec, ok, err := lookup(typeName)
	if err != nil {
		return Resolution{}, err
	}
	if !ok {
		return Resolution{Kind: ResolvedUntyped, Detail: fmt.Sprintf("no type %q is defined", typeName)}, nil
	}
	for i := 2; i < len(toks); i += 2 {
		childName := toks[i]
		childType, declared := rec.Children[childName]
		if !declared {
			return Resolution{Kind: ResolvedUndeclared, Detail: fmt.Sprintf("type %q declares no child %q", typeName, childName)}, nil
		}
		rec, ok, err = lookup(childType)
		if err != nil {
			return Resolution{}, err
		}
		if !ok {
			return Resolution{Kind: ResolvedUndeclared, Detail: fmt.Sprintf("child type %q (%q of %q) is not defined", childType, childName, typeName)}, nil
		}
		typeName = childType
	}
	return Resolution{Kind: ResolvedTyped, TypeName: typeName, Record: rec}, nil
}
