package node_test

import (
	"iter"
	"testing"
)

// collect gathers an iterator for assertions — the slice the tests
// compare, from the collection the client streams (design 12).
func collect[T any](t *testing.T, items iter.Seq2[T, error]) []T {
	t.Helper()
	var out []T
	for item, err := range items {
		if err != nil {
			t.Fatalf("collect: %v", err)
		}
		out = append(out, item)
	}
	return out
}
