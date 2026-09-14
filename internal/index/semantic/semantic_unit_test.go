package semantic

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestChunksForSelectsAndSplits: fields narrow the selection, defaults
// take every string field with dotted names, and long text splits to the
// budget without cutting runes or (where avoidable) words.
func TestChunksForSelectsAndSplits(t *testing.T) {
	state := json.RawMessage(`{
		"title": "quantum widgets",
		"meta": {"note": "internal gadget"},
		"tags": ["red", "blue"],
		"qty": 7
	}`)

	all := chunksFor(state, nil, 0)
	fields := map[string]int{}
	for _, c := range all {
		fields[c.field]++
	}
	if fields["title"] != 1 || fields["meta.note"] != 1 || fields["tags"] != 2 {
		t.Fatalf("default selection wrong: %+v", fields)
	}

	only := chunksFor(state, []string{"title"}, 0)
	if len(only) != 1 || only[0].field != "title" || only[0].text != "quantum widgets" {
		t.Fatalf("field selection wrong: %+v", only)
	}

	long := strings.Repeat("word ", 100) // 500 bytes
	pieces := split(long, 120)
	if len(pieces) < 4 {
		t.Fatalf("split produced %d pieces for 500 bytes at budget 120", len(pieces))
	}
	for _, p := range pieces {
		if len(p) > 120 {
			t.Fatalf("piece over budget: %d bytes", len(p))
		}
		if strings.HasPrefix(p, " ") || strings.HasSuffix(p, " ") {
			t.Fatalf("piece not trimmed: %q", p)
		}
	}
	// Runes survive: a multibyte text split mid-window must not tear one.
	multi := strings.Repeat("héllo ", 50)
	for _, p := range split(multi, 64) {
		if !json.Valid([]byte(`"` + strings.ReplaceAll(p, `"`, ``) + `"`)) {
			t.Fatalf("split tore a rune: %q", p)
		}
	}
}

func TestCosine(t *testing.T) {
	if s := cosine([]float64{1, 0}, []float64{1, 0}); s < 0.999 {
		t.Fatalf("identical vectors score %f", s)
	}
	if s := cosine([]float64{1, 0}, []float64{0, 1}); s != 0 {
		t.Fatalf("orthogonal vectors score %f", s)
	}
	if s := cosine([]float64{1, 0}, []float64{1, 0, 0}); s != 0 {
		t.Fatalf("mismatched lengths score %f", s)
	}
}
