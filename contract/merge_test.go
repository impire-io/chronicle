package contract

import (
	"encoding/json"
	"testing"
)

// TestMergePatchRFC7386 is the RFC's own appendix-A example table, plus the
// vocabulary helpers.
func TestMergePatchRFC7386(t *testing.T) {
	cases := []struct{ target, patch, want string }{
		{`{"a":"b"}`, `{"a":"c"}`, `{"a":"c"}`},
		{`{"a":"b"}`, `{"b":"c"}`, `{"a":"b","b":"c"}`},
		{`{"a":"b"}`, `{"a":null}`, `{}`},
		{`{"a":"b","b":"c"}`, `{"a":null}`, `{"b":"c"}`},
		{`{"a":["b"]}`, `{"a":"c"}`, `{"a":"c"}`},
		{`{"a":"c"}`, `{"a":["b"]}`, `{"a":["b"]}`},
		{`{"a":{"b":"c"}}`, `{"a":{"b":"d","c":null}}`, `{"a":{"b":"d"}}`},
		{`{"a":[{"b":"c"}]}`, `{"a":[1]}`, `{"a":[1]}`},
		{`["a","b"]`, `["c","d"]`, `["c","d"]`},
		{`{"a":"b"}`, `["c"]`, `["c"]`},
		{`{"a":"foo"}`, `null`, `null`},
		{`{"a":"foo"}`, `"bar"`, `"bar"`},
		{`{"e":null}`, `{"a":1}`, `{"a":1,"e":null}`},
		{`[1,2]`, `{"a":"b","c":null}`, `{"a":"b"}`},
		{`{}`, `{"a":{"bb":{"ccc":null}}}`, `{"a":{"bb":{}}}`},
	}
	for _, c := range cases {
		got, err := MergePatch(json.RawMessage(c.target), json.RawMessage(c.patch))
		if err != nil {
			t.Errorf("MergePatch(%s, %s): %v", c.target, c.patch, err)
			continue
		}
		var g, w any
		if err := json.Unmarshal(got, &g); err != nil {
			t.Errorf("MergePatch(%s, %s) returned invalid JSON %s", c.target, c.patch, got)
			continue
		}
		if err := json.Unmarshal([]byte(c.want), &w); err != nil {
			t.Fatalf("bad case: %s", c.want)
		}
		gj, _ := json.Marshal(g)
		wj, _ := json.Marshal(w)
		if string(gj) != string(wj) {
			t.Errorf("MergePatch(%s, %s) = %s, want %s", c.target, c.patch, got, c.want)
		}
	}
}

func TestEffectVocabulary(t *testing.T) {
	if NormalizeEffect("") != EffectNone {
		t.Error("unset effect must normalize to none")
	}
	if !KnownEffect("") || !KnownEffect(EffectNone) || !KnownEffect(EffectMerge) {
		t.Error("the build's own vocabulary must be known")
	}
	if KnownEffect("wasm:abc") || KnownEffect("patch") {
		t.Error("future vocabulary must be unknown to this build")
	}
}
