package conformance_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/contract"
)

func load(t *testing.T, name string, into any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("fixtures", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("decode fixture %s: %v", name, err)
	}
}

func jsonEqual(a, b json.RawMessage) bool {
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &y); err != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

func TestFixtureNames(t *testing.T) {
	var f struct {
		Cases []struct {
			Kind, Input string
			Valid       bool
		}
	}
	load(t, "names.json", &f)
	rules := map[string]func(string) error{
		"log": contract.ValidateLogName, "index": contract.ValidateIndexName, "type": contract.ValidateTypeName,
		"principal": contract.ValidatePrincipalName, "thing": contract.ValidateThing,
	}
	for _, c := range f.Cases {
		rule, ok := rules[c.Kind]
		if !ok {
			t.Fatalf("no rule for kind %q", c.Kind)
		}
		if got := rule(c.Input) == nil; got != c.Valid {
			t.Errorf("%s %q: valid=%v, want %v", c.Kind, c.Input, got, c.Valid)
		}
	}
}

func TestFixtureDerivations(t *testing.T) {
	var f struct {
		Cases []struct {
			Log, Thing, Upper, Stream, StateBucket, LogSubjects, OpsFilter, OpsSubject, OpsPrefix, ThingFromSubject, MetaLogConfig string
			MetaType                                                                                                               struct{ Type, Key string }
			MetaIndex                                                                                                              struct{ Index, Key string }
		}
		Identity struct {
			Principal struct{ ID, Key string }
			Member    struct{ ID, Key string }
			Invite    struct{ Digest, Key string }
		}
	}
	load(t, "derivations.json", &f)
	for _, c := range f.Cases {
		got := map[string]string{
			"upper": contract.UpperLog(c.Log), "stream": contract.StreamName(c.Log), "stateBucket": contract.StateBucket(c.Log),
			"logSubjects": contract.LogSubjects(c.Log), "opsFilter": contract.OpsFilter(c.Log), "opsSubject": contract.OpsSubject(c.Log, c.Thing),
			"opsPrefix": contract.OpsPrefix(c.Log), "thingFromSubject": contract.ThingFromSubject(c.Log, contract.OpsSubject(c.Log, c.Thing)),
			"metaLogConfig": contract.MetaLogConfig(c.Log), "metaType": contract.MetaLogType(c.Log, c.MetaType.Type), "metaIndex": contract.MetaIndex(c.Log, c.MetaIndex.Index),
		}
		want := map[string]string{
			"upper": c.Upper, "stream": c.Stream, "stateBucket": c.StateBucket, "logSubjects": c.LogSubjects, "opsFilter": c.OpsFilter,
			"opsSubject": c.OpsSubject, "opsPrefix": c.OpsPrefix, "thingFromSubject": c.ThingFromSubject, "metaLogConfig": c.MetaLogConfig,
			"metaType": c.MetaType.Key, "metaIndex": c.MetaIndex.Key,
		}
		for k := range want {
			if got[k] != want[k] {
				t.Errorf("%s of %q/%q = %q, want %q", k, c.Log, c.Thing, got[k], want[k])
			}
		}
	}
	if k := contract.MetaPrincipal(f.Identity.Principal.ID); k != f.Identity.Principal.Key {
		t.Errorf("principal key %q, want %q", k, f.Identity.Principal.Key)
	}
	if k := contract.MetaMember(f.Identity.Member.ID); k != f.Identity.Member.Key {
		t.Errorf("member key %q, want %q", k, f.Identity.Member.Key)
	}
	if k := contract.MetaInvite(f.Identity.Invite.Digest); k != f.Identity.Invite.Key {
		t.Errorf("invite key %q, want %q", k, f.Identity.Invite.Key)
	}
}

func TestFixtureHeaders(t *testing.T) {
	var f struct {
		Cases []struct {
			Name string
			Op   struct {
				ID, Type, Author, Ts, Version string
				Parents                       []string
			}
			Headers map[string][]string
		}
	}
	load(t, "headers.json", &f)
	for _, c := range f.Cases {
		op := contract.Op{ID: c.Op.ID, Type: c.Op.Type, Author: c.Op.Author, Parents: c.Op.Parents, Version: c.Op.Version}
		if c.Op.Ts != "" {
			ts, err := time.Parse(time.RFC3339Nano, c.Op.Ts)
			if err != nil {
				t.Fatalf("%s: fixture ts: %v", c.Name, err)
			}
			op.Ts = ts
		}
		got := map[string][]string(op.Header())
		if !reflect.DeepEqual(got, c.Headers) {
			t.Errorf("%s: headers %v, want %v", c.Name, got, c.Headers)
		}
		back := contract.ParseOp("subject", 1, nats.Header(c.Headers), nil)
		wantVersion := c.Op.Version
		if wantVersion == "" {
			wantVersion = contract.EnvelopeVersion
		}
		if back.ID != c.Op.ID || back.Type != c.Op.Type || back.Author != c.Op.Author || back.Version != wantVersion || !back.Ts.Equal(op.Ts) {
			t.Errorf("%s: parsed back %+v", c.Name, back)
		}
		if len(back.Parents) != len(c.Op.Parents) || (len(back.Parents) > 0 && !reflect.DeepEqual(back.Parents, c.Op.Parents)) {
			t.Errorf("%s: parents parsed back %v, want %v", c.Name, back.Parents, c.Op.Parents)
		}
	}
}

func lookupOf(types map[string]json.RawMessage) contract.TypeLookup {
	return func(name string) (*contract.TypeRecord, bool, error) {
		raw, ok := types[name]
		if !ok {
			return nil, false, nil
		}
		var rec contract.TypeRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, false, err
		}
		return &rec, true, nil
	}
}

func kindName(k contract.ResolutionKind) string {
	switch k {
	case contract.ResolvedTyped:
		return "typed"
	case contract.ResolvedUndeclared:
		return "undeclared"
	}
	return "untyped"
}

func TestFixtureResolve(t *testing.T) {
	var f struct {
		Types map[string]json.RawMessage
		Cases []struct{ Thing, Kind, Type string }
	}
	load(t, "resolve.json", &f)
	lookup := lookupOf(f.Types)
	for _, c := range f.Cases {
		res, err := contract.ResolveTail(c.Thing, lookup)
		if err != nil {
			t.Fatalf("%s: %v", c.Thing, err)
		}
		if kindName(res.Kind) != c.Kind || res.TypeName != c.Type {
			t.Errorf("%s: %s %q (%s), want %s %q", c.Thing, kindName(res.Kind), res.TypeName, res.Detail, c.Kind, c.Type)
		}
	}
}

func TestFixtureMerge(t *testing.T) {
	var f struct {
		Cases []struct{ Target, Patch, Result json.RawMessage }
	}
	load(t, "merge.json", &f)
	for _, c := range f.Cases {
		got, err := contract.MergePatch(c.Target, c.Patch)
		if err != nil {
			t.Errorf("merge %s + %s: %v", c.Target, c.Patch, err)
			continue
		}
		if !jsonEqual(got, c.Result) {
			t.Errorf("merge %s + %s = %s, want %s", c.Target, c.Patch, got, c.Result)
		}
	}
}

func TestFixtureFold(t *testing.T) {
	var f struct {
		Types map[string]json.RawMessage
		Cases []struct {
			Name, Thing string
			Ops         []struct {
				Type    string
				Payload json.RawMessage
				Raw     string
			}
			Decisions []string
			State     json.RawMessage
		}
	}
	load(t, "fold.json", &f)
	lookup := lookupOf(f.Types)
	for _, c := range f.Cases {
		res, err := contract.ResolveTail(c.Thing, lookup)
		if err != nil {
			t.Fatalf("%s: resolve: %v", c.Name, err)
		}
		var state json.RawMessage
		var decisions []string
		for i, o := range c.Ops {
			op := contract.Op{ID: "op", Type: o.Type, Seq: uint64(i + 1), Payload: o.Payload}
			if o.Raw != "" {
				op.Payload = []byte(o.Raw)
			}
			out := contract.FoldStep(res, state, op)
			decisions = append(decisions, out.Decision.String())
			if out.Moved {
				state = out.State
			}
		}
		if !reflect.DeepEqual(decisions, c.Decisions) {
			t.Errorf("%s: decisions %v, want %v", c.Name, decisions, c.Decisions)
		}
		wantNone := string(c.State) == "null"
		switch {
		case wantNone && len(state) != 0:
			t.Errorf("%s: state %s, want none", c.Name, state)
		case !wantNone && !jsonEqual(state, c.State):
			t.Errorf("%s: state %s, want %s", c.Name, state, c.State)
		}
	}
}

func TestFixtureGuardRetry(t *testing.T) {
	var f struct {
		Cases []struct {
			Name, LastOpID, RetriedOpID string
			Landed                      bool
		}
	}
	load(t, "guard-retry.json", &f)
	for _, c := range f.Cases {
		// The rule the client applies after a guard refusal (client/ops.go
		// ownOpLanded): the subject's last op carries the retried ID, or
		// the thing moved.
		if landed := c.LastOpID != "" && c.LastOpID == c.RetriedOpID; landed != c.Landed {
			t.Errorf("%s: landed=%v, want %v", c.Name, landed, c.Landed)
		}
	}
}
