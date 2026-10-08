package client_test

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// The artifact's proof (design 12 § the artifact): every constant,
// pattern, subject, code, default, shape and version the Go contract
// package holds equals the artifact's, and every Go request and reply
// type satisfies the schema the artifact publishes for it. An SDK
// generated from the artifact therefore restates exactly what this build
// speaks; a divergence fails here before it ships.

type artifact struct {
	Version string `json:"version"`
	Shapes  []struct {
		Name string `json:"name"`
	} `json:"shapes"`
	Headers struct {
		Operation       map[string]string `json:"operation"`
		EnvelopeVersion string            `json:"envelopeVersion"`
		Guards          map[string]string `json:"guards"`
		RollupSubject   string            `json:"rollupSubject"`
		Stream          map[string]string `json:"stream"`
	} `json:"headers"`
	Grammars struct {
		Name          struct{ Pattern string } `json:"name"`
		InstanceToken struct{ Pattern string } `json:"instanceToken"`
		Path          struct {
			Separator       string `json:"separator"`
			StoredSeparator string `json:"storedSeparator"`
		} `json:"path"`
		ReservedStoreNames []string          `json:"reservedStoreNames"`
		ServicePrincipal   string            `json:"servicePrincipal"`
		Root               string            `json:"root"`
		MetaBucket         string            `json:"metaBucket"`
		SnapshotOpType     string            `json:"snapshotOpType"`
		StateFoldKey       string            `json:"stateFoldKey"`
		Derivations        map[string]string `json:"derivations"`
		MetaKeys           map[string]string `json:"metaKeys"`
		Roles              []string          `json:"roles"`
		Effects            []string          `json:"effects"`
		History            []string          `json:"history"`
		IndexKinds         []string          `json:"indexKinds"`
		PrincipalKinds     []string          `json:"principalKinds"`
	} `json:"grammars"`
	StreamSettings struct {
		DuplicateWindowSeconds int   `json:"duplicateWindowSeconds"`
		DefaultMaxBytes        int64 `json:"defaultMaxBytes"`
	} `json:"streamSettings"`
	ClientDefaults struct {
		StreamStallSeconds int `json:"streamStallSeconds"`
		StreamPendingMsgs  int `json:"streamPendingMsgs"`
		StreamPendingBytes int `json:"streamPendingBytes"`
		ChunkByteBudget    int `json:"chunkByteBudget"`
		DedupWindowSeconds int `json:"dedupWindowSeconds"`
	} `json:"clientDefaults"`
	Fold struct {
		Decisions []string `json:"decisions"`
	} `json:"fold"`
	Errors       []contract.ErrorCode `json:"errors"`
	Interactions []struct {
		Name    string          `json:"name"`
		Shape   string          `json:"shape"`
		Subject string          `json:"subject"`
		Request json.RawMessage `json:"request"`
		Reply   json.RawMessage `json:"reply"`
		Item    json.RawMessage `json:"item"`
		Trailer json.RawMessage `json:"trailer"`
		Errors  []string        `json:"errors"`
		Headers []string        `json:"headers"`
	} `json:"interactions"`
}

func loadArtifact(t *testing.T) artifact {
	t.Helper()
	raw, err := os.ReadFile("../contract/sdk-contract.json")
	if err != nil {
		t.Fatalf("read the artifact: %v", err)
	}
	var a artifact
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatalf("decode the artifact: %v", err)
	}
	return a
}

func fill(template string, subs map[string]string) string {
	for k, v := range subs {
		template = strings.ReplaceAll(template, k, v)
	}
	return template
}

func TestArtifactMatchesTheContractPackage(t *testing.T) {
	a := loadArtifact(t)
	if a.Version != contract.ContractVersion {
		t.Fatalf("version: artifact %q, package %q", a.Version, contract.ContractVersion)
	}
	var shapes []string
	for _, s := range a.Shapes {
		shapes = append(shapes, s.Name)
	}
	if !reflect.DeepEqual(shapes, contract.Shapes) {
		t.Fatalf("shapes: artifact %v, package %v", shapes, contract.Shapes)
	}

	// The operation record.
	for _, h := range []string{contract.HdrMsgID, contract.HdrOpType, contract.HdrOpAuthor, contract.HdrOpParents, contract.HdrOpTs, contract.HdrOpVersion} {
		if _, ok := a.Headers.Operation[h]; !ok {
			t.Errorf("operation header %s is not in the artifact", h)
		}
	}
	if len(a.Headers.Operation) != 6 {
		t.Errorf("the artifact lists %d operation headers, the record has 6", len(a.Headers.Operation))
	}
	if a.Headers.EnvelopeVersion != contract.EnvelopeVersion {
		t.Errorf("envelope version: artifact %q, package %q", a.Headers.EnvelopeVersion, contract.EnvelopeVersion)
	}
	for _, h := range []string{contract.HdrExpectedLastSubjSeq, contract.HdrRollup} {
		if _, ok := a.Headers.Guards[h]; !ok {
			t.Errorf("guard header %s is not in the artifact", h)
		}
	}
	if a.Headers.RollupSubject != contract.RollupSubject {
		t.Errorf("rollup subject: artifact %q, package %q", a.Headers.RollupSubject, contract.RollupSubject)
	}
	for _, h := range []string{contract.HdrChunk, contract.HdrEnd} {
		if _, ok := a.Headers.Stream[h]; !ok {
			t.Errorf("stream header %s is not in the artifact", h)
		}
	}

	// The grammars.
	g := a.Grammars
	if g.Name.Pattern != contract.NamePattern || g.InstanceToken.Pattern != contract.InstanceTokenPattern {
		t.Errorf("patterns: artifact %q %q, package %q %q", g.Name.Pattern, g.InstanceToken.Pattern, contract.NamePattern, contract.InstanceTokenPattern)
	}
	if g.Path.Separator != contract.PathSeparator || g.Path.StoredSeparator != contract.TailSeparator {
		t.Errorf("path separators: artifact %q %q, package %q %q", g.Path.Separator, g.Path.StoredSeparator, contract.PathSeparator, contract.TailSeparator)
	}
	if !reflect.DeepEqual(g.ReservedStoreNames, contract.ReservedStoreNames) {
		t.Errorf("reserved store names: artifact %v, package %v", g.ReservedStoreNames, contract.ReservedStoreNames)
	}
	if g.ServicePrincipal != contract.ServicePrincipal || g.Root != contract.Root || g.MetaBucket != contract.MetaBucket ||
		g.SnapshotOpType != contract.OpTypeSnapshot || g.StateFoldKey != contract.StateFoldKey {
		t.Errorf("names: artifact %+v", g)
	}
	subs := map[string]string{"<STORE>": contract.UpperStore("my-store"), "<store>": "my-store", "<tail>": "invoice.inv-1", "<type>": "invoice", "<index>": "text", "<id>": "alice", "<digest>": "abc"}
	derived := map[string]string{
		"stream":        contract.StreamName("my-store"),
		"stateBucket":   contract.StateBucket("my-store"),
		"storeSubjects": contract.StoreSubjects("my-store"),
		"opsFilter":     contract.OpsFilter("my-store"),
		"opsSubject":    contract.OpsSubject("my-store", "invoice.inv-1"),
	}
	for name, want := range derived {
		if got := fill(g.Derivations[name], subs); got != want {
			t.Errorf("derivation %s: artifact %q, package %q", name, got, want)
		}
	}
	keys := map[string]string{
		"storeConfig": contract.MetaStoreConfig("my-store"),
		"type":        contract.MetaStoreType("my-store", "invoice"),
		"index":       contract.MetaIndex("my-store", "text"),
		"principal":   contract.MetaPrincipal("alice"),
		"member":      contract.MetaMember("alice"),
		"invite":      contract.MetaInvite("abc"),
	}
	for name, want := range keys {
		if got := fill(g.MetaKeys[name], subs); got != want {
			t.Errorf("meta key %s: artifact %q, package %q", name, got, want)
		}
	}
	if !reflect.DeepEqual(g.Roles, contract.Roles) {
		t.Errorf("roles: artifact %v, package %v", g.Roles, contract.Roles)
	}
	for _, e := range g.Effects {
		if !contract.KnownEffect(e) {
			t.Errorf("effect %q is not the package's", e)
		}
	}
	if !reflect.DeepEqual(g.History, []string{contract.HistoryCompactable, contract.HistoryFull}) {
		t.Errorf("history: artifact %v", g.History)
	}
	for _, k := range g.IndexKinds {
		// The state kind is the node's own (0023): a kind that exists but
		// is not declarable, so it is outside KnownIndexKind by design.
		if k != contract.IndexKindState && !contract.KnownIndexKind(k) {
			t.Errorf("index kind %q is not the package's", k)
		}
	}
	if !reflect.DeepEqual(g.PrincipalKinds, []string{contract.PrincipalKindMember, contract.PrincipalKindService}) {
		t.Errorf("principal kinds: artifact %v", g.PrincipalKinds)
	}

	// Stream settings and client defaults.
	if a.StreamSettings.DuplicateWindowSeconds != int(contract.DuplicateWindow.Seconds()) || a.StreamSettings.DefaultMaxBytes != contract.DefaultMaxBytes {
		t.Errorf("stream settings: artifact %+v", a.StreamSettings)
	}
	d := a.ClientDefaults
	if d.StreamStallSeconds != int(contract.StreamStall.Seconds()) || d.StreamPendingMsgs != contract.StreamPendingMsgs ||
		d.StreamPendingBytes != contract.StreamPendingBytes || d.ChunkByteBudget != contract.ChunkByteBudget ||
		d.DedupWindowSeconds != int(contract.DuplicateWindow.Seconds()) {
		t.Errorf("client defaults: artifact %+v", d)
	}

	// The fold's decisions, in the package's order.
	var decisions []string
	for dec := contract.Merge; dec <= contract.MalformedSnapshot; dec++ {
		decisions = append(decisions, dec.String())
	}
	if !reflect.DeepEqual(a.Fold.Decisions, decisions) {
		t.Errorf("fold decisions: artifact %v, package %v", a.Fold.Decisions, decisions)
	}

	// The error catalog, whole and in order.
	if !reflect.DeepEqual(a.Errors, contract.ErrorCatalog) {
		t.Errorf("error catalog differs:\nartifact %+v\npackage  %+v", a.Errors, contract.ErrorCatalog)
	}
}

// The interactions: subjects and shapes equal the client's, every error
// code is catalogued, and the Go types satisfy the published schemas.
func TestArtifactInteractionsMatchTheClient(t *testing.T) {
	a := loadArtifact(t)
	subjects := map[string]string{
		"ping":                        client.PingSubject,
		"store.create":                client.StoreCreateSubject,
		"type.define":                 client.TypeDefineSubject,
		"instance.snapshot":           client.InstanceSnapshotSubject,
		"index.declare":               client.IndexDeclareSubject,
		"index.delete":                client.IndexDeleteSubject,
		"member.add":                  client.MemberAddSubject,
		"member.revoke":               client.MemberRevokeSubject,
		"index.query.search":          client.IndexQuerySubject("<store>", "<index>"),
		"index.query.graph.neighbors": client.IndexQuerySubject("<store>", "<index>"),
		"index.query.graph.walk":      client.IndexQuerySubject("<store>", "<index>"),
		"index.query.semantic":        client.IndexQuerySubject("<store>", "<index>"),
		"append":                      contract.OpsSubject("<store>", "<tail>"),
	}
	shapes := map[string]string{
		"ping": contract.ShapeRequestReply, "store.create": contract.ShapeRequestReply, "type.define": contract.ShapeRequestReply,
		"instance.snapshot": contract.ShapeRequestReply, "index.declare": contract.ShapeRequestReply, "index.delete": contract.ShapeRequestReply,
		"member.add": contract.ShapeRequestReply, "member.revoke": contract.ShapeRequestReply,
		"index.query.search": contract.ShapeStreamedReply, "index.query.graph.neighbors": contract.ShapeStreamedReply,
		"index.query.graph.walk": contract.ShapeStreamedReply, "index.query.semantic": contract.ShapeStreamedReply,
		"append":  contract.ShapePublish,
		"history": contract.ShapeSubscribe, "tail": contract.ShapeSubscribe, "watch.state": contract.ShapeSubscribe, "watch.declarations": contract.ShapeSubscribe,
		"list.stores": contract.ShapeSubscribe, "list.types": contract.ShapeSubscribe, "list.indexes": contract.ShapeSubscribe, "list.members": contract.ShapeSubscribe, "list.instances": contract.ShapeSubscribe,
	}
	schema := json.RawMessage(`{"type":"object"}`)
	requests := map[string]any{
		"store.create":                client.StoreCreateRequest{Principal: "alice", Store: "orders", Description: "the orders", MaxBytes: 1 << 20, History: contract.HistoryFull},
		"type.define":                 client.TypeDefineRequest{Principal: "alice", Store: "orders", Type: "invoice", Schema: schema, History: contract.HistoryCompactable, Children: map[string]string{"comments": "comment"}, Operations: map[string]contract.OpDef{"send": {Schema: schema, Effect: contract.EffectMerge}}},
		"instance.snapshot":           client.InstanceSnapshotRequest{Principal: "alice", Store: "orders", Instance: "invoice/inv-1"},
		"index.declare":               client.IndexDeclareRequest{Principal: "alice", Store: "orders", Index: "text", Kind: contract.IndexKindSearch, Config: json.RawMessage(`{"source":"history"}`)},
		"index.delete":                client.IndexDeleteRequest{Principal: "alice", Store: "orders", Index: "text"},
		"member.add":                  client.MemberAddRequest{Principal: "alice", Member: "bob", Role: contract.RoleWriter, Kind: contract.PrincipalKindService, PublicKey: "UBOB", GithubID: 42},
		"member.revoke":               client.MemberRevokeRequest{Principal: "alice", Member: "bob"},
		"index.query.search":          client.IndexQueryRequest{Principal: "alice", Query: "widgets", Limit: 10},
		"index.query.graph.neighbors": client.GraphQueryRequest{Principal: "alice", Op: contract.GraphOpNeighbors, Instance: "invoice/inv-1", Direction: contract.GraphDirectionBoth, Label: "customer", Limit: 10},
		"index.query.graph.walk":      client.GraphQueryRequest{Principal: "alice", Op: contract.GraphOpWalk, Instance: "invoice/inv-1", Direction: contract.GraphDirectionOut, Labels: []string{"customer"}, Depth: 2, Limit: 10},
		"index.query.semantic":        client.SemanticQueryRequest{Principal: "alice", Text: "widgets", Limit: 10},
	}
	replies := map[string]any{
		"ping":              client.About{Name: "chronicle-node", Version: "v0/0/0"},
		"store.create":      client.StoreCreateResponse{Stream: "LOG_ORDERS"},
		"type.define":       client.TypeDefineResponse{Revision: 1},
		"instance.snapshot": client.InstanceSnapshotResponse{Taken: false, Reason: "nothing to compact"},
		"index.declare":     client.IndexDeclareResponse{Query: client.IndexQuerySubject("orders", "text")},
		"index.delete":      client.IndexDeleteResponse{Deleted: true},
		"member.add":        client.MemberAddResponse{Member: "bob", Role: contract.RoleWriter},
		"member.revoke":     client.MemberRevokeResponse{Member: "bob", PublicKey: "UBOB"},
	}
	items := map[string]any{
		"index.query.search":          client.IndexHit{Instance: "invoice/inv-1", Score: 0.5},
		"index.query.graph.neighbors": contract.GraphEdge{From: "invoice/inv-1", To: "customer/c-1", Label: "customer"},
		"index.query.graph.walk":      contract.GraphVisit{Instance: "customer/c-1", Depth: 1, Via: "customer"},
		"index.query.semantic":        client.SemanticHit{Instance: "invoice/inv-1", Score: 0.9, Field: "title"},
		"history":                     map[string]any{"seq": 1, "id": "op-1", "type": "snapshot", "author": "alice", "parents": []string{}, "ts": "2026-09-21T00:00:00Z", "payload": map[string]any{"state": map[string]any{}}},
		"tail":                        map[string]any{"seq": 2, "id": "op-2", "type": "send", "author": "alice", "parents": []string{"op-1"}, "payload": map[string]any{"to": "x"}},
		"watch.state":                 contract.StateValue{Seq: 2, State: json.RawMessage(`{"to":"x"}`)},
		"watch.declarations":          client.Declaration{Kind: "type", Name: "invoice", Revision: 3, Value: json.RawMessage(`{"revision":3,"schema":{}}`)},
		"list.stores":                 "orders",
		"list.types":                  "invoice",
		"list.indexes":                client.IndexInfo{Name: "text", Kind: contract.IndexKindSearch, Config: json.RawMessage(`{"source":"history"}`)},
		"list.members":                client.MemberInfo{Name: "bob", Role: contract.RoleWriter, Kind: contract.PrincipalKindMember, PublicKey: "UBOB", GithubID: 42},
		"list.instances":              client.InstanceInfo{Path: "invoice/inv-1", Type: "invoice", Seq: 3, State: json.RawMessage(`{"total":120}`)},
	}
	trailers := map[string]any{
		"index.query.search":          client.QueryTrailer{Total: 1},
		"index.query.graph.neighbors": client.GraphTrailer{Total: 1},
		"index.query.graph.walk":      client.GraphTrailer{Total: 1, DepthCapped: true, Truncated: true},
		"index.query.semantic":        client.SemanticTrailer{Total: 1, Unembedded: 0},
	}

	seen := map[string]bool{}
	for _, in := range a.Interactions {
		seen[in.Name] = true
		if want, ok := subjects[in.Name]; ok && in.Subject != want {
			t.Errorf("%s: subject artifact %q, client %q", in.Name, in.Subject, want)
		}
		if want, ok := shapes[in.Name]; !ok || in.Shape != want {
			t.Errorf("%s: shape %q, want %q", in.Name, in.Shape, want)
		}
		for _, code := range in.Errors {
			if !contract.KnownErrorCode(code) {
				t.Errorf("%s: error code %q is not catalogued", in.Name, code)
			}
		}
		if in.Name == "append" && !reflect.DeepEqual(in.Headers, []string{contract.HdrMsgID, contract.HdrOpType, contract.HdrOpAuthor, contract.HdrOpParents, contract.HdrOpTs, contract.HdrOpVersion}) {
			t.Errorf("append headers: %v", in.Headers)
		}
		check := func(what string, raw json.RawMessage, sample any, present bool) {
			if len(raw) == 0 {
				if present {
					t.Errorf("%s: the artifact has no %s schema", in.Name, what)
				}
				return
			}
			sch, err := contract.CompileSchema(raw)
			if err != nil {
				t.Errorf("%s: %s schema does not compile: %v", in.Name, what, err)
				return
			}
			if !present {
				return
			}
			b, _ := json.Marshal(sample)
			var v any
			_ = json.Unmarshal(b, &v)
			if err := sch.Validate(v); err != nil {
				t.Errorf("%s: the Go %s %s does not satisfy the artifact's schema: %v", in.Name, what, b, err)
			}
		}
		req, hasReq := requests[in.Name]
		check("request", in.Request, req, hasReq)
		rep, hasRep := replies[in.Name]
		check("reply", in.Reply, rep, hasRep)
		item, hasItem := items[in.Name]
		check("item", in.Item, item, hasItem)
		tr, hasTr := trailers[in.Name]
		check("trailer", in.Trailer, tr, hasTr)
	}
	for name := range shapes {
		if !seen[name] {
			t.Errorf("interaction %s is not in the artifact", name)
		}
	}
}

// No retired word survives in the artifact's own text: an SDK generated
// from it would restate it (decision 0044 § 4).
func TestArtifactSpeaksTheUsersLanguage(t *testing.T) {
	raw, err := os.ReadFile("../contract/sdk-contract.json")
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ToLower(string(raw))
	for _, word := range []string{"\"thing", "things", "aspect", "preserved", "\"ops\"", "birth", "bad-log-name", "no-such-log", "log-exists"} {
		if strings.Contains(text, word) {
			t.Errorf("the artifact still says %q", word)
		}
	}
}
