package contract_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/impire-io/chronicle/contract"
)

// The judgement of decision 0059: an operation's precondition against the
// thing's current state, and the thing schema against the state a merge
// produces.

func ticketRecord() *contract.TypeRecord {
	return &contract.TypeRecord{
		Revision: 1,
		Schema:   json.RawMessage(`{"type":"object","required":["status"],"properties":{"status":{"enum":["open","closed"]}}}`),
		Operations: map[string]contract.OpDef{
			"create": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge},
			"close":  {Schema: json.RawMessage(`{"type":"object","required":["status"]}`), Effect: contract.EffectMerge, Precondition: json.RawMessage(`{"properties":{"status":{"const":"open"}},"required":["status"]}`)},
			"drop":   {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge},
			"remark": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectNone, Precondition: json.RawMessage(`{"properties":{"status":{"const":"open"}},"required":["status"]}`)},
		},
	}
}

func TestJudgePrecondition(t *testing.T) {
	rec := ticketRecord()
	closeOp := rec.Operations["close"]
	if detail := contract.JudgePrecondition(closeOp, json.RawMessage(`{"status":"open"}`)); detail != "" {
		t.Fatalf("an open ticket satisfies close's precondition: %s", detail)
	}
	if detail := contract.JudgePrecondition(closeOp, json.RawMessage(`{"status":"closed"}`)); !strings.HasPrefix(detail, "the operation's precondition is not met") {
		t.Fatalf("a closed ticket must not satisfy close's precondition: %q", detail)
	}
	// No state yet is the empty object: a required status is missing.
	if detail := contract.JudgePrecondition(closeOp, nil); detail == "" {
		t.Fatal("no state must be judged as the empty object, which lacks status")
	}
	// No precondition declared: anything goes.
	if detail := contract.JudgePrecondition(rec.Operations["drop"], nil); detail != "" {
		t.Fatalf("an operation without a precondition applies in any state: %s", detail)
	}
	// A precondition that does not compile is said so.
	if detail := contract.JudgePrecondition(contract.OpDef{Precondition: json.RawMessage(`{"type":12}`)}, nil); !strings.Contains(detail, "does not compile") {
		t.Fatalf("a precondition that does not compile: %q", detail)
	}
}

func TestFoldStepJudgesPreconditionAndStateShape(t *testing.T) {
	rec := ticketRecord()
	res, err := contract.ResolveInstance("ticket.t-1", func(name string) (*contract.TypeRecord, bool, error) {
		if name == "ticket" {
			return rec, true, nil
		}
		return nil, false, nil
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	step := func(state json.RawMessage, opType, payload string) contract.FoldOutcome {
		return contract.FoldStep(res, state, contract.Op{ID: "op", Type: opType, Seq: 1, Payload: []byte(payload)})
	}

	// In turn: the create, then a close on an open ticket.
	out := step(nil, "create", `{"status":"open"}`)
	if out.Decision != contract.Merge || !out.Moved {
		t.Fatalf("create: %s %s", out.Decision, out.Detail)
	}
	open := out.State
	out = step(open, "close", `{"status":"closed"}`)
	if out.Decision != contract.Merge || string(out.State) != `{"status":"closed"}` {
		t.Fatalf("close on open: %s %s %s", out.Decision, out.Detail, out.State)
	}
	closed := out.State

	// Out of turn: marked, the state standing where it was.
	out = step(closed, "close", `{"status":"closed"}`)
	if out.Decision != contract.PreconditionUnmet || out.Moved || string(out.State) != string(closed) {
		t.Fatalf("close on closed: %s %s moved=%v", out.Decision, out.Detail, out.Moved)
	}
	if out.Decision.String() != "precondition-unmet" {
		t.Fatalf("decision name: %s", out.Decision)
	}
	// An operation with effect none is judged too.
	out = step(closed, "remark", `{"text":"late"}`)
	if out.Decision != contract.PreconditionUnmet {
		t.Fatalf("remark on closed: %s %s", out.Decision, out.Detail)
	}
	if out = step(open, "remark", `{"text":"in time"}`); out.Decision != contract.None {
		t.Fatalf("remark on open: %s %s", out.Decision, out.Detail)
	}

	// Off the schema: a merge that drops the required status is marked.
	out = step(open, "drop", `{"status":null}`)
	if out.Decision != contract.OffSchema || out.Moved || string(out.State) != string(open) {
		t.Fatalf("drop status: %s %s moved=%v", out.Decision, out.Detail, out.Moved)
	}
	if out.Decision.String() != "off-schema" || !strings.Contains(out.Detail, "fails the type's schema") {
		t.Fatalf("off-schema detail: %s %q", out.Decision, out.Detail)
	}
	// A merge that keeps the shape applies.
	if out = step(open, "drop", `{"note":"kept"}`); out.Decision != contract.Merge {
		t.Fatalf("drop within the shape: %s %s", out.Decision, out.Detail)
	}
}
