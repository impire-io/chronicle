package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// Decision 0059: an operation may declare a precondition on the instance's
// state, and the type's schema is the shape every derived state keeps.
// The SDK refuses before sending, the node refuses at the door of a
// verified write, and the fold marks what a bare publish sneaks past.
func TestPreconditionRefusedAndMarked(t *testing.T) {
	catcher := &logCatcher{}
	nc, alice := startNode(t, catcher)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := alice.CreateStore(ctx, "tickets", ""); err != nil {
		t.Fatalf("create store: %v", err)
	}
	preOpen := json.RawMessage(`{"properties":{"status":{"const":"open"}},"required":["status"]}`)
	defineType(ctx, t, alice, "tickets", "ticket", client.TypeDefinition{
		Schema: json.RawMessage(`{"type":"object","required":["status"],"properties":{"status":{"enum":["open","closed"]}}}`),
		Operations: map[string]contract.OpDef{
			"create": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge},
			"close":  {Schema: json.RawMessage(`{"type":"object","required":["status"]}`), Effect: contract.EffectMerge, Precondition: preOpen},
			"drop":   {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge},
		},
	})

	ack, err := alice.Create(ctx, "tickets", "ticket/t-1", "create", []byte(`{"status":"open"}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	waitStateAt(ctx, t, alice, "tickets", "ticket/t-1", ack.Seq)

	// In turn: the precondition holds and the close applies.
	ack, err = alice.Apply(ctx, "tickets", "ticket/t-1", "close", []byte(`{"status":"closed"}`))
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	sv := waitStateAt(ctx, t, alice, "tickets", "ticket/t-1", ack.Seq)
	if string(sv.State) != `{"status":"closed"}` {
		t.Fatalf("state after close: %s", sv.State)
	}

	// Out of turn: the SDK's pre-flight refuses, nothing is sent.
	if _, err := alice.Apply(ctx, "tickets", "ticket/t-1", "close", []byte(`{"status":"closed"}`)); !errors.Is(err, client.ErrPreconditionFailed) {
		t.Fatalf("a second close must be refused pre-flight with ErrPreconditionFailed: %v", err)
	}
	// Through the node: the same refusal, catalogued.
	if _, err := alice.Apply(ctx, "tickets", "ticket/t-1", "close", []byte(`{"status":"closed"}`), client.Verified()); !errors.Is(err, client.ErrPreconditionFailed) {
		t.Fatalf("the node must refuse a second close with precondition-failed: %v", err)
	}
	// Off the schema: a merge that drops the required status.
	if _, err := alice.Apply(ctx, "tickets", "ticket/t-1", "drop", []byte(`{"status":null}`)); !errors.Is(err, client.ErrStateOffSchema) {
		t.Fatalf("a merge leaving the state off schema must be refused with ErrStateOffSchema: %v", err)
	}
	if _, err := alice.Apply(ctx, "tickets", "ticket/t-1", "drop", []byte(`{"status":null}`), client.Verified()); !errors.Is(err, client.ErrStateOffSchema) {
		t.Fatalf("the node must refuse the same with state-off-schema: %v", err)
	}
	stateStaysAt(ctx, t, alice, "tickets", "ticket/t-1", ack.Seq)

	// A bare publish skips every pre-flight: the history keeps it and the
	// fold marks it, the state standing where it was.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	publish := func(opType, payload string) {
		t.Helper()
		op := contract.Op{ID: nuid.Next(), Type: opType, Author: alice.Author(), Ts: time.Now(), Payload: []byte(payload)}
		msg := nats.NewMsg(contract.OpsSubject("tickets", "ticket.t-1"))
		msg.Header = op.Header()
		msg.Data = op.Payload
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			t.Fatalf("bare publish of %s: %v", opType, err)
		}
	}
	publish("close", `{"status":"closed"}`)
	catcher.wait(t, "marked precondition not met")
	publish("drop", `{"status":null}`)
	catcher.wait(t, "marked state off schema")
	stateStaysAt(ctx, t, alice, "tickets", "ticket/t-1", ack.Seq)

	// A precondition that does not compile is refused at DEFINE as a bad
	// schema, before anything is declared.
	_, err = alice.DefineType(ctx, "tickets", "broken", client.TypeDefinition{
		Schema: json.RawMessage(`{"type":"object"}`),
		Operations: map[string]contract.OpDef{
			"x": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge, Precondition: json.RawMessage(`{"type":12}`)},
		},
	})
	var serr *client.ServiceError
	if !errors.As(err, &serr) || serr.Code != contract.CodeBadSchema {
		t.Fatalf("a precondition that does not compile must be refused with bad-schema: %v", err)
	}
}
