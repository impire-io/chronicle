package node

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/natstest"
)

// TestReplayDeclinesWhenARollupLandsBeforeItsFirstFetch is the interleaving
// behind tracker chronicle-49: a replay counts its pending messages, a
// peer's rollup then purges the subject before the first fetch, so the
// first message is that rollup and the messages still counted are gone.
// The replay must decline at its next stall — one wait — not spin out the
// whole budget and answer an error.
func TestReplayDeclinesWhenARollupLandsBeforeItsFirstFetch(t *testing.T) {
	nc, err := nats.Connect(natstest.StartJetStream(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	subject := contract.OpsSubject("orders", "invoice.inv-1")
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:        contract.StreamName("orders"),
		Subjects:    []string{contract.OpsSubject("orders", ">")},
		AllowRollup: true,
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := js.Publish(ctx, subject, []byte(`{}`)); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{FilterSubjects: []string{subject}})
	if err != nil {
		t.Fatalf("ordered consumer: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil || info.NumPending != 3 {
		t.Fatalf("pending = %v, %v; want 3", info, err)
	}

	// The peer's rollup lands: everything before it on the subject is gone.
	rollup := nats.NewMsg(subject)
	rollup.Header.Set(contract.HdrRollup, contract.RollupSubject)
	rollup.Data = []byte(`{}`)
	if _, err := js.PublishMsg(ctx, rollup); err != nil {
		t.Fatalf("publish rollup: %v", err)
	}

	first, lost, err := replayNext(ctx, cons, stream, subject, 0)
	if err != nil || lost || first.Headers().Get(contract.HdrRollup) == "" {
		t.Fatalf("first fetch = %v, lost %v, %v; want the peer's rollup", first, lost, err)
	}
	md, err := first.Metadata()
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}

	started := time.Now()
	_, lost, err = replayNext(ctx, cons, stream, subject, md.Sequence.Stream)
	if err != nil || !lost {
		t.Fatalf("the replay after a destroyed history: lost %v, %v; want a decline", lost, err)
	}
	if took := time.Since(started); took > replayBudget/2 {
		t.Fatalf("the decline took %s: it waited out the budget instead of one stall", took)
	}
}
