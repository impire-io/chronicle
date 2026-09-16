package control_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/control"
	"github.com/impire-io/chronicle/internal/natstest"
)

// TestFleetCredsIsRecordVerified: the record is the authorization. The
// assigned executor's pull answers with the tenant's service creds — here
// through the log-tail fold, the bucket being absent, which is exactly the
// assign-outruns-the-bucket case the design names — an unassigned
// executor is refused, and a stop revokes naturally.
func TestFleetCredsIsRecordVerified(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	url := natstest.StartJetStream(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	// The fleet log with a workload's custody tail: born, then assigned to
	// "right". No STATE_FLEET exists — the tail is the record.
	if _, err := js.CreateStream(ctx, contract.LogStreamConfig(contract.FleetLog, 0, "")); err != nil {
		t.Fatalf("create fleet stream: %v", err)
	}
	subject := contract.OpsSubject(contract.FleetLog, contract.FleetWorkloadThing("t1", contract.WorkloadNodeName))
	publish := func(opType, payload string, expected uint64) {
		t.Helper()
		op := contract.Op{ID: nats.NewInbox(), Type: opType, Author: "test"}
		msg := &nats.Msg{Subject: subject, Header: op.Header(), Data: []byte(payload)}
		if _, err := js.PublishMsg(ctx, msg, jetstream.WithExpectLastSequencePerSubject(expected)); err != nil {
			t.Fatalf("publish %s: %v", opType, err)
		}
	}
	publish(contract.OpTypeSnapshot, `{"state":{"kind":"node","tenant":"t1","replicas":1,"slots":{}}}`, 0)
	publish(contract.FleetOpAssign, `{"slots":{"0":{"executor":"right"}}}`, 1)

	accounts := t.TempDir()
	if err := os.MkdirAll(filepath.Join(accounts, "t1"), 0o700); err != nil {
		t.Fatal(err)
	}
	want := []byte("the tenant service creds")
	if err := os.WriteFile(filepath.Join(accounts, "t1", "service.creds"), want, 0o600); err != nil {
		t.Fatal(err)
	}

	svc, err := control.Start(nc, control.Config{URL: url, AccountsDir: accounts})
	if err != nil {
		t.Fatalf("start control: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop() })

	pull := func(executor string) ([]byte, string) {
		t.Helper()
		data, _ := json.Marshal(contract.FleetCredsRequest{Executor: executor, Tenant: "t1", Workload: contract.WorkloadNodeName})
		msg, err := nc.RequestWithContext(ctx, contract.FleetCredsSubject, data)
		if err != nil {
			t.Fatalf("creds request: %v", err)
		}
		if code := msg.Header.Get(micro.ErrorCodeHeader); code != "" {
			return nil, code
		}
		var resp contract.FleetCredsResponse
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			t.Fatalf("decode creds: %v", err)
		}
		return resp.Creds, ""
	}

	creds, code := pull("right")
	if code != "" || string(creds) != string(want) {
		t.Fatalf("assigned pull refused: code=%q creds=%q", code, creds)
	}
	if _, code := pull("wrong"); code != "not-assigned" {
		t.Fatalf("unassigned pull answered: code=%q", code)
	}

	// A stop revokes naturally: the record no longer verifies anyone.
	publish(contract.FleetOpStop, `{"stopped":true}`, 2)
	if _, code := pull("right"); code != "not-assigned" {
		t.Fatalf("stopped workload still armed: code=%q", code)
	}
}
