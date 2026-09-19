package workloads_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/natstest"
	"github.com/impire-io/chronicle/internal/workloads"
)

// TestBridgeStampsTenantsAndRefusesMembers proves the tenant-stamped
// bridge on real account machinery: a minted tenant's service user
// reports over the local subject, the import chronicle signed maps it to
// the stamped form, and the workload service lands the record for that
// tenant — while a member-baseline user cannot reach the bridge at all,
// because CHRONX.> sits outside the one root the baseline grants.
func TestBridgeStampsTenantsAndRefusesMembers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	url, b := natstest.StartOperator(t)
	sysConn, err := mint.ConnectCreds(url, b.SysCreds, "test-sys")
	if err != nil {
		t.Fatalf("connect sys: %v", err)
	}
	t.Cleanup(sysConn.Close)
	ctrlConn, err := mint.ConnectCreds(url, b.ControlCreds, "test-control")
	if err != nil {
		t.Fatalf("connect control: %v", err)
	}
	t.Cleanup(ctrlConn.Close)

	svc, err := workloads.Start(ctx, ctrlConn, workloads.Config{
		ScanEvery:     150 * time.Millisecond,
		AuctionWindow: 80 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start workloads: %v", err)
	}
	t.Cleanup(svc.Stop)

	driver := b.Driver(sysConn, url)
	acct, err := driver.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint account: %v", err)
	}

	// The service user reports a declaration over the bridge's local
	// subject; the stamp is the server's, not the caller's.
	svcCreds, err := mint.IssueServiceUser(acct, "chronicle-node")
	if err != nil {
		t.Fatalf("issue service user: %v", err)
	}
	tenantConn, err := mint.ConnectCreds(url, svcCreds.File, "test-tenant-svc")
	if err != nil {
		t.Fatalf("connect tenant service user: %v", err)
	}
	t.Cleanup(tenantConn.Close)

	report := func(action string) contract.FleetIndexReportAck {
		t.Helper()
		data, _ := json.Marshal(contract.FleetIndexReport{Action: action, Log: "orders", Index: "text", Kind: contract.IndexKindSearch})
		msg, err := tenantConn.RequestWithContext(ctx, contract.FleetBridgeLocalSubject, data)
		if err != nil {
			t.Fatalf("report %s over the bridge: %v", action, err)
		}
		if code := msg.Header.Get(micro.ErrorCodeHeader); code != "" {
			t.Fatalf("report %s refused: %s %s", action, code, msg.Header.Get(micro.ErrorHeader))
		}
		var ack contract.FleetIndexReportAck
		if err := json.Unmarshal(msg.Data, &ack); err != nil {
			t.Fatalf("decode ack: %v", err)
		}
		return ack
	}

	if ack := report(contract.IndexReportDeclared); !ack.Recorded {
		t.Fatal("declared report not recorded")
	}
	workload := contract.WorkloadIndexName("orders", "text")
	waitFor(t, "the stamped dispatch on the record", func() bool {
		ws, ok := workloadFromBucket(ctx, t, ctrlConn, "acme", workload)
		return ok && ws.Kind == contract.WorkloadKindIndexSearch && ws.Tenant == "acme" && !ws.Stopped
	})

	if ack := report(contract.IndexReportDeleted); !ack.Recorded {
		t.Fatal("deleted report not recorded")
	}
	waitFor(t, "the retirement on the record", func() bool {
		ws, ok := workloadFromBucket(ctx, t, ctrlConn, "acme", workload)
		return ok && ws.Stopped
	})

	// A member cannot publish CHRONX.>: the baseline grants CHRON.> and
	// nothing else — the request finds no path, not merely no responder
	// with an excuse.
	memberCreds, err := mint.IssueMember(acct, "dana")
	if err != nil {
		t.Fatalf("issue member: %v", err)
	}
	memberConn, err := mint.ConnectCreds(url, memberCreds.File, "test-member")
	if err != nil {
		t.Fatalf("connect member: %v", err)
	}
	t.Cleanup(memberConn.Close)
	shortCtx, shortCancel := context.WithTimeout(ctx, 2*time.Second)
	defer shortCancel()
	data, _ := json.Marshal(contract.FleetIndexReport{Action: contract.IndexReportDeclared, Log: "orders", Index: "forged", Kind: contract.IndexKindSearch})
	if _, err := memberConn.RequestWithContext(shortCtx, contract.FleetBridgeLocalSubject, data); err == nil {
		t.Fatal("a member reached the bridge; the permission boundary is broken")
	}
	if _, ok := workloadFromBucket(ctx, t, ctrlConn, "acme", contract.WorkloadIndexName("orders", "forged")); ok {
		t.Fatal("a member's forged report landed on the record")
	}
}
