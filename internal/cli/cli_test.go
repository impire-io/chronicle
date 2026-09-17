package cli_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/impire-io/chronicle/internal/cli"
	"github.com/impire-io/chronicle/internal/fleet"
	"github.com/impire-io/chronicle/internal/version"
)

func run(ctx context.Context, t *testing.T, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	if err := cli.Run(ctx, args, &out); err != nil {
		t.Fatalf("chronicle %s: %v\n%s", strings.Join(args, " "), err, out.String())
	}
	return out.String()
}

func TestCLISpine(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	f, err := fleet.Up(ctx, fleet.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	defer f.Stop()

	creds := filepath.Join(t.TempDir(), "dana.creds")
	out := run(ctx, t, "tenant", "create", "acme", "--dir", dir, "--admin", "dana", "--out", creds)
	if !strings.Contains(out, "tenant acme minted") {
		t.Fatalf("tenant create output: %s", out)
	}

	run(ctx, t, "log", "create", "orders", "--dir", dir, "--creds", creds)
	run(ctx, t, "type", "define", "orders", "invoice", "--dir", dir, "--creds", creds,
		"--def", `{"schema":{"type":"object"},"operations":{"comment.add":{"schema":{"type":"object","required":["body"]}}}}`)
	out = run(ctx, t, "type", "list", "orders", "--dir", dir, "--creds", creds)
	if strings.TrimSpace(out) != "invoice" {
		t.Fatalf("type list output: %s", out)
	}
	out = run(ctx, t, "type", "inspect", "orders", "invoice", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "comment.add") {
		t.Fatalf("type inspect output: %s", out)
	}
	out = run(ctx, t, "thing", "create", "orders", "invoice.invoice-1", "--dir", dir, "--creds", creds,
		"--state", `{"total":3}`)
	if !strings.Contains(out, "born: invoice.invoice-1") {
		t.Fatalf("thing create output: %s", out)
	}
	out = run(ctx, t, "append", "orders", "invoice.invoice-1", "comment.add", "--dir", dir, "--creds", creds,
		"--payload", `{"body":"hi"}`)
	var head uint64
	if _, err := fmt.Sscanf(out, "appended: seq %d", &head); err != nil {
		t.Fatalf("append output: %s", out)
	}

	// Pre-flight refusal happens before the wire — the CLI surfaces it.
	var errOut bytes.Buffer
	if err := cli.Run(ctx, []string{"append", "orders", "invoice.invoice-1", "comment.add",
		"--dir", dir, "--creds", creds, "--payload", `{"nobody":1}`}, &errOut); err == nil {
		t.Fatal("invalid payload appended")
	}

	// The expected-sequence guard reaches the wire (0018): guarded at the
	// observed head the append lands; the same guard re-used is a typed
	// refusal — the thing moved.
	run(ctx, t, "append", "orders", "invoice.invoice-1", "comment.add", "--dir", dir, "--creds", creds,
		"--payload", `{"body":"guarded"}`, "--expect-seq", strconv.FormatUint(head, 10))
	var stale bytes.Buffer
	if err := cli.Run(ctx, []string{"append", "orders", "invoice.invoice-1", "comment.add",
		"--dir", dir, "--creds", creds, "--payload", `{"body":"late"}`,
		"--expect-seq", strconv.FormatUint(head, 10)}, &stale); err == nil || !strings.Contains(err.Error(), "moved past the guard") {
		t.Fatalf("stale guard not refused: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		var stateOut bytes.Buffer
		err := cli.Run(ctx, []string{"state", "orders", "invoice.invoice-1", "--dir", dir, "--creds", creds}, &stateOut)
		if err == nil && strings.Contains(stateOut.String(), `{"total":3}`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("state never appeared: %v %s", err, stateOut.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The effect reaches the wire: re-defining the type (revision 2) with
	// a merge operation moves state.
	run(ctx, t, "type", "define", "orders", "invoice", "--dir", dir, "--creds", creds,
		"--def", `{"schema":{"type":"object"},"operations":{"comment.add":{"schema":{"type":"object","required":["body"]}},"status.set":{"schema":{"type":"object"},"effect":"merge"}}}`)
	run(ctx, t, "append", "orders", "invoice.invoice-1", "status.set", "--dir", dir, "--creds", creds,
		"--payload", `{"status":"closed"}`)
	deadline = time.Now().Add(10 * time.Second)
	for {
		var stateOut bytes.Buffer
		err := cli.Run(ctx, []string{"state", "orders", "invoice.invoice-1", "--dir", dir, "--creds", creds}, &stateOut)
		if err == nil && strings.Contains(stateOut.String(), `"status":"closed"`) &&
			strings.Contains(stateOut.String(), `"total":3`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("merged state never appeared: %v %s", err, stateOut.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	out = run(ctx, t, "replay", "orders", "invoice.invoice-1", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "snapshot") || !strings.Contains(out, "comment.add") || !strings.Contains(out, "by dana") {
		t.Fatalf("replay output: %s", out)
	}

	// A typed compactable thing compacts past its effect-none comment —
	// the declaration replaced the per-op veto (0022 § 5) — while an
	// untyped thing keeps 0011 § 4's protection, reason on stdout.
	out = run(ctx, t, "thing", "rollup", "orders", "invoice.invoice-1", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "compacted: invoice.invoice-1") {
		t.Fatalf("typed rollup output: %s", out)
	}
	run(ctx, t, "thing", "create", "orders", "freeform-1", "--dir", dir, "--creds", creds,
		"--state", `{}`)
	run(ctx, t, "append", "orders", "freeform-1", "note.add", "--dir", dir, "--creds", creds,
		"--payload", `{"body":"kept"}`)
	out = run(ctx, t, "thing", "rollup", "orders", "freeform-1", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "not compacted") || !strings.Contains(out, "untyped") {
		t.Fatalf("untyped rollup veto output: %s", out)
	}

	// A fully merge-covered thing compacts to one snapshot.
	run(ctx, t, "thing", "create", "orders", "invoice.invoice-2", "--dir", dir, "--creds", creds,
		"--state", `{"total":1}`)
	run(ctx, t, "append", "orders", "invoice.invoice-2", "status.set", "--dir", dir, "--creds", creds,
		"--payload", `{"status":"paid"}`)
	out = run(ctx, t, "thing", "rollup", "orders", "invoice.invoice-2", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "compacted: invoice.invoice-2") {
		t.Fatalf("thing rollup output: %s", out)
	}
	out = run(ctx, t, "replay", "orders", "invoice.invoice-2", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "by chronicle-node") || strings.Contains(out, "status.set") {
		t.Fatalf("replay after rollup: %s", out)
	}

	// A preserved log (0019): declared at creation, the node declines its
	// rollup with the declaration named.
	run(ctx, t, "log", "create", "audit", "--dir", dir, "--creds", creds, "--history", "preserved")
	run(ctx, t, "thing", "create", "audit", "case-1", "--dir", dir, "--creds", creds,
		"--state", `{}`)
	run(ctx, t, "append", "audit", "case-1", "status.set", "--dir", dir, "--creds", creds,
		"--payload", `{"status":"open"}`)
	out = run(ctx, t, "thing", "rollup", "audit", "case-1", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "not compacted") || !strings.Contains(out, "preserved") {
		t.Fatalf("preserved rollup output: %s", out)
	}

	// Bad flags fail before dialing anything.
	var bad bytes.Buffer
	if err := cli.Run(ctx, []string{"log", "create", "orders"}, &bad); err == nil {
		t.Fatal("log create without --creds must fail")
	}
}

// TestCLIIndex drives the index verbs over a real fleet: declare prints
// the query subject, query prints hits once the supervisor's indexer is
// caught up, delete retires it.
func TestCLIIndex(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	f, err := fleet.Up(ctx, fleet.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	defer f.Stop()

	creds := filepath.Join(t.TempDir(), "dana.creds")
	run(ctx, t, "tenant", "create", "acme", "--dir", dir, "--admin", "dana", "--out", creds)
	run(ctx, t, "log", "create", "orders", "--dir", dir, "--creds", creds)
	run(ctx, t, "thing", "create", "orders", "invoice.invoice-1", "--dir", dir, "--creds", creds,
		"--state", `{"title":"quantum widgets"}`)

	out := run(ctx, t, "index", "declare", "orders", "text", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "CHRON.API.INDEX.QUERY.orders.text") {
		t.Fatalf("index declare output: %s", out)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		var qout bytes.Buffer
		err := cli.Run(ctx, []string{"index", "query", "orders", "text", "widgets",
			"--dir", dir, "--creds", creds}, &qout)
		if err == nil && strings.Contains(qout.String(), "invoice-1") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("index query never hit: %v %s", err, qout.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	out = run(ctx, t, "index", "delete", "orders", "text", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "retired") {
		t.Fatalf("index delete output: %s", out)
	}
}

// TestCLIVersion: the version verb prints the stamped version — release
// consumers and the brew formula's install test assert on it.
func TestCLIVersion(t *testing.T) {
	var out bytes.Buffer
	if err := cli.Run(context.Background(), []string{"version"}, &out); err != nil {
		t.Fatalf("version: %v", err)
	}
	if want := version.Version + "\n"; out.String() != want {
		t.Errorf("version output = %q, want %q", out.String(), want)
	}
}

// TestCLIMemberVerbs drives the membership lifecycle the way an operator
// would: add a member, revoke the leak, rekey the tenant, and end up
// holding only credentials that work.
func TestCLIMemberVerbs(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	f, err := fleet.Up(ctx, fleet.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	defer f.Stop()

	danaCreds := filepath.Join(t.TempDir(), "dana.creds")
	run(ctx, t, "tenant", "create", "acme", "--dir", dir, "--admin", "dana", "--out", danaCreds)
	run(ctx, t, "log", "create", "orders", "--dir", dir, "--creds", danaCreds)

	erinCreds := filepath.Join(t.TempDir(), "erin.creds")
	out := run(ctx, t, "member", "add", "acme", "erin", "--dir", dir, "--out", erinCreds)
	if !strings.Contains(out, "member erin added to acme: role writer") || !strings.Contains(out, "only copy") {
		t.Fatalf("member add output: %s", out)
	}
	run(ctx, t, "thing", "create", "orders", "ticket-1", "--dir", dir, "--creds", erinCreds, "--state", `{}`)

	out = run(ctx, t, "member", "revoke", "acme", "erin", "--dir", dir)
	if !strings.Contains(out, "member erin revoked from acme") {
		t.Fatalf("member revoke output: %s", out)
	}
	var dead bytes.Buffer
	if err := cli.Run(ctx, []string{"replay", "orders", "ticket-1", "--dir", dir, "--creds", erinCreds}, &dead); err == nil {
		t.Fatal("revoked creds still speak")
	}

	outDir := t.TempDir()
	out = run(ctx, t, "member", "rekey", "acme", "--dir", dir, "--out-dir", outDir)
	if !strings.Contains(out, "tenant acme rekeyed") || !strings.Contains(out, "member dana (admin) re-issued") {
		t.Fatalf("member rekey output: %s", out)
	}
	var stale bytes.Buffer
	if err := cli.Run(ctx, []string{"log", "create", "late", "--dir", dir, "--creds", danaCreds}, &stale); err == nil {
		t.Fatal("pre-rekey creds still speak")
	}
	newDana := filepath.Join(outDir, "acme-dana.creds")
	run(ctx, t, "log", "create", "after-rekey", "--dir", dir, "--creds", newDana)
}
