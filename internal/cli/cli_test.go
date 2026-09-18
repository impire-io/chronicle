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

func runErr(ctx context.Context, args ...string) error {
	var out bytes.Buffer
	return cli.Run(ctx, args, &out)
}

// TestCLISpine runs the design's day-in-the-life (07-the-cli.md) over a
// real fleet: mint ends connected, log create selects, the sentences
// speak through the context, and the refusals teach.
func TestCLISpine(t *testing.T) {
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	f, err := fleet.Up(ctx, fleet.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	defer f.Stop()

	// Before anything is saved, a sentence teaches the context fix.
	if err := runErr(ctx, "get", "invoice.invoice-1"); err == nil || !strings.Contains(err.Error(), "no credentials") {
		t.Fatalf("creds teaching error missing: %v", err)
	}

	// Onboarding ends connected: the mint saves and selects a context.
	creds := filepath.Join(t.TempDir(), "dana.creds")
	out := run(ctx, t, "tenant", "create", "acme", "--dir", dir, "--admin", "dana", "--out", creds)
	if !strings.Contains(out, "tenant acme minted") || !strings.Contains(out, "context acme-dana saved and selected") {
		t.Fatalf("tenant create output: %s", out)
	}

	// Connected but nothing selected: the log teaching error.
	if err := runErr(ctx, "get", "invoice.invoice-1"); err == nil || !strings.Contains(err.Error(), "no log selected") {
		t.Fatalf("log teaching error missing: %v", err)
	}

	// log create selects the log it made.
	out = run(ctx, t, "log", "create", "orders")
	if !strings.Contains(out, "log orders created") || !strings.Contains(out, "selected as the working log") {
		t.Fatalf("log create output: %s", out)
	}
	out = run(ctx, t, "context", "show")
	if !strings.Contains(out, "context: acme-dana") || !strings.Contains(out, "log:     orders") {
		t.Fatalf("context show output: %s", out)
	}

	// One act defines the type; the define echoes its facets back.
	out = run(ctx, t, "type", "define", "invoice",
		"--def", `{"schema":{"type":"object"},"operations":{"create":{"schema":{"type":"object"},"effect":"merge"},"comment.add":{"schema":{"type":"object","required":["body"]}}}}`)
	if !strings.Contains(out, "type invoice in orders: revision 1") ||
		!strings.Contains(out, "operations: comment.add (none), create (merge)") {
		t.Fatalf("type define output: %s", out)
	}
	out = run(ctx, t, "type", "list")
	if strings.TrimSpace(out) != "invoice" {
		t.Fatalf("type list output: %s", out)
	}
	out = run(ctx, t, "type", "inspect", "invoice")
	if !strings.Contains(out, "type invoice — revision 1 · history compactable") ||
		!strings.Contains(out, "comment.add") {
		t.Fatalf("type inspect output: %s", out)
	}
	out = run(ctx, t, "type", "inspect", "invoice", "--json")
	if !strings.Contains(out, `"revision": 1`) {
		t.Fatalf("type inspect --json output: %s", out)
	}

	// Create is an operation: birth through the type's constructor.
	out = run(ctx, t, "create", "invoice.invoice-1", "--payload", `{"total":3}`)
	if !strings.Contains(out, "born: invoice.invoice-1") || !strings.Contains(out, "via create") {
		t.Fatalf("create output: %s", out)
	}

	// The sentence verb invokes an operation; the payload pre-flights.
	out = run(ctx, t, "do", "invoice.invoice-1", "comment.add", "--payload", `{"body":"hi"}`)
	var head uint64
	if _, err := fmt.Sscanf(out, "done: seq %d", &head); err != nil {
		t.Fatalf("do output: %s", out)
	}
	if err := runErr(ctx, "do", "invoice.invoice-1", "comment.add", "--payload", `{"nobody":1}`); err == nil {
		t.Fatal("invalid payload appended")
	}

	// A refused operation teaches what the type defines.
	if err := runErr(ctx, "do", "invoice.invoice-1", "close"); err == nil ||
		!strings.Contains(err.Error(), "operations on invoice: comment.add, create") {
		t.Fatalf("do teaching error missing: %v", err)
	}

	// The expected-sequence guard reaches the wire (0018): guarded at the
	// observed head the op lands; the same guard re-used is a typed
	// refusal — the thing moved.
	run(ctx, t, "do", "invoice.invoice-1", "comment.add",
		"--payload", `{"body":"guarded"}`, "--expect-seq", strconv.FormatUint(head, 10))
	if err := runErr(ctx, "do", "invoice.invoice-1", "comment.add",
		"--payload", `{"body":"late"}`, "--expect-seq", strconv.FormatUint(head, 10)); err == nil ||
		!strings.Contains(err.Error(), "moved past the guard") {
		t.Fatalf("stale guard not refused: %v", err)
	}

	// The constructor's merge fed the fold: get reads it back.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var stateOut bytes.Buffer
		err := cli.Run(ctx, []string{"get", "invoice.invoice-1"}, &stateOut)
		if err == nil && strings.Contains(stateOut.String(), `"total":3`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("state never appeared: %v %s", err, stateOut.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Operations are managed where they live: define narrates, list shows,
	// the new operation moves state.
	out = run(ctx, t, "op", "define", "invoice", "status.set", "--schema", `{"type":"object"}`, "--effect", "merge")
	if !strings.Contains(out, "operation status.set on invoice: effect merge, revision 2") {
		t.Fatalf("op define output: %s", out)
	}
	out = run(ctx, t, "operation", "list", "invoice")
	if !strings.Contains(out, "status.set") || !strings.Contains(out, "merge") {
		t.Fatalf("operation list output: %s", out)
	}
	run(ctx, t, "do", "invoice.invoice-1", "status.set", "--payload", `{"status":"closed"}`)
	deadline = time.Now().Add(10 * time.Second)
	for {
		var stateOut bytes.Buffer
		err := cli.Run(ctx, []string{"get", "invoice.invoice-1"}, &stateOut)
		if err == nil && strings.Contains(stateOut.String(), `"status":"closed"`) &&
			strings.Contains(stateOut.String(), `"total":3`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("merged state never appeared: %v %s", err, stateOut.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	// An effect change is narrated before the write.
	out = run(ctx, t, "op", "define", "invoice", "status.set", "--schema", `{"type":"object"}`)
	if !strings.Contains(out, "effect changes merge → none: derived state is suspect") {
		t.Fatalf("effect change narration missing: %s", out)
	}
	// rm narrates the re-fold and removes.
	out = run(ctx, t, "op", "rm", "invoice", "status.set")
	if !strings.Contains(out, "re-fold as effect none with a warning") ||
		!strings.Contains(out, "operation status.set removed from invoice") {
		t.Fatalf("op rm output: %s", out)
	}

	// History speaks the operations, create included.
	out = run(ctx, t, "history", "invoice.invoice-1")
	if !strings.Contains(out, "create") || !strings.Contains(out, "comment.add") || !strings.Contains(out, "by dana") {
		t.Fatalf("history output: %s", out)
	}

	// A typed compactable thing compacts past its effect-none comment —
	// the declaration replaced the per-op veto (0022 § 5) — while an
	// untyped thing keeps 0011 § 4's protection, reason on stdout.
	out = run(ctx, t, "rollup", "invoice.invoice-1")
	if !strings.Contains(out, "compacted: invoice.invoice-1") {
		t.Fatalf("typed rollup output: %s", out)
	}
	out = run(ctx, t, "create", "freeform-1", "--payload", `{}`)
	if !strings.Contains(out, "born: freeform-1") || strings.Contains(out, "via") {
		t.Fatalf("untyped create output: %s", out)
	}
	run(ctx, t, "do", "freeform-1", "note.add", "--payload", `{"body":"kept"}`)
	out = run(ctx, t, "rollup", "freeform-1")
	if !strings.Contains(out, "not compacted") || !strings.Contains(out, "untyped") {
		t.Fatalf("untyped rollup veto output: %s", out)
	}

	// A typed thing whose type declares no constructor is refused with
	// the operations listed.
	run(ctx, t, "type", "define", "receipt", "--def", `{"schema":{"type":"object"},"operations":{"scan":{"schema":{"type":"object"}}}}`)
	if err := runErr(ctx, "create", "receipt.r-1"); err == nil ||
		!strings.Contains(err.Error(), "operations on receipt: scan") {
		t.Fatalf("constructor teaching error missing: %v", err)
	}

	// A preserved log (0019): created and selected, its rollup declines
	// with the declaration named; the selection moved with log create.
	run(ctx, t, "log", "create", "audit", "--history", "preserved")
	run(ctx, t, "create", "case-1", "--payload", `{}`)
	run(ctx, t, "do", "case-1", "status.set", "--payload", `{"status":"open"}`)
	out = run(ctx, t, "rollup", "case-1")
	if !strings.Contains(out, "not compacted") || !strings.Contains(out, "preserved") {
		t.Fatalf("preserved rollup output: %s", out)
	}

	// --log overrides the selection without moving it.
	out = run(ctx, t, "get", "invoice.invoice-1", "--log", "orders")
	if !strings.Contains(out, "seq ") {
		t.Fatalf("--log override output: %s", out)
	}
	out = run(ctx, t, "context", "show")
	if !strings.Contains(out, "log:     audit") {
		t.Fatalf("selection moved by --log: %s", out)
	}

	// log select moves the selection back and refuses the unknown.
	if err := runErr(ctx, "log", "select", "nowhere"); err == nil ||
		!strings.Contains(err.Error(), "logs: audit, orders") {
		t.Fatalf("log select teaching error missing: %v", err)
	}
	out = run(ctx, t, "log", "select", "orders")
	if !strings.Contains(out, "working log: orders") {
		t.Fatalf("log select output: %s", out)
	}
	out = run(ctx, t, "log", "list")
	if !strings.Contains(out, "audit") || !strings.Contains(out, "orders (selected)") {
		t.Fatalf("log list output: %s", out)
	}

	// Discovery: the things and their pair grammar, from the state keys.
	out = run(ctx, t, "things")
	if !strings.Contains(out, "invoice.invoice-1") || !strings.Contains(out, "freeform-1") {
		t.Fatalf("things output: %s", out)
	}
	out = run(ctx, t, "things", "invoice")
	if !strings.Contains(out, "invoice.invoice-1") || strings.Contains(out, "freeform-1") {
		t.Fatalf("things prefix output: %s", out)
	}
}

// TestCLIContextStore drives the context verbs alone — the store needs
// no fleet.
func TestCLIContextStore(t *testing.T) {
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	ctx := context.Background()

	credsA := filepath.Join(t.TempDir(), "a.creds")

	out := run(ctx, t, "context", "save", "alpha", "--creds", credsA, "--url", "nats://localhost:4222")
	if !strings.Contains(out, "context alpha saved") || !strings.Contains(out, "select it") {
		t.Fatalf("context save output: %s", out)
	}
	out = run(ctx, t, "context", "select", "alpha")
	if !strings.Contains(out, "context alpha selected") {
		t.Fatalf("context select output: %s", out)
	}
	run(ctx, t, "context", "save", "beta", "--creds", credsA)
	out = run(ctx, t, "context", "list")
	if !strings.Contains(out, "* alpha") || !strings.Contains(out, "  beta") {
		t.Fatalf("context list output: %s", out)
	}
	out = run(ctx, t, "context", "show")
	if !strings.Contains(out, "context: alpha") || !strings.Contains(out, "nats://localhost:4222") ||
		!strings.Contains(out, "none selected — chronicle log select") {
		t.Fatalf("context show output: %s", out)
	}
	if err := runErr(ctx, "context", "select", "gamma"); err == nil ||
		!strings.Contains(err.Error(), "not saved") {
		t.Fatalf("selecting a missing context: %v", err)
	}
	out = run(ctx, t, "context", "rm", "alpha")
	if !strings.Contains(out, "context alpha removed") {
		t.Fatalf("context rm output: %s", out)
	}
	// Removing the selected context clears the selection.
	if err := runErr(ctx, "context", "show"); err == nil ||
		!strings.Contains(err.Error(), "no context selected") {
		t.Fatalf("dangling selection survived rm: %v", err)
	}
}

// TestCLIQuery drives the one query verb over a real fleet: the declared
// kind shapes the query, bare invocations explain, the state kind
// declines to get, an unknown index teaches what is declared.
func TestCLIQuery(t *testing.T) {
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
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
	run(ctx, t, "log", "create", "orders")
	run(ctx, t, "create", "invoice.invoice-1", "--payload", `{"title":"quantum widgets"}`)

	out := run(ctx, t, "index", "declare", "text")
	if !strings.Contains(out, "CHRON.API.INDEX.QUERY.orders.text") {
		t.Fatalf("index declare output: %s", out)
	}
	out = run(ctx, t, "index", "list")
	if !strings.Contains(out, "text\tsearch") || !strings.Contains(out, "state\tstate\t(born with the log, undeletable)") {
		t.Fatalf("index list output: %s", out)
	}

	// Asked bare, the index explains itself.
	out = run(ctx, t, "query", "text")
	if !strings.Contains(out, "search index: chronicle query text <text...>") {
		t.Fatalf("bare query output: %s", out)
	}
	// The state index is read with get.
	if err := runErr(ctx, "query", "state"); err == nil ||
		!strings.Contains(err.Error(), "chronicle get <thing>") {
		t.Fatalf("state query not declined: %v", err)
	}
	// An undeclared index teaches what is declared.
	if err := runErr(ctx, "query", "nowhere", "words"); err == nil ||
		!strings.Contains(err.Error(), "declared indexes: state, text") {
		t.Fatalf("unknown index teaching error missing: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		var qout bytes.Buffer
		err := cli.Run(ctx, []string{"query", "text", "widgets"}, &qout)
		if err == nil && strings.Contains(qout.String(), "invoice-1") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("query never hit: %v %s", err, qout.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	out = run(ctx, t, "index", "delete", "text")
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
// would: add a member (whose context is saved and selected), revoke the
// leak, rekey the tenant, and end up holding only credentials that work.
func TestCLIMemberVerbs(t *testing.T) {
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
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
	run(ctx, t, "log", "create", "orders")

	erinCreds := filepath.Join(t.TempDir(), "erin.creds")
	out := run(ctx, t, "member", "add", "acme", "erin", "--dir", dir, "--out", erinCreds)
	if !strings.Contains(out, "member erin added to acme: role writer") ||
		!strings.Contains(out, "context acme-erin saved and selected") {
		t.Fatalf("member add output: %s", out)
	}
	// Erin's fresh context has no log selected — the flag serves.
	run(ctx, t, "create", "ticket-1", "--log", "orders", "--payload", `{}`)

	out = run(ctx, t, "member", "revoke", "acme", "erin", "--dir", dir)
	if !strings.Contains(out, "member erin revoked from acme") {
		t.Fatalf("member revoke output: %s", out)
	}
	if err := runErr(ctx, "history", "ticket-1", "--log", "orders"); err == nil {
		t.Fatal("revoked creds still speak")
	}

	outDir := t.TempDir()
	out = run(ctx, t, "member", "rekey", "acme", "--dir", dir, "--out-dir", outDir)
	if !strings.Contains(out, "tenant acme rekeyed") || !strings.Contains(out, "member dana (admin) re-issued") {
		t.Fatalf("member rekey output: %s", out)
	}
	// Pre-rekey creds are dead — the context that held them proves it.
	if err := runErr(ctx, "log", "create", "late", "--context", "acme-dana"); err == nil {
		t.Fatal("pre-rekey creds still speak")
	}
	newDana := filepath.Join(outDir, "acme-dana.creds")
	run(ctx, t, "context", "save", "acme-dana", "--creds", newDana)
	run(ctx, t, "context", "select", "acme-dana")
	run(ctx, t, "log", "create", "after-rekey")
}
