package cli_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/impire-io/chronicle/cli"
	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/devdir"
	"github.com/impire-io/chronicle/internal/version"
	"github.com/impire-io/chronicle/up"
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

// startLocal boots the quick start in a temp dir and returns it with the
// dir; the config store is isolated per test.
func startLocal(ctx context.Context, t *testing.T) (*up.Local, string) {
	t.Helper()
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	dir := t.TempDir()
	l, err := up.Up(ctx, up.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	t.Cleanup(l.Stop)
	return l, dir
}

// TestCLISpine runs the design's day-in-the-life (07-the-cli.md) over the
// open quick start: the local user speaks with nothing saved, a context
// saved from the same seed speaks too, log create selects, the sentences
// speak through the context, and the refusals teach.
func TestCLISpine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	l, dir := startLocal(ctx, t)

	// Nothing saved and no quick start under the default dir: a sentence
	// teaches the fixes.
	if err := runErr(ctx, "get", "invoice.invoice-1", "--dir", filepath.Join(t.TempDir(), "nowhere")); err == nil || !strings.Contains(err.Error(), "no credentials") {
		t.Fatalf("creds teaching error missing: %v", err)
	}

	// The quick start's fallback: --dir alone finds the url and the user,
	// and the user is the registry's admin.
	out := run(ctx, t, "log", "list", "--dir", dir)
	if strings.TrimSpace(out) != "" {
		t.Fatalf("fresh log list output: %q", out)
	}

	// A context saved on the same seed: the open form's way of being
	// someone on any NATS.
	out = run(ctx, t, "context", "save", "local", "--nkey", devdir.UserNkeyPath(dir), "--principal", "dana", "--url", l.URL)
	if !strings.Contains(out, "context local saved") {
		t.Fatalf("context save output: %s", out)
	}
	// dana is not a member: the node refuses, the assertion is checked.
	run(ctx, t, "context", "select", "local")
	if err := runErr(ctx, "log", "create", "orders"); err == nil || !strings.Contains(err.Error(), "not a member") {
		t.Fatalf("non-member not refused: %v", err)
	}
	run(ctx, t, "context", "save", "local", "--nkey", devdir.UserNkeyPath(dir), "--principal", devdir.LocalPrincipal)

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
	if !strings.Contains(out, "context: local") || !strings.Contains(out, "as:      admin") || !strings.Contains(out, "log:     orders") {
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

	// The expected-sequence guard reaches the wire (0018).
	run(ctx, t, "do", "invoice.invoice-1", "comment.add",
		"--payload", `{"body":"guarded"}`, "--expect-seq", strconv.FormatUint(head, 10))
	if err := runErr(ctx, "do", "invoice.invoice-1", "comment.add",
		"--payload", `{"body":"late"}`, "--expect-seq", strconv.FormatUint(head, 10)); err == nil ||
		!strings.Contains(err.Error(), "moved past the guard") {
		t.Fatalf("stale guard not refused: %v", err)
	}

	// The constructor's merge fed the fold: get reads it back.
	waitFor(ctx, t, []string{"get", "invoice.invoice-1"}, `"total":3`)

	// Operations are managed where they live.
	out = run(ctx, t, "op", "define", "invoice", "status.set", "--schema", `{"type":"object"}`, "--effect", "merge")
	if !strings.Contains(out, "operation status.set on invoice: effect merge, revision 2") {
		t.Fatalf("op define output: %s", out)
	}
	out = run(ctx, t, "operation", "list", "invoice")
	if !strings.Contains(out, "status.set") || !strings.Contains(out, "merge") {
		t.Fatalf("operation list output: %s", out)
	}
	run(ctx, t, "do", "invoice.invoice-1", "status.set", "--payload", `{"status":"closed"}`)
	waitFor(ctx, t, []string{"get", "invoice.invoice-1"}, `"status":"closed"`)

	out = run(ctx, t, "op", "define", "invoice", "status.set", "--schema", `{"type":"object"}`)
	if !strings.Contains(out, "effect changes merge → none: derived state is suspect") {
		t.Fatalf("effect change narration missing: %s", out)
	}
	out = run(ctx, t, "op", "rm", "invoice", "status.set")
	if !strings.Contains(out, "re-fold as effect none with a warning") ||
		!strings.Contains(out, "operation status.set removed from invoice") {
		t.Fatalf("op rm output: %s", out)
	}

	// History speaks the operations, create included, by the principal
	// the context states.
	out = run(ctx, t, "history", "invoice.invoice-1")
	if !strings.Contains(out, "create") || !strings.Contains(out, "comment.add") || !strings.Contains(out, "by admin") {
		t.Fatalf("history output: %s", out)
	}

	// Rollup: typed compactable compacts; untyped keeps its veto.
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

	run(ctx, t, "type", "define", "receipt", "--def", `{"schema":{"type":"object"},"operations":{"scan":{"schema":{"type":"object"}}}}`)
	if err := runErr(ctx, "create", "receipt.r-1"); err == nil ||
		!strings.Contains(err.Error(), "operations on receipt: scan") {
		t.Fatalf("constructor teaching error missing: %v", err)
	}

	// A preserved log (0019): created and selected, its rollup declines.
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

	// Who may act: the admin registers a member, who speaks through a
	// context on the same seed under their own name; revoked, they are
	// refused; the list shows the registry.
	out = run(ctx, t, "member", "add", "erin", "--role", "writer")
	if !strings.Contains(out, "member erin added: role writer") {
		t.Fatalf("member add output: %s", out)
	}
	run(ctx, t, "context", "save", "erin", "--nkey", devdir.UserNkeyPath(dir), "--principal", "erin", "--url", l.URL)
	run(ctx, t, "rollup", "invoice.invoice-1", "--log", "orders", "--context", "erin")
	if err := runErr(ctx, "log", "create", "theirs", "--context", "erin"); err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("writer governed: %v", err)
	}
	out = run(ctx, t, "member", "list")
	if !strings.Contains(out, "admin\tadmin") || !strings.Contains(out, "erin\twriter") {
		t.Fatalf("member list output: %s", out)
	}
	out = run(ctx, t, "member", "revoke", "erin")
	if !strings.Contains(out, "member erin revoked: gone from the registry") {
		t.Fatalf("member revoke output: %s", out)
	}
	if err := runErr(ctx, "rollup", "invoice.invoice-1", "--log", "orders", "--context", "erin"); err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("revoked member still speaks: %v", err)
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

func waitFor(ctx context.Context, t *testing.T, args []string, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var out bytes.Buffer
		err := cli.Run(ctx, args, &out)
		if err == nil && strings.Contains(out.String(), want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%v never showed %q: %v %s", args, want, err, out.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestCLIContextStore drives the context verbs alone — the store needs
// no server — in both ways of being someone.
func TestCLIContextStore(t *testing.T) {
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	ctx := context.Background()

	credsA := filepath.Join(t.TempDir(), "a.creds")
	seedB := filepath.Join(t.TempDir(), "b.nk")

	out := run(ctx, t, "context", "save", "alpha", "--creds", credsA, "--url", "nats://localhost:4222")
	if !strings.Contains(out, "context alpha saved") || !strings.Contains(out, "select it") {
		t.Fatalf("context save output: %s", out)
	}
	out = run(ctx, t, "context", "select", "alpha")
	if !strings.Contains(out, "context alpha selected") {
		t.Fatalf("context select output: %s", out)
	}
	// An nkey context needs its principal, and only one way at a time.
	if err := runErr(ctx, "context", "save", "beta", "--nkey", seedB); err == nil || !strings.Contains(err.Error(), "--principal") {
		t.Fatalf("nkey without principal not refused: %v", err)
	}
	if err := runErr(ctx, "context", "save", "beta", "--nkey", seedB, "--creds", credsA, "--principal", "bo"); err == nil || !strings.Contains(err.Error(), "pick one") {
		t.Fatalf("two ways not refused: %v", err)
	}
	run(ctx, t, "context", "save", "beta", "--nkey", seedB, "--principal", "bo")
	out = run(ctx, t, "context", "show", "beta")
	if !strings.Contains(out, "nkey:    "+seedB) || !strings.Contains(out, "as:      bo") {
		t.Fatalf("nkey context show output: %s", out)
	}
	out = run(ctx, t, "context", "list")
	if !strings.Contains(out, "* alpha") || !strings.Contains(out, "  beta") {
		t.Fatalf("context list output: %s", out)
	}
	out = run(ctx, t, "context", "show")
	if !strings.Contains(out, "context: alpha") || !strings.Contains(out, "nats://localhost:4222") ||
		!strings.Contains(out, "creds:   "+credsA) ||
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
	if err := runErr(ctx, "context", "show"); err == nil ||
		!strings.Contains(err.Error(), "no context selected") {
		t.Fatalf("dangling selection survived rm: %v", err)
	}
}

// TestCLIQuery drives the one query verb over the quick start: the
// declared kind shapes the query, bare invocations explain, the state
// kind declines to get, an unknown index teaches what is declared — and
// the quick start's own supervisor serves the index it declared.
func TestCLIQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	_, dir := startLocal(ctx, t)
	t.Setenv("CHRONICLE_CONTEXT", "")
	run(ctx, t, "context", "save", "local", "--nkey", devdir.UserNkeyPath(dir), "--principal", devdir.LocalPrincipal, "--url", mustURL(t, dir))
	run(ctx, t, "context", "select", "local")
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

	out = run(ctx, t, "query", "text")
	if !strings.Contains(out, "search index: chronicle query text <text...>") {
		t.Fatalf("bare query output: %s", out)
	}
	if err := runErr(ctx, "query", "state"); err == nil ||
		!strings.Contains(err.Error(), "chronicle get <thing>") {
		t.Fatalf("state query not declined: %v", err)
	}
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

func mustURL(t *testing.T, dir string) string {
	t.Helper()
	url, err := devdir.ReadClientURL(dir)
	if err != nil {
		t.Fatal(err)
	}
	return url
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

// TestCLIExtension: a build's extension is dispatched by name, prints
// its sections first, may not shadow the open grammar, and --bridge is
// refused with the reason where no build offers it.
func TestCLIExtension(t *testing.T) {
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	ctx := context.Background()
	called := ""
	ext := &cli.Extension{
		Verbs: map[string]cli.Verb{"fly": func(_ context.Context, args []string, out io.Writer) error {
			called = strings.Join(args, " ")
			fmt.Fprintln(out, "flew")
			return nil
		}},
		Usage: "run a fleet\n  chronicle fly [--far]\n",
	}
	var out bytes.Buffer
	if err := cli.RunWith(ctx, []string{"fly", "--far", "north"}, &out, ext); err != nil || called != "--far north" || !strings.Contains(out.String(), "flew") {
		t.Fatalf("extension verb: err=%v called=%q out=%q", err, called, out.String())
	}
	out.Reset()
	if err := cli.RunWith(ctx, nil, &out, ext); err == nil || !strings.HasPrefix(out.String(), "chronicle — ops-logs as a product\n\nrun a fleet\n  chronicle fly") || !strings.Contains(out.String(), "define vocabulary") {
		t.Fatalf("extension usage: %v\n%s", err, out.String())
	}
	shadow := &cli.Extension{Verbs: map[string]cli.Verb{"get": ext.Verbs["fly"]}}
	if err := cli.RunWith(ctx, []string{"get", "x"}, &out, shadow); err == nil || !strings.Contains(err.Error(), "shadows") {
		t.Fatalf("shadowing verb not refused: %v", err)
	}
	// An override wraps an open verb and may delegate to it.
	wrapped := &cli.Extension{Override: map[string]func(cli.Verb) cli.Verb{
		"member": func(open cli.Verb) cli.Verb {
			return func(ctx context.Context, args []string, out io.Writer) error {
				if len(args) >= 1 && args[0] == "mint" {
					fmt.Fprintln(out, "minted")
					return nil
				}
				return open(ctx, args, out)
			}
		},
	}}
	out.Reset()
	if err := cli.RunWith(ctx, []string{"member", "mint"}, &out, wrapped); err != nil || !strings.Contains(out.String(), "minted") {
		t.Fatalf("override: %v %s", err, out.String())
	}
	if err := cli.RunWith(ctx, []string{"member", "list"}, &out, wrapped); err == nil || !strings.Contains(err.Error(), "no credentials") {
		t.Fatalf("override did not delegate to the open verb: %v", err)
	}
	stray := &cli.Extension{Override: map[string]func(cli.Verb) cli.Verb{"fly": nil}}
	if err := cli.RunWith(ctx, []string{"version"}, &out, stray); err == nil || !strings.Contains(err.Error(), "does not have") {
		t.Fatalf("stray override not refused: %v", err)
	}
	if err := runErr(ctx, "log", "list", "--bridge", "p.json", "--account", "acme"); err == nil || !strings.Contains(err.Error(), "this build has none") {
		t.Fatalf("--bridge without a dialer not refused: %v", err)
	}
	// A bridge context carries the profile and the account: the sentences
	// need neither flag, and the dial is the build's (0035).
	dialed := ""
	bridged := &cli.Extension{BridgeDial: func(profile, account string) (*client.Client, error) {
		dialed = profile + " " + account
		return nil, errors.New("dialed")
	}}
	out.Reset()
	if err := cli.RunWith(ctx, []string{"context", "save", "hosted", "--bridge", "p.json", "--account", "acme"}, &out, bridged); err != nil {
		t.Fatalf("context save --bridge: %v", err)
	}
	if err := cli.RunWith(ctx, []string{"log", "list", "--context", "hosted"}, &out, bridged); err == nil || err.Error() != "dialed" || !strings.HasSuffix(dialed, "/p.json acme") {
		t.Fatalf("bridge context dial: err=%v dialed=%q", err, dialed)
	}
	if err := cli.RunWith(ctx, []string{"log", "list", "--context", "hosted", "--account", "other"}, &out, bridged); err == nil || !strings.HasSuffix(dialed, "/p.json other") {
		t.Fatalf("--account did not beat the context's: %q", dialed)
	}
	out.Reset()
	if err := cli.RunWith(ctx, []string{"context", "show", "hosted"}, &out, bridged); err != nil || !strings.Contains(out.String(), "bridge:  ") || !strings.Contains(out.String(), "account: acme") {
		t.Fatalf("context show for a bridge context: %v\n%s", err, out.String())
	}
	if err := cli.RunWith(ctx, []string{"log", "list", "--bridge", "p.json"}, &out, bridged); err == nil || !strings.Contains(err.Error(), "--account") {
		t.Fatalf("--bridge without an account not taught: %v", err)
	}
	// A build reads the selection back — the managed account create finds
	// the bridge login there.
	if err := cli.RunWith(ctx, []string{"context", "select", "hosted"}, &out, bridged); err != nil {
		t.Fatalf("context select: %v", err)
	}
	if c, ok, err := cli.LoadContext(""); err != nil || !ok || !strings.HasSuffix(c.Bridge, "/p.json") || c.Account != "acme" {
		t.Fatalf("LoadContext of the selection: %+v ok=%v err=%v", c, ok, err)
	}
	if _, ok, err := cli.LoadContext("nowhere"); err == nil || ok {
		t.Fatalf("LoadContext of a missing context: ok=%v err=%v", ok, err)
	}
	if err := cli.RunWith(ctx, []string{"context", "save", "half", "--bridge", "p.json"}, &out, bridged); err == nil || !strings.Contains(err.Error(), "--account") {
		t.Fatalf("context save --bridge without --account not refused: %v", err)
	}
}
