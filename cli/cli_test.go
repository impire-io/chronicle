package cli_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/impire-io/chronicle/cli"
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

func runErr(ctx context.Context, args ...string) (string, error) {
	var out bytes.Buffer
	err := cli.Run(ctx, args, &out)
	return out.String(), err
}

// startLocal boots the quick start in a temp dir with an isolated config
// store, and saves the local context the way `chronicle up` does.
func startLocal(ctx context.Context, t *testing.T) (*up.Local, string) {
	t.Helper()
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	dir := t.TempDir()
	l, err := up.Up(ctx, up.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	t.Cleanup(l.Stop)
	if err := cli.SaveLocalContext(l.URL, dir); err != nil {
		t.Fatalf("save local context: %v", err)
	}
	return l, dir
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const invoiceType = `schema:
  type: object
  properties:
    total: {type: number}
    status: {type: string}
history: compactable
children:
  comments: comment
operations:
  create:
    schema: {type: object}
    effect: merge
  send:
    schema:
      type: object
      required: [to]
    effect: merge
  note:
    schema: {type: object}
    effect: none
`

const commentType = `schema: {type: object}
operations:
  create:
    schema: {type: object}
    effect: merge
`

// TestCLISpine walks the design's quick start (07-the-cli.md) over the
// local quick start: one grammar, noun then verb; the local context `up`
// saved speaks without a flag; store create selects; the lists are
// tables; the refusals teach.
func TestCLISpine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	startLocal(ctx, t)

	// The local context is selected: context list marks it.
	out := run(ctx, t, "context", "list")
	if !strings.Contains(out, "*  local") {
		t.Fatalf("context list: %s", out)
	}
	// Nothing selected yet: the store teaching error.
	if _, err := runErr(ctx, "instance", "get", "invoice/inv-1"); err == nil || !strings.Contains(err.Error(), "no store selected") {
		t.Fatalf("store teaching error missing: %v", err)
	}
	out = run(ctx, t, "store", "create", "orders")
	if !strings.Contains(out, "store orders created") || !strings.Contains(out, "store orders selected") {
		t.Fatalf("store create: %s", out)
	}
	out = run(ctx, t, "store", "list")
	if !strings.Contains(out, "NAME") || !strings.Contains(out, "*  orders") || !strings.Contains(out, "compactable") {
		t.Fatalf("store list: %s", out)
	}
	out = run(ctx, t, "context", "show")
	if !strings.Contains(out, "context:   local") || !strings.Contains(out, "store:     orders") {
		t.Fatalf("context show: %s", out)
	}

	// Types from files; the effect is always stated.
	out = run(ctx, t, "type", "init", "invoice")
	if !strings.Contains(out, "operations:") || !strings.Contains(out, "effect: merge") {
		t.Fatalf("type init: %s", out)
	}
	out = run(ctx, t, "type", "create", "invoice", "-f", writeFile(t, "invoice.yaml", invoiceType))
	if !strings.Contains(out, "type invoice defined in store orders") || !strings.Contains(out, "operations: create (merge), note (none), send (merge)") || !strings.Contains(out, "children:   comments→comment") {
		t.Fatalf("type create: %s", out)
	}
	run(ctx, t, "type", "create", "comment", "-f", writeFile(t, "comment.yaml", commentType))
	if _, err := runErr(ctx, "type", "create", "bad", "-f", writeFile(t, "bad.yaml", "schema: {}\noperations:\n  go:\n    schema: {}\n")); err == nil || !strings.Contains(err.Error(), "operations.go.effect: state it") {
		t.Fatalf("an unstated effect must be refused: %v", err)
	}
	out = run(ctx, t, "type", "list")
	if !strings.Contains(out, "NAME") || !strings.Contains(out, "invoice") || !strings.Contains(out, "comments→comment") {
		t.Fatalf("type list: %s", out)
	}
	out = run(ctx, t, "type", "get", "invoice")
	if !strings.Contains(out, "type invoice  revision 1  history compactable") || !strings.Contains(out, "comments → comment") {
		t.Fatalf("type get: %s", out)
	}
	out = run(ctx, t, "op", "list", "invoice")
	if !strings.Contains(out, "NAME") || !strings.Contains(out, "send") {
		t.Fatalf("op list: %s", out)
	}
	if _, err := runErr(ctx, "op", "create", "invoice", "ship", "--schema", `{"type":"object"}`); err == nil || !strings.Contains(err.Error(), "--effect") {
		t.Fatalf("op create without --effect must be refused: %v", err)
	}
	out = run(ctx, t, "op", "create", "invoice", "ship", "--schema", `{"type":"object"}`, "--effect", "merge")
	if !strings.Contains(out, "operation ship created on invoice") {
		t.Fatalf("op create: %s", out)
	}
	out = run(ctx, t, "op", "delete", "invoice", "ship")
	if !strings.Contains(out, "operation ship deleted from invoice") {
		t.Fatalf("op delete: %s", out)
	}

	// Instances: create, apply, get, list, history, snapshot.
	out = run(ctx, t, "instance", "create", "invoice/inv-1", "--data", `{"total":120,"status":"draft"}`)
	if !strings.Contains(out, "created invoice/inv-1") {
		t.Fatalf("instance create: %s", out)
	}
	run(ctx, t, "instance", "create", "invoice/inv-2", "--data", `{"total":5,"status":"draft"}`)
	if _, err := runErr(ctx, "instance", "create", "invoice/inv-1", "--data", `{}`); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("a second create must say it exists: %v", err)
	}
	if _, err := runErr(ctx, "instance", "create", "invoice.inv-3"); err == nil || !strings.Contains(err.Error(), `separated by "/"`) {
		t.Fatalf("a dotted path must teach the grammar: %v", err)
	}
	if _, err := runErr(ctx, "instance", "create", "widget/w-1"); err == nil || !strings.Contains(err.Error(), `no type "widget" is defined`) {
		t.Fatalf("an undefined type must be named: %v", err)
	}
	out = run(ctx, t, "instance", "apply", "invoice/inv-1", "send", "--data", `{"to":"x","status":"sent"}`)
	if !strings.Contains(out, "applied send to invoice/inv-1") {
		t.Fatalf("instance apply: %s", out)
	}
	if _, err := runErr(ctx, "instance", "apply", "invoice/inv-1", "ship"); err == nil || !strings.Contains(err.Error(), `invoice defines no operation "ship". It defines: create, note, send`) {
		t.Fatalf("an undefined operation must list the defined ones: %v", err)
	}
	if _, err := runErr(ctx, "instance", "apply", "invoice/inv-1", "send", "--data", `{}`); err == nil || !strings.Contains(err.Error(), "does not fit the operation's schema") {
		t.Fatalf("a schema miss must be said: %v", err)
	}
	run(ctx, t, "instance", "create", "invoice/inv-1/comments/c-1", "--data", `{"body":"first"}`)
	if _, err := runErr(ctx, "instance", "create", "invoice/inv-1/notes/n-1"); err == nil || !strings.Contains(err.Error(), `declares no child "notes"`) {
		t.Fatalf("an undeclared child must be named: %v", err)
	}

	waitFor(t, func() bool {
		out, err := runErr(ctx, "instance", "get", "invoice/inv-1")
		return err == nil && strings.Contains(out, `"status": "sent"`)
	})
	out = run(ctx, t, "instance", "get", "invoice/inv-1")
	if !strings.HasPrefix(out, "invoice/inv-1  (sequence ") {
		t.Fatalf("instance get: %s", out)
	}

	// The list that was missing: by type, under a parent, with a filter.
	out = run(ctx, t, "instance", "list", "--type", "invoice")
	if !strings.Contains(out, "PATH") || !strings.Contains(out, "STATUS") || !strings.Contains(out, "invoice/inv-1") || !strings.Contains(out, "invoice/inv-2") || strings.Contains(out, "comments") {
		t.Fatalf("instance list --type: %s", out)
	}
	out = run(ctx, t, "instance", "list", "--type", "invoice", "--where", "status=sent")
	if !strings.Contains(out, "invoice/inv-1") || strings.Contains(out, "invoice/inv-2") {
		t.Fatalf("instance list --where: %s", out)
	}
	out = run(ctx, t, "instance", "list", "--in", "invoice/inv-1")
	if !strings.Contains(out, "invoice/inv-1/comments/c-1") || strings.Contains(out, "invoice/inv-2") {
		t.Fatalf("instance list --in: %s", out)
	}
	out = run(ctx, t, "instance", "list", "--output", "jsonl")
	if !strings.Contains(out, `"path":"invoice/inv-1"`) || !strings.Contains(out, `"type":"invoice"`) {
		t.Fatalf("instance list --output jsonl: %s", out)
	}
	out = run(ctx, t, "instance", "list", "--type", "invoice", "--sort", "total", "--limit", "1")
	if !strings.Contains(out, "invoice/inv-2") || strings.Contains(out, "invoice/inv-1") {
		t.Fatalf("instance list --sort --limit: %s", out)
	}
	out = run(ctx, t, "instance", "history", "invoice/inv-1")
	if !strings.Contains(out, "SEQ") || !strings.Contains(out, "create") || !strings.Contains(out, "send") {
		t.Fatalf("instance history: %s", out)
	}
	out = run(ctx, t, "instance", "snapshot", "invoice/inv-1")
	if !strings.Contains(out, "snapshot taken of invoice/inv-1") {
		t.Fatalf("instance snapshot: %s", out)
	}
	out = run(ctx, t, "instance", "history", "invoice/inv-1")
	if !strings.Contains(out, "snapshot") || strings.Contains(out, "send") {
		t.Fatalf("history after a snapshot: %s", out)
	}

	// Members and service accounts are two nouns over one registry.
	run(ctx, t, "member", "add", "jordan", "--role", "reader")
	run(ctx, t, "service-account", "create", "billing-svc", "--role", "writer")
	out = run(ctx, t, "member", "list")
	if !strings.Contains(out, "jordan") || strings.Contains(out, "billing-svc") {
		t.Fatalf("member list: %s", out)
	}
	out = run(ctx, t, "service-account", "list")
	if !strings.Contains(out, "billing-svc") || strings.Contains(out, "jordan") {
		t.Fatalf("service-account list: %s", out)
	}
	if _, err := runErr(ctx, "member", "set-role", "jordan", "admin"); err == nil || !strings.Contains(err.Error(), "not available yet") {
		t.Fatalf("set-role must say it waits on its verb: %v", err)
	}
	run(ctx, t, "member", "remove", "jordan")
	run(ctx, t, "service-account", "revoke", "billing-svc")
}

// TestCLIIndexes declares, lists and queries an index in the new words,
// and keeps state out of the index list.
func TestCLIIndexes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	startLocal(ctx, t)
	run(ctx, t, "store", "create", "orders")
	run(ctx, t, "type", "create", "invoice", "-f", writeFile(t, "invoice.yaml", invoiceType))
	run(ctx, t, "instance", "create", "invoice/inv-1", "--data", `{"total":1,"status":"quantum widgets"}`)
	run(ctx, t, "instance", "create", "invoice/inv-2", "--data", `{"total":2,"status":"plain paperclips"}`)

	out := run(ctx, t, "index", "list")
	if !strings.Contains(out, "no indexes") || strings.Contains(out, "state") {
		t.Fatalf("index list before: %s", out)
	}
	if _, err := runErr(ctx, "index", "create", "text"); err == nil || !strings.Contains(err.Error(), "--kind is required") {
		t.Fatalf("index create without a kind must teach: %v", err)
	}
	out = run(ctx, t, "index", "create", "text", "--kind", "search")
	if !strings.Contains(out, "index text created in store orders (search, from state)") {
		t.Fatalf("index create: %s", out)
	}
	out = run(ctx, t, "index", "list")
	if !strings.Contains(out, "NAME") || !strings.Contains(out, "text") || strings.Contains(out, "\nstate ") {
		t.Fatalf("index list: %s", out)
	}
	out = run(ctx, t, "index", "get", "text")
	if !strings.Contains(out, "index text  kind search  source state") {
		t.Fatalf("index get: %s", out)
	}
	waitFor(t, func() bool {
		out, err := runErr(ctx, "index", "query", "text", "widgets")
		return err == nil && strings.Contains(out, "invoice/inv-1") && strings.Contains(out, "1 of 1")
	})
	out = run(ctx, t, "index", "query", "text")
	if !strings.Contains(out, "index text is a search index: chronicle index query text TEXT...") {
		t.Fatalf("a bare query must say what the index accepts: %s", out)
	}
	if _, err := runErr(ctx, "index", "query", "nowhere", "x"); err == nil || !strings.Contains(err.Error(), "indexes: text") {
		t.Fatalf("an unknown index must list the declared ones: %v", err)
	}
	out = run(ctx, t, "index", "query", "text", "widgets", "--output", "jsonl")
	if !strings.Contains(out, `"instance":"invoice/inv-1"`) || !strings.Contains(out, `"trailer"`) {
		t.Fatalf("index query --output jsonl: %s", out)
	}
	out = run(ctx, t, "index", "delete", "text")
	if !strings.Contains(out, "index text deleted from store orders") {
		t.Fatalf("index delete: %s", out)
	}
}

// TestCLIHelp: help is everywhere and exits clean; an unknown sentence
// prints the help and fails; connection flags are documented once.
func TestCLIHelp(t *testing.T) {
	ctx := context.Background()
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	out := run(ctx, t)
	if !strings.Contains(out, "Your account:") || !strings.Contains(out, "instance") || !strings.Contains(out, "service-account") {
		t.Fatalf("root help: %s", out)
	}
	for _, args := range [][]string{{"--help"}, {"help"}, {"help", "instance"}, {"instance", "--help"}, {"instance", "list", "--help"}, {"version", "-h"}, {"context", "add", "-h"}} {
		if _, err := runErr(ctx, args...); err != nil {
			t.Fatalf("chronicle %s: help must exit clean: %v", strings.Join(args, " "), err)
		}
	}
	out, _ = runErr(ctx, "instance", "list", "--help")
	if !strings.Contains(out, "Usage: chronicle instance list") || !strings.Contains(out, "--where") || strings.Contains(out, "--creds") {
		t.Fatalf("verb help must show its own flags and not the connection flags: %s", out)
	}
	out, err := runErr(ctx, "instance", "frobnicate")
	if !errors.Is(err, cli.ErrUsage) || !strings.Contains(out, "chronicle instance list") {
		t.Fatalf("an unknown verb must print the noun's help and fail: %v\n%s", err, out)
	}
	if _, err := runErr(ctx, "frobnicate"); !errors.Is(err, cli.ErrUsage) {
		t.Fatalf("an unknown noun must fail with usage: %v", err)
	}
	if _, err := runErr(ctx, "instance"); !errors.Is(err, cli.ErrUsage) {
		t.Fatalf("a noun without a verb must fail with usage: %v", err)
	}
	if out := run(ctx, t, "version"); strings.TrimSpace(out) != version.Version {
		t.Fatalf("version: %q", out)
	}
}

// TestCLIContexts: contexts are added, selected, shown and removed; a
// store selection lives on its context.
func TestCLIContexts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	l, dir := startLocal(ctx, t)

	out := run(ctx, t, "context", "add", "dev", "--nkey", devdir.UserNkeyPath(dir), "--principal", devdir.LocalPrincipal, "--url", l.URL)
	if !strings.Contains(out, "context dev added") || !strings.Contains(out, "select it: chronicle context select dev") {
		t.Fatalf("context add: %s", out)
	}
	if _, err := runErr(ctx, "context", "add", "nobody"); !errors.Is(err, cli.ErrUsage) {
		t.Fatalf("context add without a credential must teach: %v", err)
	}
	run(ctx, t, "context", "select", "dev")
	run(ctx, t, "store", "create", "orders")
	out = run(ctx, t, "context", "show", "dev")
	if !strings.Contains(out, "store:     orders") || !strings.Contains(out, "as:        admin") {
		t.Fatalf("context show dev: %s", out)
	}
	out = run(ctx, t, "context", "show", "local")
	if strings.Contains(out, "store:     orders") {
		t.Fatalf("the selection must live on its own context: %s", out)
	}
	out = run(ctx, t, "store", "list", "--context", "local")
	if !strings.Contains(out, "orders") || strings.Contains(out, "*  orders") {
		t.Fatalf("store list through another context: %s", out)
	}
	out = run(ctx, t, "context", "remove", "dev")
	if !strings.Contains(out, "context dev removed") {
		t.Fatalf("context remove: %s", out)
	}
	if _, err := runErr(ctx, "store", "list"); err == nil || !strings.Contains(err.Error(), "no credential") {
		t.Fatalf("after removing the selection, the teaching error: %v", err)
	}
}

// TestCLIExtension: a build adds verbs and overrides nouns through the
// seam; a shadowing name is refused.
func TestCLIExtension(t *testing.T) {
	ctx := context.Background()
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	var out bytes.Buffer
	ext := &cli.Extension{
		Verbs: map[string]cli.Verb{"operator": func(_ context.Context, args []string, w io.Writer) error {
			_, err := w.Write([]byte("operator " + strings.Join(args, " ") + "\n"))
			return err
		}},
		Override: map[string]func(cli.Verb) cli.Verb{"member": func(open cli.Verb) cli.Verb {
			return func(ctx context.Context, args []string, w io.Writer) error {
				if len(args) >= 1 && args[0] == "rekey" {
					_, err := w.Write([]byte("rekeyed\n"))
					return err
				}
				return open(ctx, args, w)
			}
		}},
		Usage: "As the operator:\n  operator         the environment's own verbs",
	}
	if err := cli.RunWith(ctx, []string{"operator", "seal"}, &out, ext); err != nil || out.String() != "operator seal\n" {
		t.Fatalf("extension verb: %v %q", err, out.String())
	}
	out.Reset()
	if err := cli.RunWith(ctx, []string{"member", "rekey"}, &out, ext); err != nil || out.String() != "rekeyed\n" {
		t.Fatalf("override: %v %q", err, out.String())
	}
	out.Reset()
	if err := cli.RunWith(ctx, nil, &out, ext); err != nil || !strings.Contains(out.String(), "As the operator:") {
		t.Fatalf("root help must carry the extension's section: %v %s", err, out.String())
	}
	out.Reset()
	if err := cli.RunWith(ctx, []string{"member", "list", "--help"}, &out, ext); err != nil || !strings.Contains(out.String(), "chronicle member") {
		t.Fatalf("an overridden noun delegates help: %v %s", err, out.String())
	}
	bad := &cli.Extension{Verbs: map[string]cli.Verb{"store": ext.Verbs["operator"]}}
	if err := cli.RunWith(ctx, []string{"store", "list"}, &out, bad); err == nil || !strings.Contains(err.Error(), "shadows") {
		t.Fatalf("a shadowing extension verb must be refused: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not hold in time")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
