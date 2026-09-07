package cli_test

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/impire-io/chronicle/internal/cli"
	"github.com/impire-io/chronicle/internal/fleet"
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
	run(ctx, t, "schema", "set", "orders", "comment.add", "--dir", dir, "--creds", creds,
		"--schema", `{"type":"object","required":["body"]}`)
	out = run(ctx, t, "thing", "create", "orders", "invoice-1", "--dir", dir, "--creds", creds,
		"--state", `{"total":3}`)
	if !strings.Contains(out, "born: invoice-1") {
		t.Fatalf("thing create output: %s", out)
	}
	run(ctx, t, "append", "orders", "invoice-1", "comment.add", "--dir", dir, "--creds", creds,
		"--payload", `{"body":"hi"}`)

	// Pre-flight refusal happens before the wire — the CLI surfaces it.
	var errOut bytes.Buffer
	if err := cli.Run(ctx, []string{"append", "orders", "invoice-1", "comment.add",
		"--dir", dir, "--creds", creds, "--payload", `{"nobody":1}`}, &errOut); err == nil {
		t.Fatal("invalid payload appended")
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		var stateOut bytes.Buffer
		err := cli.Run(ctx, []string{"state", "orders", "invoice-1", "--dir", dir, "--creds", creds}, &stateOut)
		if err == nil && strings.Contains(stateOut.String(), `{"total":3}`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("state never appeared: %v %s", err, stateOut.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The effect flag reaches the wire: a merge-declared type moves state.
	run(ctx, t, "schema", "set", "orders", "status.set", "--dir", dir, "--creds", creds,
		"--schema", `{"type":"object"}`, "--effect", "merge")
	run(ctx, t, "append", "orders", "invoice-1", "status.set", "--dir", dir, "--creds", creds,
		"--payload", `{"status":"closed"}`)
	deadline = time.Now().Add(10 * time.Second)
	for {
		var stateOut bytes.Buffer
		err := cli.Run(ctx, []string{"state", "orders", "invoice-1", "--dir", dir, "--creds", creds}, &stateOut)
		if err == nil && strings.Contains(stateOut.String(), `"status":"closed"`) &&
			strings.Contains(stateOut.String(), `"total":3`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("merged state never appeared: %v %s", err, stateOut.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	out = run(ctx, t, "replay", "orders", "invoice-1", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "snapshot") || !strings.Contains(out, "comment.add") || !strings.Contains(out, "by dana") {
		t.Fatalf("replay output: %s", out)
	}

	// invoice-1 carries an effect-none comment: the node declines, with
	// the reason on stdout.
	out = run(ctx, t, "thing", "rollup", "orders", "invoice-1", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "not compacted") || !strings.Contains(out, "effect none") {
		t.Fatalf("thing rollup veto output: %s", out)
	}

	// A fully merge-covered thing compacts to one snapshot.
	run(ctx, t, "thing", "create", "orders", "invoice-2", "--dir", dir, "--creds", creds,
		"--state", `{"total":1}`)
	run(ctx, t, "append", "orders", "invoice-2", "status.set", "--dir", dir, "--creds", creds,
		"--payload", `{"status":"paid"}`)
	out = run(ctx, t, "thing", "rollup", "orders", "invoice-2", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "compacted: invoice-2") {
		t.Fatalf("thing rollup output: %s", out)
	}
	out = run(ctx, t, "replay", "orders", "invoice-2", "--dir", dir, "--creds", creds)
	if !strings.Contains(out, "by chronicle-node") || strings.Contains(out, "status.set") {
		t.Fatalf("replay after rollup: %s", out)
	}

	// Bad flags fail before dialing anything.
	var bad bytes.Buffer
	if err := cli.Run(ctx, []string{"log", "create", "orders"}, &bad); err == nil {
		t.Fatal("log create without --creds must fail")
	}
}
