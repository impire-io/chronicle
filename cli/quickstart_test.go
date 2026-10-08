package cli_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/impire-io/chronicle/cli"
)

// The README's quick start runs, line by line, against an in-process
// quick start (decision 0045 § 5): the block fenced as `sh quick-start`
// is the one a new user types, so it is the one CI executes. A line that
// reads derived state is retried for a moment — state and indexes trail
// the history — and a line that still fails fails the test.

// quickStartLines reads the fenced block from the README.
func quickStartLines(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	in := false
	for _, line := range strings.Split(string(raw), "\n") {
		switch {
		case strings.HasPrefix(line, "```sh quick-start"):
			in = true
		case in && strings.HasPrefix(line, "```"):
			return lines
		case in:
			if cmd := strings.TrimSpace(stripComment(line)); cmd != "" {
				lines = append(lines, cmd)
			}
		}
	}
	t.Fatal("README.md has no ```sh quick-start block")
	return nil
}

// stripComment drops a trailing "# …" outside quotes.
func stripComment(line string) string {
	quoted := false
	for i, r := range line {
		switch {
		case r == '\'':
			quoted = !quoted
		case r == '#' && !quoted && (i == 0 || line[i-1] == ' '):
			return line[:i]
		}
	}
	return line
}

// splitShell splits one line into words, honouring single quotes, and
// peels a trailing "> file" redirect.
func splitShell(line string) (words []string, redirect string) {
	var cur strings.Builder
	quoted, have := false, false
	flush := func() {
		if have {
			words = append(words, cur.String())
			cur.Reset()
			have = false
		}
	}
	for _, r := range line {
		switch {
		case r == '\'':
			quoted = !quoted
			have = true
		case r == ' ' && !quoted:
			flush()
		default:
			cur.WriteRune(r)
			have = true
		}
	}
	flush()
	for i, w := range words {
		if w == ">" && i+1 < len(words) {
			return words[:i], words[i+1]
		}
	}
	return words, ""
}

func TestREADMEQuickStartRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	startLocal(ctx, t) // `chronicle up`, with its local context saved
	work := t.TempDir()
	lines := quickStartLines(t)
	if len(lines) < 5 {
		t.Fatalf("the quick start has only %d lines", len(lines))
	}
	for _, line := range lines {
		words, redirect := splitShell(line)
		if len(words) < 2 || words[0] != "chronicle" {
			t.Fatalf("not a chronicle sentence: %q", line)
		}
		args := words[1:]
		// Files named in the sentence live in the work dir.
		for i, a := range args {
			if strings.HasSuffix(a, ".yaml") {
				args[i] = filepath.Join(work, a)
			}
		}
		var out bytes.Buffer
		var err error
		deadline := time.Now().Add(15 * time.Second)
		for {
			out.Reset()
			err = cli.Run(ctx, args, &out)
			if err == nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("%s: %v\n%s", line, err, out.String())
		}
		if redirect != "" {
			if err := os.WriteFile(filepath.Join(work, redirect), out.Bytes(), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("$ %s\n%s", line, out.String())
	}
}
