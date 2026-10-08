package cli_test

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/impire-io/chronicle/cli"
)

// The retired words (chronicle-hq/00-META/vocabulary.md § words that never
// appear on a user surface, decision 0044 § 4): none of them appears in any
// help text the CLI prints. The mechanism's words and the substrate's stay
// in the record and behind the operator's documents.
var retired = regexp.MustCompile(`(?i)\b(things?|aspects?|fold(ed|s)?|roll-?ups?|births?|born|marked|tails?|segments?|META|buckets?|streams?|subjects?|responders?|nodes?|fleet|planes?|custody|0\d{3})\b`)

// helpTexts renders every help the grammar has: the root, each noun, and
// each verb.
func helpTexts(t *testing.T) map[string]string {
	t.Helper()
	ctx := context.Background()
	texts := map[string]string{}
	render := func(args ...string) string {
		var out bytes.Buffer
		if err := cli.Run(ctx, args, &out); err != nil {
			t.Fatalf("chronicle %s: %v", strings.Join(args, " "), err)
		}
		return out.String()
	}
	texts["root"] = render("--help")
	for _, noun := range []string{"store", "type", "op", "instance", "index", "member", "service-account", "context", "account"} {
		texts[noun] = render("help", noun)
		for _, line := range strings.Split(texts[noun], "\n") {
			if !strings.HasPrefix(line, "  chronicle ") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 3 {
				continue
			}
			verb := fields[2]
			texts[noun+" "+verb] = render(noun, verb, "--help")
		}
	}
	for _, session := range []string{"login", "logout", "version"} {
		texts[session] = render(session, "--help")
	}
	return texts
}

func TestHelpSpeaksTheUsersLanguage(t *testing.T) {
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	for name, text := range helpTexts(t) {
		if m := retired.FindString(text); m != "" {
			t.Errorf("the help for %q says %q:\n%s", name, m, text)
		}
	}
}

// Every noun's verbs are exactly the design's (07-the-cli.md § the grammar).
func TestGrammarIsTheDesigns(t *testing.T) {
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	texts := helpTexts(t)
	want := map[string][]string{
		"store":           {"list", "get", "create", "select"},
		"type":            {"list", "get", "create", "init"},
		"op":              {"list", "get", "create", "delete"},
		"instance":        {"list", "get", "create", "apply", "history", "watch", "snapshot"},
		"index":           {"list", "get", "create", "delete", "query"},
		"member":          {"list", "add", "remove", "set-role"},
		"service-account": {"list", "create", "revoke"},
		"context":         {"list", "show", "select", "add", "remove"},
		"account":         {"create"},
	}
	for noun, verbs := range want {
		for _, verb := range verbs {
			if _, ok := texts[noun+" "+verb]; !ok {
				t.Errorf("chronicle %s %s is missing from the help", noun, verb)
			}
		}
		listed := 0
		for key := range texts {
			if strings.HasPrefix(key, noun+" ") {
				listed++
			}
		}
		if listed != len(verbs) {
			t.Errorf("chronicle %s lists %d verbs, the design has %d", noun, listed, len(verbs))
		}
	}
}
