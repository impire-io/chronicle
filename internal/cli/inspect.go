package cli

// The teaching half of the vocabulary verbs (0025): the definition
// skeleton, the define echo-back, and the readable type inspection —
// where a reader learns the addressing grammar and the operations a type
// carries.

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/impire-io/chronicle/contract"
)

// typeDefinition is the --def / --file JSON: every facet of a type in one
// act (0021). Operations carry each op's payload schema and effect.
type typeDefinition struct {
	Schema     json.RawMessage           `json:"schema"`
	History    string                    `json:"history,omitempty"`
	Aspects    map[string]string         `json:"aspects,omitempty"`
	Operations map[string]contract.OpDef `json:"operations,omitempty"`
}

// typeSkeleton is what `type init` prints: a definition to edit, with a
// create operation already declared — the constructor `chronicle create`
// invokes by convention (0025 § 3).
const typeSkeleton = `{
  "schema": {
    "type": "object",
    "properties": {}
  },
  "history": "compactable",
  "aspects": {},
  "operations": {
    "create": {
      "schema": {"type": "object"},
      "effect": "merge"
    }
  }
}
`

func typeInit(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle type init", flag.ContinueOnError)
	fs.SetOutput(out)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) > 1 {
		return fmt.Errorf("type init: at most one type name")
	}
	fmt.Fprint(out, typeSkeleton)
	return nil
}

// echoDefinition says back what a define just set — the facets, not just
// a revision number (0025 § 4).
func echoDefinition(out io.Writer, def typeDefinition) {
	fmt.Fprintf(out, "  history: %s\n", contract.NormalizeHistory(def.History))
	fmt.Fprintf(out, "  aspects: %s\n", aspectsLine(def.Aspects))
	if len(def.Operations) == 0 {
		fmt.Fprintf(out, "  operations: (none)\n")
		return
	}
	names := operationNames(def.Operations)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s (%s)", name, contract.NormalizeEffect(def.Operations[name].Effect)))
	}
	fmt.Fprintf(out, "  operations: %s\n", joinComma(parts))
}

// printTypeRecord is the readable inspection: shape, operations with
// effects, the aspects map, the history facet — the raw record stays
// behind --json.
func printTypeRecord(out io.Writer, name string, rec contract.TypeRecord) {
	fmt.Fprintf(out, "type %s — revision %d · history %s\n", name, rec.Revision, contract.NormalizeHistory(rec.History))
	fmt.Fprintf(out, "schema: %s\n", compactJSON(rec.Schema))
	if len(rec.Aspects) == 0 {
		fmt.Fprintln(out, "aspects: (none)")
	} else {
		fmt.Fprintln(out, "aspects:")
		segments := make([]string, 0, len(rec.Aspects))
		for segment := range rec.Aspects {
			segments = append(segments, segment)
		}
		sort.Strings(segments)
		for _, segment := range segments {
			fmt.Fprintf(out, "  %s → %s\n", segment, rec.Aspects[segment])
		}
	}
	if len(rec.Operations) == 0 {
		fmt.Fprintln(out, "operations: (none)")
		return
	}
	fmt.Fprintln(out, "operations:")
	printOperations(out, rec.Operations)
}

func printOperations(out io.Writer, ops map[string]contract.OpDef) {
	if len(ops) == 0 {
		fmt.Fprintln(out, "(none)")
		return
	}
	width := 0
	names := operationNames(ops)
	for _, name := range names {
		if len(name) > width {
			width = len(name)
		}
	}
	for _, name := range names {
		fmt.Fprintf(out, "  %-*s  %s\n", width, name, contract.NormalizeEffect(ops[name].Effect))
	}
}

func aspectsLine(aspects map[string]string) string {
	if len(aspects) == 0 {
		return "(none)"
	}
	segments := make([]string, 0, len(aspects))
	for segment := range aspects {
		segments = append(segments, segment)
	}
	sort.Strings(segments)
	parts := make([]string, 0, len(segments))
	for _, segment := range segments {
		parts = append(parts, fmt.Sprintf("%s → %s", segment, aspects[segment]))
	}
	return joinComma(parts)
}

func joinComma(parts []string) string {
	var b bytes.Buffer
	for i, p := range parts {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(p)
	}
	return b.String()
}

func compactJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "(none)"
	}
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		return string(raw)
	}
	return b.String()
}

// schemaArg reads an operation's --schema: a file path when one exists,
// inline JSON otherwise.
func schemaArg(arg string) (json.RawMessage, error) {
	if _, err := os.Stat(arg); err == nil {
		raw, err := os.ReadFile(arg)
		if err != nil {
			return nil, err
		}
		if !json.Valid(raw) {
			return nil, fmt.Errorf("--schema %s: not valid JSON", arg)
		}
		return raw, nil
	}
	if !json.Valid([]byte(arg)) {
		return nil, fmt.Errorf("--schema: neither a readable file nor valid JSON")
	}
	return json.RawMessage(arg), nil
}
