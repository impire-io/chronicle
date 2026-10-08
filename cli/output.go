package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"sigs.k8s.io/yaml"
)

// The output formats (decision 0045 § 3): a table for people, json (one
// document — a list becomes an array), jsonl (one object per line, as the
// SDK streams it) and yaml for tools.
const (
	formatTable = "table"
	formatJSON  = "json"
	formatJSONL = "jsonl"
	formatYAML  = "yaml"
)

// outputFlag registers --output on a command.
func outputFlag(fs *flag.FlagSet) *string {
	return fs.String("output", formatTable, "table, json, jsonl or yaml")
}

func checkFormat(format string) error {
	switch format {
	case formatTable, formatJSON, formatJSONL, formatYAML:
		return nil
	}
	return fmt.Errorf("--output %q: table, json, jsonl or yaml", format)
}

// table prints aligned columns with a header row.
func table(out io.Writer, header []string, rows [][]string) {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join(header, "\t"))
	for _, row := range rows {
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	_ = tw.Flush()
}

// printList prints a collection in the asked format: the rows as a table,
// or the items as json, jsonl or yaml.
func printList(out io.Writer, format string, header []string, rows [][]string, items []any) error {
	switch format {
	case formatJSON:
		if items == nil {
			items = []any{}
		}
		return printJSON(out, items)
	case formatJSONL:
		for _, item := range items {
			if err := jsonLine(out, item); err != nil {
				return err
			}
		}
		return nil
	case formatYAML:
		if items == nil {
			items = []any{}
		}
		return printYAML(out, items)
	default:
		table(out, header, rows)
		return nil
	}
}

// printValue prints one value: the human rendering, or the value as json,
// jsonl (the same one object) or yaml.
func printValue(out io.Writer, format string, human func(), v any) error {
	switch format {
	case formatJSON, formatJSONL:
		return printJSON(out, v)
	case formatYAML:
		return printYAML(out, v)
	default:
		human()
		return nil
	}
}

func printJSON(out io.Writer, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%s\n", b)
	return err
}

// jsonLine prints one JSON object per line — a listing streams to stdout
// as it streams to the SDK caller.
func jsonLine(out io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%s\n", b)
	return err
}

func printYAML(out io.Writer, v any) error {
	b, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	_, err = out.Write(b)
	return err
}

// readDocument reads a YAML or JSON document — from a file, or inline —
// and returns it as JSON. YAML because the audience edits config in it;
// JSON is YAML, so it is accepted wherever YAML is.
func readDocument(inline, file string) (json.RawMessage, error) {
	switch {
	case inline != "" && file != "":
		return nil, fmt.Errorf("--data and -f are two ways to give the data; pick one")
	case file != "":
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", file, err)
		}
		return toJSON(raw)
	case inline != "":
		return toJSON([]byte(inline))
	}
	return json.RawMessage(`{}`), nil
}

// toJSON converts YAML (or JSON) bytes to compact JSON.
func toJSON(raw []byte) (json.RawMessage, error) {
	if json.Valid(raw) {
		return json.RawMessage(raw), nil
	}
	j, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("not valid YAML or JSON: %w", err)
	}
	return json.RawMessage(j), nil
}

// fileOrInline reads a document that may be a file path or inline text.
func fileOrInline(arg string) (json.RawMessage, error) {
	if _, err := os.Stat(arg); err == nil {
		return readDocument("", arg)
	}
	return toJSON([]byte(arg))
}

// compact renders JSON on one line, or "(none)".
func compact(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "(none)"
	}
	var b strings.Builder
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	var v any
	if err := dec.Decode(&v); err != nil {
		return string(raw)
	}
	bs, err := json.Marshal(v)
	if err != nil {
		return string(raw)
	}
	b.Write(bs)
	return b.String()
}

// pretty renders JSON indented.
func pretty(raw json.RawMessage) string {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return string(raw)
	}
	return string(b)
}

// scalarText renders a state value for a table cell: scalars as text,
// anything nested as compact JSON, absent as blank.
func scalarText(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64, bool, json.Number:
		return fmt.Sprint(x)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}
