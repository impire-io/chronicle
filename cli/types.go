package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// typeDefinition is a type file: every facet of a type in one act
// (0021), as the user writes it — YAML or JSON.
type typeDefinition struct {
	Schema     json.RawMessage           `json:"schema"`
	History    string                    `json:"history,omitempty"`
	Children   map[string]string         `json:"children,omitempty"`
	Operations map[string]contract.OpDef `json:"operations,omitempty"`
}

// typeSkeleton is what `type init` writes: a definition to edit, with a
// create operation already declared — the one `instance create` applies
// — and the effect stated on it, as it must be on every operation.
const typeSkeleton = `# A type: what an instance of it looks like, and what may happen to it.
# Edit, then:  chronicle type create %s -f %s.yaml

# The shape of an instance's state (JSON Schema).
schema:
  type: object
  properties: {}

# The history policy: compactable (a snapshot may replace older history)
# or full (every operation is kept; snapshots are declined).
history: compactable

# Children: instances nested under one of this type, by name -> type.
# For example:   comments: comment   gives  %s/<id>/comments/<id>
children: {}

# The operations this type allows. Each has a schema for its data and an
# effect: merge (updates the state: named fields overwrite, null deletes)
# or none (recorded in history only; the state is unchanged).
operations:
  create:          # applied by: chronicle instance create %s/<id>
    schema:
      type: object
    effect: merge
`

func (x *runner) typeNoun() *noun {
	return &noun{
		name:    "type",
		summary: "the types a store defines: a schema, operations, children, a history policy",
		verbs: []command{
			{"list", "the types in the store", x.typeList},
			{"get TYPE", "one type in full", x.typeGet},
			{"create TYPE -f FILE", "define a type from a YAML or JSON file; again to make its next revision", x.typeCreate},
			{"init TYPE", "write a commented type file to edit", x.typeInit},
		},
	}
}

func (x *runner) opNoun() *noun {
	return &noun{
		name:    "op",
		summary: "the operations a type allows (also: operation)",
		verbs: []command{
			{"list TYPE", "the operations the type allows", x.opList},
			{"get TYPE OP", "one operation: its effect and its schema", x.opGet},
			{"create TYPE OP --schema FILE --effect merge|none", "add or replace one operation on the type", x.opCreate},
			{"delete TYPE OP", "remove one operation from the type", x.opDelete},
		},
	}
}

func (x *runner) typeInit(_ context.Context, args []string, out io.Writer) error {
	fs := flags("type init", "init TYPE", "write a commented type file to edit", out)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one type name"); err != nil {
		return err
	}
	if err := contract.ValidateTypeName(pos[0]); err != nil {
		return err
	}
	fmt.Fprintf(out, typeSkeleton, pos[0], pos[0], pos[0], pos[0])
	return nil
}

func (x *runner) typeList(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("type list", "list", "the types in the store", out)
	cf := x.connect(fs)
	format := outputFlag(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 0, "no arguments"); err != nil {
		return err
	}
	if err := checkFormat(*format); err != nil {
		return err
	}
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	var names []string
	for name, err := range c.ListTypes(ctx, store) {
		if err != nil {
			return err
		}
		names = append(names, name)
	}
	var rows [][]string
	var items []any
	for _, name := range sortedStrings(names) {
		rec, err := c.GetType(ctx, store, name)
		if err != nil {
			return err
		}
		rows = append(rows, []string{name, fmt.Sprint(rec.Revision), strings.Join(sortedKeys(rec.Operations), ", "), childrenLine(rec.Children), contract.NormalizeHistory(rec.History)})
		items = append(items, map[string]any{"name": name, "revision": rec.Revision, "operations": sortedKeys(rec.Operations), "children": rec.Children, "history": contract.NormalizeHistory(rec.History)})
	}
	return printList(out, *format, []string{"NAME", "REVISION", "OPERATIONS", "CHILDREN", "HISTORY"}, rows, items)
}

func (x *runner) typeGet(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("type get", "get TYPE", "one type in full", out)
	cf := x.connect(fs)
	format := outputFlag(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one type name"); err != nil {
		return err
	}
	if err := checkFormat(*format); err != nil {
		return err
	}
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	rec, err := c.GetType(ctx, store, pos[0])
	if err != nil {
		return teachNoType(err, pos[0])
	}
	return printValue(out, *format, func() { printTypeRecord(out, pos[0], rec) }, rec)
}

func teachNoType(err error, name string) error {
	if errors.Is(err, client.ErrNoType) {
		return fmt.Errorf("%w — chronicle type create %s -f FILE", err, name)
	}
	return err
}

// printTypeRecord is the readable form: the facets, in words.
func printTypeRecord(out io.Writer, name string, rec contract.TypeRecord) {
	fmt.Fprintf(out, "type %s  revision %d  history %s\n", name, rec.Revision, contract.NormalizeHistory(rec.History))
	fmt.Fprintf(out, "schema: %s\n", compact(rec.Schema))
	if len(rec.Children) == 0 {
		fmt.Fprintln(out, "children: (none)")
	} else {
		fmt.Fprintln(out, "children:")
		for _, child := range sortedKeys(rec.Children) {
			fmt.Fprintf(out, "  %s → %s\n", child, rec.Children[child])
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
	width := 0
	names := sortedKeys(ops)
	for _, name := range names {
		if len(name) > width {
			width = len(name)
		}
	}
	for _, name := range names {
		fmt.Fprintf(out, "  %-*s  %s\n", width, name, contract.NormalizeEffect(ops[name].Effect))
	}
}

func childrenLine(children map[string]string) string {
	if len(children) == 0 {
		return ""
	}
	parts := make([]string, 0, len(children))
	for _, child := range sortedKeys(children) {
		parts = append(parts, child+"→"+children[child])
	}
	return strings.Join(parts, ", ")
}

func (x *runner) typeCreate(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("type create", "create TYPE -f FILE", "define a type from a YAML or JSON file; again to make its next revision", out)
	cf := x.connect(fs)
	file := fs.String("f", "", "the type file (YAML or JSON): schema, history, children, operations")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one type name"); err != nil {
		return err
	}
	if *file == "" {
		fs.Usage()
		return fmt.Errorf("%w: -f FILE is required (chronicle type init %s writes one)", ErrUsage, pos[0])
	}
	raw, err := readDocument("", *file)
	if err != nil {
		return err
	}
	var def typeDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		return fmt.Errorf("%s: %w", *file, err)
	}
	if err := checkDefinition(def); err != nil {
		return fmt.Errorf("%s: %w", *file, err)
	}
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.DefineType(ctx, store, pos[0], client.TypeDefinition{
		Schema: def.Schema, History: def.History, Children: def.Children, Operations: def.Operations,
	})
	if err != nil {
		return err
	}
	if resp.Revision == 1 {
		fmt.Fprintf(out, "type %s defined in store %s\n", pos[0], store)
	} else {
		fmt.Fprintf(out, "type %s redefined in store %s: revision %d\n", pos[0], store, resp.Revision)
	}
	echoDefinition(out, def)
	return nil
}

// checkDefinition refuses, before the wire, what the node would refuse
// in its own words, and the one rule the surfaces add: the effect is
// always stated (decision 0044 § 3).
func checkDefinition(def typeDefinition) error {
	if h := contract.NormalizeHistory(def.History); h != contract.HistoryCompactable && h != contract.HistoryFull {
		return fmt.Errorf("history: %q (want compactable, full)", def.History)
	}
	for name, op := range def.Operations {
		if op.Effect == "" {
			return fmt.Errorf("operations.%s.effect: state it — merge (updates the state) or none (recorded in history only)", name)
		}
		if !contract.KnownEffect(op.Effect) {
			return fmt.Errorf("operations.%s.effect: unknown effect %q (want merge, none)", name, op.Effect)
		}
		if len(op.Schema) == 0 {
			return fmt.Errorf("operations.%s.schema: give the data's schema ({} accepts anything)", name)
		}
	}
	return nil
}

// echoDefinition says back what a define just set — the facets, not just
// a revision number.
func echoDefinition(out io.Writer, def typeDefinition) {
	fmt.Fprintf(out, "  history:    %s\n", contract.NormalizeHistory(def.History))
	if len(def.Children) == 0 {
		fmt.Fprintf(out, "  children:   (none)\n")
	} else {
		fmt.Fprintf(out, "  children:   %s\n", childrenLine(def.Children))
	}
	if len(def.Operations) == 0 {
		fmt.Fprintf(out, "  operations: (none)\n")
		return
	}
	parts := make([]string, 0, len(def.Operations))
	for _, name := range sortedKeys(def.Operations) {
		parts = append(parts, fmt.Sprintf("%s (%s)", name, contract.NormalizeEffect(def.Operations[name].Effect)))
	}
	fmt.Fprintf(out, "  operations: %s\n", strings.Join(parts, ", "))
}

// loadType is the operation verbs' shared preamble: the connection, the
// store, and the type record the edit starts from.
func (x *runner) loadType(ctx context.Context, cf connectFlags, out io.Writer, typeName string) (*client.Client, string, contract.TypeRecord, error) {
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return nil, "", contract.TypeRecord{}, err
	}
	rec, err := c.GetType(ctx, store, typeName)
	if err != nil {
		c.Close()
		return nil, "", contract.TypeRecord{}, teachNoType(err, typeName)
	}
	return c, store, rec, nil
}

// redefine writes the type back whole — 0021's one act, composed by the
// CLI. The node bumps the revision; two racing edits resolve by
// latest-declaration-wins.
func redefine(ctx context.Context, c *client.Client, store, typeName string, rec contract.TypeRecord) (uint64, error) {
	resp, err := c.DefineType(ctx, store, typeName, client.TypeDefinition{
		Schema: rec.Schema, History: rec.History, Children: rec.Children, Operations: rec.Operations,
	})
	if err != nil {
		return 0, err
	}
	return resp.Revision, nil
}

func (x *runner) opList(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("op list", "list TYPE", "the operations the type allows", out)
	cf := x.connect(fs)
	format := outputFlag(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one type name"); err != nil {
		return err
	}
	if err := checkFormat(*format); err != nil {
		return err
	}
	c, _, rec, err := x.loadType(ctx, cf, out, pos[0])
	if err != nil {
		return err
	}
	defer c.Close()
	var rows [][]string
	var items []any
	for _, name := range sortedKeys(rec.Operations) {
		def := rec.Operations[name]
		rows = append(rows, []string{name, contract.NormalizeEffect(def.Effect), compact(def.Schema)})
		items = append(items, map[string]any{"name": name, "effect": contract.NormalizeEffect(def.Effect), "schema": def.Schema})
	}
	return printList(out, *format, []string{"NAME", "EFFECT", "SCHEMA"}, rows, items)
}

func (x *runner) opGet(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("op get", "get TYPE OP", "one operation: its effect and its schema", out)
	cf := x.connect(fs)
	format := outputFlag(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 2, "TYPE OP"); err != nil {
		return err
	}
	if err := checkFormat(*format); err != nil {
		return err
	}
	c, _, rec, err := x.loadType(ctx, cf, out, pos[0])
	if err != nil {
		return err
	}
	defer c.Close()
	def, ok := rec.Operations[pos[1]]
	if !ok {
		return undefinedOperation(pos[0], pos[1], rec.Operations)
	}
	return printValue(out, *format, func() {
		fmt.Fprintf(out, "operation %s on %s  effect %s\n%s\n", pos[1], pos[0], contract.NormalizeEffect(def.Effect), pretty(def.Schema))
	}, map[string]any{"name": pos[1], "type": pos[0], "effect": contract.NormalizeEffect(def.Effect), "schema": def.Schema})
}

// undefinedOperation is the teaching refusal: what the type does define.
func undefinedOperation(typeName, op string, ops map[string]contract.OpDef) error {
	if len(ops) == 0 {
		return fmt.Errorf("%s defines no operation %q. It defines none yet (chronicle op create %s %s --schema FILE --effect merge)", typeName, op, typeName, op)
	}
	return fmt.Errorf("%s defines no operation %q. It defines: %s", typeName, op, strings.Join(sortedKeys(ops), ", "))
}

func (x *runner) opCreate(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("op create", "create TYPE OP --schema FILE --effect merge|none", "add or replace one operation on the type", out)
	cf := x.connect(fs)
	schema := fs.String("schema", "", "the data's schema (JSON Schema): a YAML or JSON file, or inline JSON (required)")
	effect := fs.String("effect", "", "what applying it does to the state: merge (updates the state) or none (recorded in history only) (required)")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 2, "TYPE OP"); err != nil {
		return err
	}
	if *schema == "" || *effect == "" {
		fs.Usage()
		return fmt.Errorf("%w: --schema and --effect are both required", ErrUsage)
	}
	if !contract.KnownEffect(*effect) {
		return fmt.Errorf("--effect %q: merge (updates the state) or none (recorded in history only)", *effect)
	}
	raw, err := fileOrInline(*schema)
	if err != nil {
		return fmt.Errorf("--schema: %w", err)
	}
	c, store, rec, err := x.loadType(ctx, cf, out, pos[0])
	if err != nil {
		return err
	}
	defer c.Close()
	if rec.Operations == nil {
		rec.Operations = map[string]contract.OpDef{}
	}
	// The consequence, said before the write: a changed effect makes the
	// derived state suspect, and the store's state is rebuilt.
	if old, ok := rec.Operations[pos[1]]; ok && contract.NormalizeEffect(old.Effect) != *effect {
		fmt.Fprintf(out, "effect changes %s → %s: the state of store %s is rebuilt from its history\n", contract.NormalizeEffect(old.Effect), *effect, store)
	}
	rec.Operations[pos[1]] = contract.OpDef{Schema: raw, Effect: *effect}
	revision, err := redefine(ctx, c, store, pos[0], rec)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "operation %s created on %s (effect %s; %s is at revision %d)\n", pos[1], pos[0], *effect, pos[0], revision)
	return nil
}

func (x *runner) opDelete(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("op delete", "delete TYPE OP", "remove one operation from the type", out)
	cf := x.connect(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 2, "TYPE OP"); err != nil {
		return err
	}
	c, store, rec, err := x.loadType(ctx, cf, out, pos[0])
	if err != nil {
		return err
	}
	defer c.Close()
	if _, ok := rec.Operations[pos[1]]; !ok {
		return undefinedOperation(pos[0], pos[1], rec.Operations)
	}
	// The consequence, said before the write: history is never touched;
	// its operations of this name read as invalid from now on.
	fmt.Fprintf(out, "history keeps every %s operation already applied; they count as invalid from now on and move no state\n", pos[1])
	delete(rec.Operations, pos[1])
	revision, err := redefine(ctx, c, store, pos[0], rec)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "operation %s deleted from %s (%s is at revision %d)\n", pos[1], pos[0], pos[0], revision)
	return nil
}

func sortedStrings(s []string) []string {
	out := append([]string(nil), s...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
