package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

func (x *runner) instanceNoun() *noun {
	return &noun{
		name:    "instance",
		summary: "the instances of a type: create, apply operations, read state and history, take a snapshot",
		verbs: []command{
			{"list [--type TYPE] [--in PATH] [--where FIELD=VALUE]... [--sort FIELD] [--limit N]", "the instances in the store, as a table", x.instanceList},
			{"get PATH", "an instance's current state and the sequence it stands at", x.instanceGet},
			{"create PATH [--data JSON | -f FILE] [--op OP]", "create an instance by applying its type's create operation", x.instanceCreate},
			{"apply PATH OP [--data JSON | -f FILE] [--expect SEQ]", "apply an operation to an instance", x.instanceApply},
			{"history PATH [--from SEQ] [--follow]", "an instance's operations in order", x.instanceHistory},
			{"watch PATH", "print an instance's state each time it changes", x.instanceWatch},
			{"snapshot PATH", "write the current state as a snapshot and compact the history before it", x.instanceSnapshot},
		},
	}
}

// whereFlag collects repeatable --where FIELD=VALUE clauses.
type whereFlag [][2]string

func (w *whereFlag) String() string { return fmt.Sprint([][2]string(*w)) }
func (w *whereFlag) Set(v string) error {
	field, value, ok := strings.Cut(v, "=")
	if !ok || field == "" {
		return fmt.Errorf("--where %q: FIELD=VALUE", v)
	}
	*w = append(*w, [2]string{field, value})
	return nil
}

func (x *runner) instanceList(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("instance list", "list [--type TYPE] [--in PATH] [--where FIELD=VALUE]... [--sort FIELD] [--limit N]", "the instances in the store, as a table", out)
	cf := x.connect(fs)
	format := outputFlag(fs)
	typeName := fs.String("type", "", "only instances of this type (top level, or the children of --in that are of it)")
	under := fs.String("in", "", "only the direct children of this instance")
	var where whereFlag
	fs.Var(&where, "where", "only instances whose state field equals the value (repeatable; filtered on this side — an index is the answer for large stores)")
	sortBy := fs.String("sort", "", "sort by a state field, or path, type, seq")
	limit := fs.Int("limit", 0, "stop after this many rows (0: all)")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 0, "no arguments (narrow with --type, --in and --where)"); err != nil {
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
	var opts []client.ListOpt
	if *typeName != "" {
		opts = append(opts, client.ByType(*typeName))
	}
	if *under != "" {
		opts = append(opts, client.Under(*under))
	}
	for _, clause := range where {
		opts = append(opts, client.Where(clause[0], clause[1]))
	}
	var infos []client.InstanceInfo
	for info, err := range c.ListInstances(ctx, store, opts...) {
		if err != nil {
			return err
		}
		infos = append(infos, info)
	}
	// A listing is sorted and shaped here: it is small by the time a human
	// reads it, and the state is already in hand.
	states := make([]map[string]any, len(infos))
	for i, info := range infos {
		_ = json.Unmarshal(info.State, &states[i])
	}
	if err := sortInstances(infos, states, *sortBy); err != nil {
		return err
	}
	if *limit > 0 && len(infos) > *limit {
		infos, states = infos[:*limit], states[:*limit]
	}
	columns := summaryColumns(states)
	header := append([]string{"PATH", "TYPE", "SEQ"}, upper(columns)...)
	var rows [][]string
	var items []any
	for i, info := range infos {
		row := []string{info.Path, info.Type, fmt.Sprint(info.Seq)}
		for _, col := range columns {
			row = append(row, scalarText(states[i][col]))
		}
		rows = append(rows, row)
		items = append(items, info)
	}
	if err := printList(out, *format, header, rows, items); err != nil {
		return err
	}
	if *format == formatTable && len(rows) == 0 {
		fmt.Fprintf(out, "(no instances%s)\n", narrowing(*typeName, *under, where))
	}
	return nil
}

func narrowing(typeName, under string, where whereFlag) string {
	var parts []string
	if typeName != "" {
		parts = append(parts, "of type "+typeName)
	}
	if under != "" {
		parts = append(parts, "in "+under)
	}
	for _, w := range where {
		parts = append(parts, w[0]+"="+w[1])
	}
	if len(parts) == 0 {
		return ""
	}
	return " " + strings.Join(parts, ", ")
}

// summaryColumns picks the state fields a table shows beside the path:
// the first few scalar fields, by name, across the rows.
func summaryColumns(states []map[string]any) []string {
	const most = 4
	seen := map[string]bool{}
	for _, st := range states {
		for k, v := range st {
			switch v.(type) {
			case string, float64, bool, nil:
				seen[k] = true
			}
		}
	}
	cols := sortedKeys(seen)
	if len(cols) > most {
		cols = cols[:most]
	}
	return cols
}

func upper(s []string) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[i] = strings.ToUpper(v)
	}
	return out
}

func sortInstances(infos []client.InstanceInfo, states []map[string]any, by string) error {
	if by == "" {
		by = "path"
	}
	idx := make([]int, len(infos))
	for i := range idx {
		idx[i] = i
	}
	key := func(i int) string {
		switch by {
		case "path":
			return infos[i].Path
		case "type":
			return infos[i].Type + " " + infos[i].Path
		case "seq":
			return fmt.Sprintf("%020d", infos[i].Seq)
		}
		v, ok := states[i][by]
		if !ok {
			return "\xff" // absent sorts last
		}
		if f, isNum := v.(float64); isNum {
			return fmt.Sprintf("%020.6f", f)
		}
		return scalarText(v)
	}
	sort.SliceStable(idx, func(a, b int) bool { return key(idx[a]) < key(idx[b]) })
	sortedInfos := make([]client.InstanceInfo, len(infos))
	sortedStates := make([]map[string]any, len(states))
	for i, j := range idx {
		sortedInfos[i], sortedStates[i] = infos[j], states[j]
	}
	copy(infos, sortedInfos)
	copy(states, sortedStates)
	return nil
}

func (x *runner) instanceGet(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("instance get", "get PATH", "an instance's current state and the sequence it stands at", out)
	cf := x.connect(fs)
	format := outputFlag(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one path (type/id)"); err != nil {
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
	sv, err := c.State(ctx, store, pos[0])
	if err != nil {
		if errors.Is(err, client.ErrNoState) {
			return fmt.Errorf("%s has no state in store %s: it does not exist yet, or its first operation has not been applied to the state yet", pos[0], store)
		}
		return err
	}
	return printValue(out, *format, func() {
		fmt.Fprintf(out, "%s  (sequence %d)\n%s\n", pos[0], sv.Seq, pretty(sv.State))
	}, map[string]any{"path": pos[0], "seq": sv.Seq, "state": sv.State})
}

func (x *runner) instanceCreate(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("instance create", "create PATH [--data JSON | -f FILE] [--op OP]", "create an instance by applying its type's create operation", out)
	cf := x.connect(fs)
	data := fs.String("data", "", "the operation's data, inline (JSON)")
	file := fs.String("f", "", "the operation's data, from a YAML or JSON file")
	opName := fs.String("op", "create", "the operation that creates")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one path (type/id)"); err != nil {
		return err
	}
	payload, err := readDocument(*data, *file)
	if err != nil {
		return err
	}
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	res, err := c.Resolve(ctx, store, pos[0])
	if err != nil {
		return err
	}
	switch res.Kind {
	case contract.ResolvedTyped:
		ack, err := c.Create(ctx, store, pos[0], *opName, payload)
		if err != nil {
			if errors.Is(err, client.ErrUndefinedOperation) {
				return fmt.Errorf("%s defines no operation %q to create with. It defines: %s (--op picks another)", res.TypeName, *opName, strings.Join(sortedKeys(res.Record.Operations), ", "))
			}
			return teachWrite(err, pos[0], store)
		}
		fmt.Fprintf(out, "created %s (sequence %d)\n", pos[0], ack.Seq)
	case contract.ResolvedUndeclared:
		return fmt.Errorf("%s cannot be created: %s", pos[0], res.Detail)
	default:
		var names []string
		for t, err := range c.ListTypes(ctx, store) {
			if err == nil {
				names = append(names, t)
			}
		}
		if len(names) == 0 {
			return fmt.Errorf("%s: no type %q is defined in store %s — define one first (chronicle type init %s)", pos[0], contract.PathType(pos[0]), store, contract.PathType(pos[0]))
		}
		return fmt.Errorf("%s: no type %q is defined in store %s — types: %s", pos[0], contract.PathType(pos[0]), store, strings.Join(sortedStrings(names), ", "))
	}
	return nil
}

// teachWrite puts a refused write in the user's words.
func teachWrite(err error, path, store string) error {
	switch {
	case errors.Is(err, client.ErrInstanceExists):
		return fmt.Errorf("%s already exists in store %s", path, store)
	case errors.Is(err, client.ErrInstanceMoved):
		return fmt.Errorf("%s moved past the expected sequence: read it again (chronicle instance get %s) and retry with its sequence", path, path)
	case errors.Is(err, client.ErrSchemaViolation):
		return fmt.Errorf("the data does not fit the operation's schema: %w", err)
	}
	return err
}

func (x *runner) instanceApply(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("instance apply", "apply PATH OP [--data JSON | -f FILE] [--expect SEQ]", "apply an operation to an instance", out)
	cf := x.connect(fs)
	data := fs.String("data", "", "the operation's data, inline (JSON)")
	file := fs.String("f", "", "the operation's data, from a YAML or JSON file")
	expect := fs.Int64("expect", -1, "the sequence the instance must still stand at (the one instance get printed); refused if it moved")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 2, "PATH OP"); err != nil {
		return err
	}
	payload, err := readDocument(*data, *file)
	if err != nil {
		return err
	}
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	var opts []client.ApplyOpt
	if *expect >= 0 {
		opts = append(opts, client.WithExpectedSeq(uint64(*expect)))
	}
	ack, err := c.Apply(ctx, store, pos[0], pos[1], payload, opts...)
	if err != nil {
		if errors.Is(err, client.ErrUndefinedOperation) {
			if res, rerr := c.Resolve(ctx, store, pos[0]); rerr == nil && res.Kind == contract.ResolvedTyped {
				return undefinedOperation(res.TypeName, pos[1], res.Record.Operations)
			}
		}
		return teachWrite(err, pos[0], store)
	}
	fmt.Fprintf(out, "applied %s to %s (sequence %d)\n", pos[1], pos[0], ack.Seq)
	return nil
}

// opRow is one operation as history prints it.
type opRow struct {
	Seq       uint64          `json:"seq"`
	Operation string          `json:"operation"`
	Author    string          `json:"author"`
	Time      string          `json:"time,omitempty"`
	ID        string          `json:"id"`
	Parents   []string        `json:"parents,omitempty"`
	Data      json.RawMessage `json:"data"`
}

func toOpRow(op contract.Op) opRow {
	row := opRow{Seq: op.Seq, Operation: op.Type, Author: op.Author, ID: op.ID, Parents: op.Parents, Data: op.Payload}
	if !op.Ts.IsZero() {
		row.Time = op.Ts.UTC().Format(time.RFC3339)
	}
	if len(row.Data) == 0 {
		row.Data = json.RawMessage("null")
	}
	return row
}

func (x *runner) instanceHistory(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("instance history", "history PATH [--from SEQ] [--follow]", "an instance's operations in order", out)
	cf := x.connect(fs)
	format := outputFlag(fs)
	from := fs.Uint64("from", 0, "start after this sequence")
	follow := fs.Bool("follow", false, "keep printing operations as they land (stop with Ctrl-C)")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one path (type/id)"); err != nil {
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
	var ops func(func(contract.Op, error) bool)
	switch {
	case *follow:
		var topts []client.TailOpt
		if *from > 0 {
			topts = append(topts, client.After(*from))
		}
		ops = c.Tail(ctx, store, pos[0], topts...)
	case *from > 0:
		ops = c.FoldTail(ctx, store, pos[0], *from)
	default:
		ops = c.History(ctx, store, pos[0])
	}
	// A history streams: each row prints as it arrives, so --follow and a
	// long history read the same way; a table holds its header only.
	if *format == formatTable {
		table(out, []string{"SEQ", "OPERATION", "AUTHOR", "TIME", "DATA"}, nil)
	}
	n := 0
	for op, err := range ops {
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		n++
		row := toOpRow(op)
		switch *format {
		case formatTable:
			fmt.Fprintf(out, "%d  %s  %s  %s  %s\n", row.Seq, row.Operation, row.Author, row.Time, compact(row.Data))
		case formatYAML:
			if err := printYAML(out, []any{row}); err != nil {
				return err
			}
		default:
			if err := jsonLine(out, row); err != nil {
				return err
			}
		}
	}
	if *format == formatTable && n == 0 {
		fmt.Fprintf(out, "(no history for %s)\n", pos[0])
	}
	return nil
}

func (x *runner) instanceWatch(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("instance watch", "watch PATH", "print an instance's state each time it changes", out)
	cf := x.connect(fs)
	format := outputFlag(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one path (type/id)"); err != nil {
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
	for sv, err := range c.Watch(ctx, store, pos[0]) {
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if *format == formatTable {
			fmt.Fprintf(out, "sequence %d  %s\n", sv.Seq, compact(sv.State))
			continue
		}
		if err := jsonLine(out, map[string]any{"path": pos[0], "seq": sv.Seq, "state": sv.State}); err != nil {
			return err
		}
	}
	return nil
}

func (x *runner) instanceSnapshot(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("instance snapshot", "snapshot PATH", "write the current state as a snapshot and compact the history before it", out)
	cf := x.connect(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one path (type/id)"); err != nil {
		return err
	}
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.Snapshot(ctx, store, pos[0])
	if err != nil {
		return err
	}
	// Declining is an answer, not a failure: the reason is named.
	if !resp.Taken {
		fmt.Fprintf(out, "no snapshot taken: %s\n", resp.Reason)
		return nil
	}
	fmt.Fprintf(out, "snapshot taken of %s (sequence %d)\n", pos[0], resp.Seq)
	return nil
}
