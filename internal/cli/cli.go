// Package cli implements the chronicle CLI verbs over the client package —
// an adapter on the one product surface, never a side door.
package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/version"
)

// Run dispatches one CLI invocation (everything except `up`, which is the
// composition root's).
func Run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return usage(out)
	}
	switch args[0] {
	case "tenant":
		if len(args) >= 2 && args[1] == "create" {
			return tenantCreate(ctx, args[2:], out)
		}
		return usage(out)
	case "log":
		if len(args) >= 2 && args[1] == "create" {
			return logCreate(ctx, args[2:], out)
		}
		return usage(out)
	case "schema":
		if len(args) >= 2 && args[1] == "set" {
			return schemaSet(ctx, args[2:], out)
		}
		return usage(out)
	case "thing":
		if len(args) >= 2 && args[1] == "create" {
			return thingCreate(ctx, args[2:], out)
		}
		if len(args) >= 2 && args[1] == "rollup" {
			return thingRollup(ctx, args[2:], out)
		}
		return usage(out)
	case "index":
		if len(args) >= 2 && args[1] == "declare" {
			return indexDeclare(ctx, args[2:], out)
		}
		if len(args) >= 2 && args[1] == "delete" {
			return indexDelete(ctx, args[2:], out)
		}
		if len(args) >= 2 && args[1] == "query" {
			return indexQuery(ctx, args[2:], out)
		}
		return usage(out)
	case "semantic":
		if len(args) >= 2 && args[1] == "query" {
			return semanticQuery(ctx, args[2:], out)
		}
		return usage(out)
	case "graph":
		if len(args) >= 2 && args[1] == "neighbors" {
			return graphNeighbors(ctx, args[2:], out)
		}
		if len(args) >= 2 && args[1] == "walk" {
			return graphWalk(ctx, args[2:], out)
		}
		return usage(out)
	case "append":
		return appendOp(ctx, args[1:], out)
	case "state":
		return state(ctx, args[1:], out)
	case "replay":
		return replay(ctx, args[1:], out)
	case "version":
		fmt.Fprintln(out, version.Version)
		return nil
	default:
		return usage(out)
	}
}

func usage(out io.Writer) error {
	fmt.Fprint(out, `chronicle — ops-logs as a product (walking skeleton)

  chronicle up [--dir D] [--port N]                     run the local fleet
  chronicle tenant create <name> [--dir D] [--admin P] [--out F]
  chronicle log create <log> --creds F [--url U] [--desc S]
  chronicle schema set <log> <op.type> --creds F --schema JSON | --file F [--effect E]
  chronicle thing create <log> <thing> --creds F [--state JSON]
  chronicle thing rollup <log> <thing> --creds F
  chronicle index declare <log> <index> --creds F [--kind K] [--config JSON]
  chronicle index delete <log> <index> --creds F
  chronicle index query <log> <index> [query...] --creds F [--limit N] [--offset N]
  chronicle semantic query <log> <index> <text...> --creds F [--limit N] [--offset N]
  chronicle graph neighbors <log> <index> <thing> --creds F [--direction D] [--label L] [--limit N] [--offset N]
  chronicle graph walk <log> <index> <thing> --creds F [--direction D] [--labels a,b] [--depth N] [--limit N]
  chronicle append <log> <thing> <op.type> --creds F [--payload JSON] [--parents a,b]
  chronicle state <log> <thing> --creds F
  chronicle replay <log> <thing> --creds F
  chronicle version                                     print the version
`)
	return fmt.Errorf("usage")
}

// parseArgs parses flags and positionals interleaved: the stdlib flag
// package stops at the first positional, so keep popping positionals and
// re-parsing until everything is consumed.
func parseArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

// connectFlags are the flags every tenant-side verb shares.
type connectFlags struct {
	url   *string
	creds *string
	dir   *string
}

func addConnectFlags(fs *flag.FlagSet) connectFlags {
	return connectFlags{
		url:   fs.String("url", "", "NATS url (default: the --dir fleet's recorded url)"),
		creds: fs.String("creds", "", "credentials file (required)"),
		dir:   fs.String("dir", devdir.Default(), "local fleet data dir, used when --url is not given"),
	}
}

func (cf connectFlags) dial() (*client.Client, error) {
	if *cf.creds == "" {
		return nil, fmt.Errorf("--creds is required")
	}
	url := *cf.url
	if url == "" {
		var err error
		url, err = devdir.ReadClientURL(*cf.dir)
		if err != nil {
			return nil, err
		}
	}
	return client.ConnectFile(url, *cf.creds)
}

func tenantCreate(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle tenant create", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "local fleet data dir")
	admin := fs.String("admin", "admin", "first principal's ID")
	outFile := fs.String("out", "", "where to write the admin .creds (default <name>-<admin>.creds)")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("tenant create: exactly one tenant name")
	}
	name := pos[0]

	url, err := devdir.ReadClientURL(*dir)
	if err != nil {
		return err
	}
	creds, err := os.ReadFile(devdir.ControlCredsPath(*dir))
	if err != nil {
		return fmt.Errorf("read control creds: %w", err)
	}
	nc, err := client.ConnectControlCreds(url, creds)
	if err != nil {
		return err
	}
	defer nc.Close()

	resp, err := nc.MintTenant(ctx, name, *admin)
	if err != nil {
		return err
	}
	path := *outFile
	if path == "" {
		path = fmt.Sprintf("%s-%s.creds", name, resp.Admin)
	}
	if err := os.WriteFile(path, resp.AdminCreds, 0o600); err != nil {
		return fmt.Errorf("write admin creds: %w", err)
	}
	fmt.Fprintf(out, "tenant %s minted: account %s\n", name, resp.Account)
	fmt.Fprintf(out, "admin creds (the only copy): %s\n", path)
	return nil
}

func logCreate(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle log create", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	desc := fs.String("desc", "", "log description")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("log create: exactly one log name")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.CreateLog(ctx, pos[0], *desc)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "log %s created: stream %s\n", pos[0], resp.Stream)
	return nil
}

func schemaSet(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle schema set", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	inline := fs.String("schema", "", "JSON Schema, inline")
	file := fs.String("file", "", "JSON Schema file")
	effect := fs.String("effect", "", "how the op moves state: none (default) or merge")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("schema set: <log> <op.type>")
	}
	var schema []byte
	switch {
	case *inline != "" && *file != "":
		return fmt.Errorf("--schema and --file are exclusive")
	case *inline != "":
		schema = []byte(*inline)
	case *file != "":
		var err error
		schema, err = os.ReadFile(*file)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("one of --schema or --file is required")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.SetSchema(ctx, pos[0], pos[1], schema, *effect)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "schema %s %s: revision %d\n", pos[0], pos[1], resp.Revision)
	return nil
}

func thingCreate(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle thing create", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	state := fs.String("state", "{}", "the birth snapshot's state")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("thing create: <log> <thing>")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	ack, err := c.CreateThing(ctx, pos[0], pos[1], json.RawMessage(*state))
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "born: %s at seq %d (op %s)\n", pos[1], ack.Seq, ack.OpID)
	return nil
}

func thingRollup(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle thing rollup", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("thing rollup: <log> <thing>")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.RollupThing(ctx, pos[0], pos[1])
	if err != nil {
		return err
	}
	// Declining is an answer, not a failure: the node names its reason.
	if !resp.Rolled {
		fmt.Fprintf(out, "not compacted: %s\n", resp.Reason)
		return nil
	}
	fmt.Fprintf(out, "compacted: %s at seq %d\n", pos[1], resp.Seq)
	return nil
}

func indexDeclare(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle index declare", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	kind := fs.String("kind", "search", "index kind")
	config := fs.String("config", "", "kind config as JSON (graph: edge rules)")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("index declare: <log> <index>")
	}
	var cfgRaw json.RawMessage
	if *config != "" {
		cfgRaw = json.RawMessage(*config)
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.DeclareIndex(ctx, pos[0], pos[1], *kind, cfgRaw)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "index %s/%s declared (%s): query %s\n", pos[0], pos[1], *kind, resp.Query)
	return nil
}

func indexDelete(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle index delete", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("index delete: <log> <index>")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.DeleteIndex(ctx, pos[0], pos[1]); err != nil {
		return err
	}
	fmt.Fprintf(out, "index %s/%s retired\n", pos[0], pos[1])
	return nil
}

func indexQuery(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle index query", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	limit := fs.Int("limit", 0, "max hits (default 10, cap 100)")
	offset := fs.Int("offset", 0, "hits to skip")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 2 {
		return fmt.Errorf("index query: <log> <index> [query...]")
	}
	query := strings.Join(pos[2:], " ")
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.QueryIndex(ctx, pos[0], pos[1], query, *limit, *offset)
	if err != nil {
		return err
	}
	for _, hit := range resp.Hits {
		fmt.Fprintf(out, "%s\t%.4f\n", hit.Thing, hit.Score)
	}
	fmt.Fprintf(out, "%d of %d\n", len(resp.Hits), resp.Total)
	return nil
}

func appendOp(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle append", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	payload := fs.String("payload", "{}", "the op's payload")
	parents := fs.String("parents", "", "comma-separated parent op IDs")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 3 {
		return fmt.Errorf("append: <log> <thing> <op.type>")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	var opts []client.AppendOpt
	if *parents != "" {
		opts = append(opts, client.WithParents(strings.Split(*parents, ",")...))
	}
	ack, err := c.Append(ctx, pos[0], pos[1], pos[2], []byte(*payload), opts...)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "appended: seq %d (op %s)\n", ack.Seq, ack.OpID)
	return nil
}

func state(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle state", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("state: <log> <thing>")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	sv, err := c.State(ctx, pos[0], pos[1])
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "seq %d\n%s\n", sv.Seq, sv.State)
	return nil
}

func replay(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle replay", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("replay: <log> <thing>")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	ops, err := c.Replay(ctx, pos[0], pos[1])
	if err != nil {
		return err
	}
	for _, op := range ops {
		fmt.Fprintf(out, "seq %d  %s  by %s  op %s\n  %s\n", op.Seq, op.Type, op.Author, op.ID, op.Payload)
	}
	return nil
}

func graphNeighbors(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle graph neighbors", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	direction := fs.String("direction", "out", "out, in, or both")
	label := fs.String("label", "", "filter to one edge label")
	limit := fs.Int("limit", 0, "max edges")
	offset := fs.Int("offset", 0, "skip edges")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 3 {
		return fmt.Errorf("graph neighbors: <log> <index> <thing>")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.GraphNeighbors(ctx, pos[0], pos[1], client.GraphQueryRequest{
		Thing: pos[2], Direction: *direction, Label: *label, Limit: *limit, Offset: *offset,
	})
	if err != nil {
		return err
	}
	for _, e := range resp.Edges {
		fmt.Fprintf(out, "%s -[%s]-> %s\n", e.From, e.Label, e.To)
	}
	fmt.Fprintf(out, "%d of %d\n", len(resp.Edges), resp.Total)
	return nil
}

func graphWalk(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle graph walk", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	direction := fs.String("direction", "out", "out, in, or both")
	labels := fs.String("labels", "", "comma-separated traversable labels")
	depth := fs.Int("depth", 1, "walk depth (capped)")
	limit := fs.Int("limit", 0, "max things")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 3 {
		return fmt.Errorf("graph walk: <log> <index> <thing>")
	}
	var labelList []string
	if *labels != "" {
		labelList = strings.Split(*labels, ",")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.GraphWalk(ctx, pos[0], pos[1], client.GraphQueryRequest{
		Thing: pos[2], Direction: *direction, Labels: labelList, Depth: *depth, Limit: *limit,
	})
	if err != nil {
		return err
	}
	for _, v := range resp.Things {
		fmt.Fprintf(out, "%s\tdepth %d\tvia %s\n", v.Thing, v.Depth, v.Via)
	}
	fmt.Fprintf(out, "%d things", resp.Total)
	if resp.DepthCapped {
		fmt.Fprintf(out, " (depth capped at %d)", contract.GraphWalkMaxDepth)
	}
	if resp.Truncated {
		fmt.Fprint(out, " (truncated)")
	}
	fmt.Fprintln(out)
	return nil
}

func semanticQuery(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle semantic query", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	limit := fs.Int("limit", 0, "max hits")
	offset := fs.Int("offset", 0, "skip hits")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 3 {
		return fmt.Errorf("semantic query: <log> <index> <text...>")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.QuerySemantic(ctx, pos[0], pos[1], strings.Join(pos[2:], " "), *limit, *offset)
	if err != nil {
		return err
	}
	for _, h := range resp.Hits {
		fmt.Fprintf(out, "%s\t%.4f", h.Thing, h.Score)
		if h.Field != "" {
			fmt.Fprintf(out, "\t%s", h.Field)
		}
		fmt.Fprintln(out)
	}
	fmt.Fprintf(out, "%d of %d", len(resp.Hits), resp.Total)
	if resp.Unembedded > 0 {
		fmt.Fprintf(out, " (%d not yet embedded)", resp.Unembedded)
	}
	fmt.Fprintln(out)
	return nil
}
