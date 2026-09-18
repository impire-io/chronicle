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
	"path/filepath"
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
	case "member":
		if len(args) >= 2 && args[1] == "add" {
			return memberAdd(ctx, args[2:], out)
		}
		if len(args) >= 2 && args[1] == "revoke" {
			return memberRevoke(ctx, args[2:], out)
		}
		if len(args) >= 2 && args[1] == "rekey" {
			return memberRekey(ctx, args[2:], out)
		}
		return usage(out)
	case "log":
		if len(args) >= 2 && args[1] == "create" {
			return logCreate(ctx, args[2:], out)
		}
		return usage(out)
	case "type":
		if len(args) >= 2 && args[1] == "define" {
			return typeDefine(ctx, args[2:], out)
		}
		if len(args) >= 2 && args[1] == "inspect" {
			return typeInspect(ctx, args[2:], out)
		}
		if len(args) >= 2 && args[1] == "list" {
			return typeList(ctx, args[2:], out)
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
	case "login":
		return login(ctx, args[1:], out)
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
  chronicle member add <tenant> <principal> [--dir D] [--role R] [--out F]
  chronicle member revoke <tenant> <principal> [--dir D]
  chronicle member rekey <tenant> [--dir D] [--out-dir P]
  chronicle operator rotate-signing-key [--dir D]       rotate the trust root (fleet stopped)
  chronicle operator emit-cluster-config --node <name>=<host>[:cp[:kp]] ... [--dir D] [--out P]
                                                        render the cluster's server configs
  chronicle login [--bridge F]                          log in with GitHub (device flow)
      every client verb below also takes --bridge F --tenant T instead of --creds
  chronicle log create <log> --creds F [--url U] [--desc S] [--history H]
  chronicle type define <log> <type> --creds F --def JSON | --file F
  chronicle type inspect <log> <type> --creds F
  chronicle type list <log> --creds F
  chronicle thing create <log> <thing> --creds F [--state JSON]
  chronicle thing rollup <log> <thing> --creds F
  chronicle index declare <log> <index> --creds F [--kind K] [--config JSON]
  chronicle index delete <log> <index> --creds F
  chronicle index query <log> <index> [query...] --creds F [--limit N] [--offset N]
  chronicle semantic query <log> <index> <text...> --creds F [--limit N] [--offset N]
  chronicle graph neighbors <log> <index> <thing> --creds F [--direction D] [--label L] [--limit N] [--offset N]
  chronicle graph walk <log> <index> <thing> --creds F [--direction D] [--labels a,b] [--depth N] [--limit N]
  chronicle append <log> <thing> <op.type> --creds F [--payload JSON] [--parents a,b] [--expect-seq N]
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

// connectFlags are the flags every tenant-side verb shares. A verb dials
// with --creds (possession is authentication) or through the browser
// bridge with --bridge + --tenant (decision 0026) — never both.
type connectFlags struct {
	url    *string
	creds  *string
	dir    *string
	bridge *string
	tenant *string
}

func addConnectFlags(fs *flag.FlagSet) connectFlags {
	return connectFlags{
		url:    fs.String("url", "", "NATS url (default: the --dir fleet's recorded url)"),
		creds:  fs.String("creds", "", "credentials file"),
		dir:    fs.String("dir", devdir.Default(), "local fleet data dir, used when --url is not given"),
		bridge: fs.String("bridge", "", "bridge profile from the install; dial via GitHub login (see: chronicle login)"),
		tenant: fs.String("tenant", "", "target tenant for a --bridge dial"),
	}
}

func (cf connectFlags) dial() (*client.Client, error) {
	if *cf.bridge != "" {
		if *cf.creds != "" {
			return nil, fmt.Errorf("--creds and --bridge are two ways to be someone; pick one")
		}
		if *cf.tenant == "" {
			return nil, fmt.Errorf("--bridge needs --tenant")
		}
		return bridgeDial(*cf.bridge, *cf.tenant)
	}
	if *cf.creds == "" {
		return nil, fmt.Errorf("--creds is required (or --bridge with --tenant)")
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

// dialControl is the control-plane verbs' shared preamble: the fleet
// dir's recorded url, dialed with its control creds.
func dialControl(dir string) (*client.Control, error) {
	url, err := devdir.ReadClientURL(dir)
	if err != nil {
		return nil, err
	}
	creds, err := os.ReadFile(devdir.ControlCredsPath(dir))
	if err != nil {
		return nil, fmt.Errorf("read control creds: %w", err)
	}
	return client.ConnectControlCreds(url, creds)
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

	nc, err := dialControl(*dir)
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

func memberAdd(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle member add", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "local fleet data dir")
	role := fs.String("role", contract.RoleWriter, "membership role: admin, writer, or reader")
	githubID := fs.Int64("github-id", 0, "bind the membership to a GitHub identity (numeric user id) for the browser bridge")
	outFile := fs.String("out", "", "where to write the member .creds (default <tenant>-<principal>.creds)")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("member add: <tenant> <principal>")
	}

	nc, err := dialControl(*dir)
	if err != nil {
		return err
	}
	defer nc.Close()

	resp, err := nc.AddMember(ctx, pos[0], pos[1], *role, *githubID)
	if err != nil {
		return err
	}
	path := *outFile
	if path == "" {
		path = fmt.Sprintf("%s-%s.creds", pos[0], resp.Principal)
	}
	if err := os.WriteFile(path, resp.Creds, 0o600); err != nil {
		return fmt.Errorf("write member creds: %w", err)
	}
	fmt.Fprintf(out, "member %s added to %s: role %s\n", resp.Principal, pos[0], resp.Role)
	fmt.Fprintf(out, "member creds (the only copy): %s\n", path)
	return nil
}

func memberRevoke(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle member revoke", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "local fleet data dir")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("member revoke: <tenant> <principal>")
	}

	nc, err := dialControl(*dir)
	if err != nil {
		return err
	}
	defer nc.Close()

	resp, err := nc.RevokeMember(ctx, pos[0], pos[1])
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "member %s revoked from %s: user %s is dead on the wire and gone from the registry\n", resp.Principal, pos[0], resp.PublicKey)
	return nil
}

func memberRekey(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle member rekey", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "local fleet data dir")
	outDir := fs.String("out-dir", ".", "where to write the re-issued .creds files")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("member rekey: exactly one tenant name")
	}
	tenant := pos[0]

	nc, err := dialControl(*dir)
	if err != nil {
		return err
	}
	defer nc.Close()

	resp, err := nc.RekeyMembers(ctx, tenant)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "tenant %s rekeyed: every prior member credential is dead\n", tenant)
	for _, m := range resp.Members {
		path := filepath.Join(*outDir, fmt.Sprintf("%s-%s.creds", tenant, m.Principal))
		if err := os.WriteFile(path, m.Creds, 0o600); err != nil {
			return fmt.Errorf("write %s creds: %w", m.Principal, err)
		}
		fmt.Fprintf(out, "member %s (%s) re-issued (the only copy): %s\n", m.Principal, m.Role, path)
	}
	return nil
}

func logCreate(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle log create", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	desc := fs.String("desc", "", "log description")
	history := fs.String("history", "", "history declaration (0019): compactable (default) or preserved")
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
	var opts []client.LogOpt
	if *history != "" {
		opts = append(opts, client.WithHistory(*history))
	}
	resp, err := c.CreateLog(ctx, pos[0], *desc, opts...)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "log %s created: stream %s\n", pos[0], resp.Stream)
	return nil
}

// typeDefinition is the --def / --file JSON: every facet of a type in one
// act (0021). Operations carry each op's payload schema and effect.
type typeDefinition struct {
	Schema     json.RawMessage           `json:"schema"`
	History    string                    `json:"history,omitempty"`
	Aspects    map[string]string         `json:"aspects,omitempty"`
	Operations map[string]contract.OpDef `json:"operations,omitempty"`
}

func typeDefine(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle type define", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	inline := fs.String("def", "", `the type definition, inline: {"schema":…,"history":…,"aspects":…,"operations":…}`)
	file := fs.String("file", "", "the type definition, from a file")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("type define: <log> <type>")
	}
	var raw []byte
	switch {
	case *inline != "" && *file != "":
		return fmt.Errorf("--def and --file are exclusive")
	case *inline != "":
		raw = []byte(*inline)
	case *file != "":
		var err error
		raw, err = os.ReadFile(*file)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("one of --def or --file is required")
	}
	var def typeDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		return fmt.Errorf("decode type definition: %w", err)
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.DefineType(ctx, pos[0], pos[1], client.TypeDefinition{
		Schema:     def.Schema,
		History:    def.History,
		Aspects:    def.Aspects,
		Operations: def.Operations,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "type %s %s: revision %d\n", pos[0], pos[1], resp.Revision)
	return nil
}

func typeInspect(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle type inspect", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("type inspect: <log> <type>")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	rec, err := c.GetType(ctx, pos[0], pos[1])
	if err != nil {
		return err
	}
	pretty, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "%s\n", pretty)
	return nil
}

func typeList(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle type list", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("type list: <log>")
	}
	c, err := cf.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	names, err := c.ListTypes(ctx, pos[0])
	if err != nil {
		return err
	}
	for _, name := range names {
		fmt.Fprintln(out, name)
	}
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
	expectSeq := fs.Int64("expect-seq", -1, "expected-sequence guard: the last op seq observed on the thing (unguarded when absent)")
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
	if *expectSeq >= 0 {
		opts = append(opts, client.WithExpectedSeq(uint64(*expectSeq)))
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
