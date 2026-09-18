// Package cli implements the chronicle CLI verbs over the client package —
// an adapter on the one product surface, never a side door. The grammar is
// decision 0025's: vocabulary nouns get noun-verb, everyday sentences get
// bare verbs, and the connection and working log come from the selected
// context instead of every invocation.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/version"
)

// Run dispatches one CLI invocation (everything except `up` and the
// operator ceremony, which are the composition root's).
func Run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return usage(out)
	}
	sub := ""
	if len(args) >= 2 {
		sub = args[1]
	}
	switch args[0] {
	case "tenant":
		if sub == "create" {
			return tenantCreate(ctx, args[2:], out)
		}
		return usage(out)
	case "member":
		switch sub {
		case "add":
			return memberAdd(ctx, args[2:], out)
		case "revoke":
			return memberRevoke(ctx, args[2:], out)
		case "rekey":
			return memberRekey(ctx, args[2:], out)
		}
		return usage(out)
	case "context":
		switch sub {
		case "save":
			return contextSave(args[2:], out)
		case "select":
			return contextSelect(args[2:], out)
		case "list":
			return contextList(out)
		case "show":
			return contextShow(args[2:], out)
		case "rm":
			return contextRm(args[2:], out)
		}
		return usage(out)
	case "log":
		switch sub {
		case "create":
			return logCreate(ctx, args[2:], out)
		case "select":
			return logSelect(ctx, args[2:], out)
		case "list":
			return logList(ctx, args[2:], out)
		}
		return usage(out)
	case "type":
		switch sub {
		case "init":
			return typeInit(args[2:], out)
		case "define":
			return typeDefine(ctx, args[2:], out)
		case "inspect":
			return typeInspect(ctx, args[2:], out)
		case "list":
			return typeList(ctx, args[2:], out)
		}
		return usage(out)
	case "operation", "op":
		switch sub {
		case "define":
			return opDefine(ctx, args[2:], out)
		case "list":
			return opList(ctx, args[2:], out)
		case "inspect":
			return opInspect(ctx, args[2:], out)
		case "rm":
			return opRm(ctx, args[2:], out)
		}
		return usage(out)
	case "index":
		switch sub {
		case "declare":
			return indexDeclare(ctx, args[2:], out)
		case "delete":
			return indexDelete(ctx, args[2:], out)
		case "list":
			return indexList(ctx, args[2:], out)
		}
		return usage(out)
	case "login":
		return login(ctx, args[1:], out)
	case "create":
		return createThing(ctx, args[1:], out)
	case "do":
		return doOperation(ctx, args[1:], out)
	case "get":
		return getState(ctx, args[1:], out)
	case "history":
		return history(ctx, args[1:], out)
	case "rollup":
		return rollup(ctx, args[1:], out)
	case "query":
		return query(ctx, args[1:], out)
	case "things":
		return things(ctx, args[1:], out)
	case "version":
		fmt.Fprintln(out, version.Version)
		return nil
	default:
		return usage(out)
	}
}

func usage(out io.Writer) error {
	fmt.Fprint(out, `chronicle — ops-logs as a product

run a fleet
  chronicle up [--dir D] [--port N]
  chronicle operator rotate-signing-key [--dir D]        the trust root, fleet stopped
  chronicle operator emit-cluster-config --node <name>=<host>[:cp[:kp]] ... [--dir D] [--out P]

own its tenants (fleet dir)
  chronicle tenant create <name> [--admin P] [--out F]   mints and selects the admin's context
  chronicle member add <tenant> <principal> [--role R] [--out F]
  chronicle member revoke <tenant> <principal>
  chronicle member rekey <tenant> [--out-dir P]

define vocabulary (your context)
  chronicle login [--bridge F]                           log in with GitHub (device flow)
      every context verb below also takes --bridge F --tenant T instead of creds
  chronicle context save <name> --creds F [--url U]
  chronicle context select <name> | show | list | rm <name>
  chronicle log create <log> [--desc S] [--history H]    creates and selects the working log
  chronicle log select <log> | list
  chronicle type init [<type>]                           print a definition skeleton
  chronicle type define <type> --file F | --def JSON
  chronicle type inspect <type> [--json] | list
  chronicle op define <type> <operation> --schema S [--effect E]      op = operation
  chronicle op list <type> | inspect <type> <operation> | rm <type> <operation>
  chronicle index declare <index> [--kind K] [--config JSON]
  chronicle index delete <index> | list

work with things
  chronicle create <thing> [--payload JSON] [--op O]     birth through the type's create operation
  chronicle do <thing> <operation> [--payload JSON] [--parents a,b] [--expect-seq N]
  chronicle get <thing>
  chronicle history <thing>
  chronicle rollup <thing>

find things
  chronicle query <index> [text...] [--from T] [--depth N] [--limit N] [--offset N]
  chronicle things [prefix]

chronicle version
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

// connectFlags are the flags every tenant-plane verb shares. Resolution is
// field-wise (0025): explicit flags beat CHRONICLE_CONTEXT / CHRONICLE_LOG
// beat the selected context; a missing url falls back to the --dir fleet's
// recorded url. The browser bridge (decision 0026) stands beside it: a
// verb dials with creds (possession is authentication) or through the
// bridge with --bridge + --tenant — never both.
type connectFlags struct {
	url     *string
	creds   *string
	dir     *string
	logName *string
	ctxName *string
	bridge  *string
	tenant  *string
}

func addConnectFlags(fs *flag.FlagSet) connectFlags {
	return connectFlags{
		url:     fs.String("url", "", "NATS url (default: the context's, else the --dir fleet's recorded url)"),
		creds:   fs.String("creds", "", "credentials file (default: the context's)"),
		dir:     fs.String("dir", devdir.Default(), "local fleet data dir, the url fallback"),
		logName: fs.String("log", "", "the log to speak to (default: CHRONICLE_LOG, else the selected log)"),
		ctxName: fs.String("context", "", "context name (default: CHRONICLE_CONTEXT, else the selection)"),
		bridge:  fs.String("bridge", "", "bridge profile from the install; dial via GitHub login (see: chronicle login)"),
		tenant:  fs.String("tenant", "", "target tenant for a --bridge dial"),
	}
}

// contextName is the context an invocation addresses: the flag, else the
// selection. Empty means none.
func (cf connectFlags) contextName(root string) string {
	if *cf.ctxName != "" {
		return *cf.ctxName
	}
	return currentContextName(root)
}

// resolved is one invocation's effective connection.
type resolved struct {
	url    string
	creds  string
	log    string // may be empty; verbs that need one call needLog
	bridge string
	tenant string
}

func (cf connectFlags) resolve() (resolved, error) {
	root, err := configRoot()
	if err != nil {
		return resolved{}, err
	}
	var sc storedContext
	if name := cf.contextName(root); name != "" {
		sc, err = loadStoredContext(root, name)
		if err != nil {
			return resolved{}, err
		}
	}
	r := resolved{url: *cf.url, creds: *cf.creds, log: *cf.logName, bridge: *cf.bridge, tenant: *cf.tenant}
	if r.bridge != "" && r.creds != "" {
		return resolved{}, fmt.Errorf("--creds and --bridge are two ways to be someone; pick one")
	}
	if r.creds == "" && r.bridge == "" {
		r.creds = sc.Creds
	}
	if r.creds == "" && r.bridge == "" {
		return resolved{}, errNoCreds
	}
	if r.log == "" {
		r.log = os.Getenv("CHRONICLE_LOG")
	}
	if r.log == "" {
		r.log = sc.Log
	}
	if r.url == "" {
		r.url = sc.URL
	}
	if r.url == "" && r.bridge == "" {
		r.url, err = devdir.ReadClientURL(*cf.dir)
		if err != nil {
			return resolved{}, err
		}
	}
	return r, nil
}

func (r resolved) needLog() (string, error) {
	if r.log == "" {
		return "", errNoLog
	}
	return r.log, nil
}

func (r resolved) dial() (*client.Client, error) {
	if r.bridge != "" {
		if r.tenant == "" {
			return nil, fmt.Errorf("--bridge needs --tenant")
		}
		return bridgeDial(r.bridge, r.tenant)
	}
	return client.ConnectFile(r.url, r.creds)
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

// saveAndSelectContext stores a freshly minted principal's context and
// selects it, so onboarding ends connected (0025 § 1). The mint already
// happened — a store failure is reported, never fatal.
func saveAndSelectContext(out io.Writer, dir, tenant, principal, credsPath string) {
	root, err := configRoot()
	if err != nil {
		fmt.Fprintf(out, "context not saved: %v\n", err)
		return
	}
	abs, err := filepath.Abs(credsPath)
	if err != nil {
		abs = credsPath
	}
	url, _ := devdir.ReadClientURL(dir) // best effort; empty falls back to --dir at use
	name := tenant + "-" + principal
	if err := saveStoredContext(root, name, storedContext{URL: url, Creds: abs}); err != nil {
		fmt.Fprintf(out, "context not saved: %v\n", err)
		return
	}
	if err := selectStoredContext(root, name); err != nil {
		fmt.Fprintf(out, "context %s saved, not selected: %v\n", name, err)
		return
	}
	fmt.Fprintf(out, "context %s saved and selected\n", name)
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
	saveAndSelectContext(out, *dir, name, resp.Admin, path)
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
	saveAndSelectContext(out, *dir, pos[0], resp.Principal, path)
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

func contextSave(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle context save", flag.ContinueOnError)
	fs.SetOutput(out)
	creds := fs.String("creds", "", "credentials file (required)")
	url := fs.String("url", "", "NATS url (default: the --dir fleet's recorded url at use)")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("context save: exactly one context name")
	}
	if *creds == "" {
		return fmt.Errorf("context save: --creds is required")
	}
	root, err := configRoot()
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(*creds)
	if err != nil {
		return err
	}
	// A re-save is field-wise: the url and the selected log survive
	// unless replaced — a rekey swaps the creds, not the connection.
	sc, _ := loadStoredContext(root, pos[0])
	sc.Creds = abs
	if *url != "" {
		sc.URL = *url
	}
	if err := saveStoredContext(root, pos[0], sc); err != nil {
		return err
	}
	fmt.Fprintf(out, "context %s saved\n", pos[0])
	if currentContextName(root) == "" {
		fmt.Fprintf(out, "select it: chronicle context select %s\n", pos[0])
	}
	return nil
}

func contextSelect(args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("context select: exactly one context name")
	}
	root, err := configRoot()
	if err != nil {
		return err
	}
	if err := selectStoredContext(root, args[0]); err != nil {
		return err
	}
	sc, err := loadStoredContext(root, args[0])
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "context %s selected\n", args[0])
	if sc.Log != "" {
		fmt.Fprintf(out, "working log: %s\n", sc.Log)
	}
	return nil
}

func contextList(out io.Writer) error {
	root, err := configRoot()
	if err != nil {
		return err
	}
	names, err := listStoredContexts(root)
	if err != nil {
		return err
	}
	current := currentContextName(root)
	for _, name := range names {
		marker := " "
		if name == current {
			marker = "*"
		}
		fmt.Fprintf(out, "%s %s\n", marker, name)
	}
	return nil
}

func contextShow(args []string, out io.Writer) error {
	root, err := configRoot()
	if err != nil {
		return err
	}
	var name string
	switch len(args) {
	case 0:
		name = currentContextName(root)
	case 1:
		name = args[0]
	default:
		return fmt.Errorf("context show: at most one context name")
	}
	if name == "" {
		return errNoContext
	}
	sc, err := loadStoredContext(root, name)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "context: %s\n", name)
	if sc.URL != "" {
		fmt.Fprintf(out, "url:     %s\n", sc.URL)
	} else {
		fmt.Fprintf(out, "url:     (the --dir fleet's recorded url)\n")
	}
	fmt.Fprintf(out, "creds:   %s\n", sc.Creds)
	if sc.Log != "" {
		fmt.Fprintf(out, "log:     %s\n", sc.Log)
	} else {
		fmt.Fprintf(out, "log:     (none selected — chronicle log select <log>)\n")
	}
	return nil
}

func contextRm(args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("context rm: exactly one context name")
	}
	root, err := configRoot()
	if err != nil {
		return err
	}
	if err := removeStoredContext(root, args[0]); err != nil {
		return err
	}
	fmt.Fprintf(out, "context %s removed\n", args[0])
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
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	c, err := r.dial()
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

	// The log just made becomes the working log (0025 § 1) — when there
	// is a context to remember it on.
	root, rerr := configRoot()
	if rerr != nil {
		return nil
	}
	if name := cf.contextName(root); name != "" {
		if err := updateSelectedLog(root, name, pos[0]); err == nil {
			fmt.Fprintf(out, "selected as the working log\n")
		}
	} else {
		fmt.Fprintf(out, "no context selected, so no working log recorded — chronicle context save <name> --creds F\n")
	}
	return nil
}

func logSelect(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle log select", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("log select: exactly one log name")
	}
	root, err := configRoot()
	if err != nil {
		return err
	}
	name := cf.contextName(root)
	if name == "" {
		return errNoContext
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	logs, err := c.ListLogs(ctx)
	if err != nil {
		return err
	}
	found := false
	for _, l := range logs {
		if l == pos[0] {
			found = true
			break
		}
	}
	if !found {
		if len(logs) == 0 {
			return fmt.Errorf("log %q not found — no logs exist yet (chronicle log create <log>)", pos[0])
		}
		return fmt.Errorf("log %q not found — logs: %s", pos[0], strings.Join(logs, ", "))
	}
	if err := updateSelectedLog(root, name, pos[0]); err != nil {
		return err
	}
	fmt.Fprintf(out, "working log: %s (context %s)\n", pos[0], name)
	return nil
}

func logList(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle log list", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	if _, err := parseArgs(fs, args); err != nil {
		return err
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	logs, err := c.ListLogs(ctx)
	if err != nil {
		return err
	}
	for _, l := range logs {
		if l == r.log {
			fmt.Fprintf(out, "%s (selected)\n", l)
			continue
		}
		fmt.Fprintln(out, l)
	}
	return nil
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
	if len(pos) != 1 {
		return fmt.Errorf("type define: exactly one type name")
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
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.DefineType(ctx, log, pos[0], client.TypeDefinition{
		Schema:     def.Schema,
		History:    def.History,
		Aspects:    def.Aspects,
		Operations: def.Operations,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "type %s in %s: revision %d\n", pos[0], log, resp.Revision)
	echoDefinition(out, def)
	return nil
}

func typeInspect(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle type inspect", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	asJSON := fs.Bool("json", false, "print the raw record")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("type inspect: exactly one type name")
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	rec, err := c.GetType(ctx, log, pos[0])
	if err != nil {
		return err
	}
	if *asJSON {
		pretty, err := json.MarshalIndent(rec, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "%s\n", pretty)
		return nil
	}
	printTypeRecord(out, pos[0], rec)
	return nil
}

func typeList(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle type list", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	if _, err := parseArgs(fs, args); err != nil {
		return err
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	names, err := c.ListTypes(ctx, log)
	if err != nil {
		return err
	}
	for _, name := range names {
		fmt.Fprintln(out, name)
	}
	return nil
}

// loadType is the operation verbs' shared preamble: the connection, the
// working log, and the type record the facet edit starts from.
func loadType(ctx context.Context, cf connectFlags, typeName string) (*client.Client, string, contract.TypeRecord, error) {
	r, err := cf.resolve()
	if err != nil {
		return nil, "", contract.TypeRecord{}, err
	}
	log, err := r.needLog()
	if err != nil {
		return nil, "", contract.TypeRecord{}, err
	}
	c, err := r.dial()
	if err != nil {
		return nil, "", contract.TypeRecord{}, err
	}
	rec, err := c.GetType(ctx, log, typeName)
	if err != nil {
		c.Close()
		if errors.Is(err, client.ErrNoType) {
			return nil, "", contract.TypeRecord{}, fmt.Errorf("%w — chronicle type define %s", err, typeName)
		}
		return nil, "", contract.TypeRecord{}, err
	}
	return c, log, rec, nil
}

// redefine writes the type back whole — 0021's one act, composed by the
// CLI (0025 § 4). The node bumps the revision; two racing vocabulary
// edits resolve by latest-declaration-wins.
func redefine(ctx context.Context, c *client.Client, log, typeName string, rec contract.TypeRecord) (uint64, error) {
	resp, err := c.DefineType(ctx, log, typeName, client.TypeDefinition{
		Schema:     rec.Schema,
		History:    rec.History,
		Aspects:    rec.Aspects,
		Operations: rec.Operations,
	})
	if err != nil {
		return 0, err
	}
	return resp.Revision, nil
}

func opDefine(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operation define", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	schema := fs.String("schema", "", "the payload's JSON Schema: inline, or a file path (required)")
	effect := fs.String("effect", "", "the op's effect: merge or none (default none)")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("operation define: <type> <operation>")
	}
	if *schema == "" {
		return fmt.Errorf("operation define: --schema is required")
	}
	if *effect != "" && !contract.KnownEffect(*effect) {
		return fmt.Errorf("operation define: unknown effect %q (want %s, %s)", *effect, contract.EffectMerge, contract.EffectNone)
	}
	raw, err := schemaArg(*schema)
	if err != nil {
		return err
	}
	c, log, rec, err := loadType(ctx, cf, pos[0])
	if err != nil {
		return err
	}
	defer c.Close()

	if rec.Operations == nil {
		rec.Operations = map[string]contract.OpDef{}
	}
	// The consequence, said before the write (0025 § 4): a changed
	// effect makes derived state suspect and the node rebuilds.
	if old, ok := rec.Operations[pos[1]]; ok &&
		contract.NormalizeEffect(old.Effect) != contract.NormalizeEffect(*effect) {
		fmt.Fprintf(out, "effect changes %s → %s: derived state is suspect, the node rebuilds %s's views\n",
			contract.NormalizeEffect(old.Effect), contract.NormalizeEffect(*effect), log)
	}
	rec.Operations[pos[1]] = contract.OpDef{Schema: raw, Effect: *effect}
	revision, err := redefine(ctx, c, log, pos[0], rec)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "operation %s on %s: effect %s, revision %d\n",
		pos[1], pos[0], contract.NormalizeEffect(*effect), revision)
	return nil
}

func opList(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operation list", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("operation list: exactly one type name")
	}
	c, _, rec, err := loadType(ctx, cf, pos[0])
	if err != nil {
		return err
	}
	defer c.Close()
	printOperations(out, rec.Operations)
	return nil
}

func opInspect(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operation inspect", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("operation inspect: <type> <operation>")
	}
	c, _, rec, err := loadType(ctx, cf, pos[0])
	if err != nil {
		return err
	}
	defer c.Close()
	def, ok := rec.Operations[pos[1]]
	if !ok {
		return fmt.Errorf("type %q defines no operation %q — operations: %s",
			pos[0], pos[1], strings.Join(operationNames(rec.Operations), ", "))
	}
	fmt.Fprintf(out, "operation %s on %s — effect %s\n", pos[1], pos[0], contract.NormalizeEffect(def.Effect))
	pretty, err := json.MarshalIndent(def.Schema, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "%s\n", pretty)
	return nil
}

func opRm(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operation rm", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("operation rm: <type> <operation>")
	}
	c, log, rec, err := loadType(ctx, cf, pos[0])
	if err != nil {
		return err
	}
	defer c.Close()
	if _, ok := rec.Operations[pos[1]]; !ok {
		return fmt.Errorf("type %q defines no operation %q — operations: %s",
			pos[0], pos[1], strings.Join(operationNames(rec.Operations), ", "))
	}
	// The consequence, said before the write (0025 § 4): history is
	// never touched; the record's tolerance re-judges it.
	fmt.Fprintf(out, "history keeps its %s ops; they re-fold as effect none with a warning\n", pos[1])
	delete(rec.Operations, pos[1])
	revision, err := redefine(ctx, c, log, pos[0], rec)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "operation %s removed from %s: revision %d\n", pos[1], pos[0], revision)
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
	if len(pos) != 1 {
		return fmt.Errorf("index declare: exactly one index name")
	}
	var cfgRaw json.RawMessage
	if *config != "" {
		cfgRaw = json.RawMessage(*config)
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.DeclareIndex(ctx, log, pos[0], *kind, cfgRaw)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "index %s/%s declared (%s): query %s\n", log, pos[0], *kind, resp.Query)
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
	if len(pos) != 1 {
		return fmt.Errorf("index delete: exactly one index name")
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.DeleteIndex(ctx, log, pos[0]); err != nil {
		return err
	}
	fmt.Fprintf(out, "index %s/%s retired\n", log, pos[0])
	return nil
}

func indexList(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle index list", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	if _, err := parseArgs(fs, args); err != nil {
		return err
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	infos, err := c.ListIndexes(ctx, log)
	if err != nil {
		return err
	}
	for _, info := range infos {
		if info.Kind == contract.IndexKindState {
			fmt.Fprintf(out, "%s\t%s\t(born with the log, undeletable)\n", info.Name, info.Kind)
			continue
		}
		fmt.Fprintf(out, "%s\t%s\n", info.Name, info.Kind)
	}
	return nil
}

func createThing(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle create", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	payload := fs.String("payload", "{}", "the constructor's payload (an untyped thing's birth state)")
	opName := fs.String("op", "", "the constructor operation (default: create)")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("create: exactly one thing")
	}
	thing := pos[0]
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	res, err := c.Resolve(ctx, log, thing)
	if err != nil {
		return err
	}
	switch res.Kind {
	case contract.ResolvedTyped:
		// Create is an operation (0025 § 3): the constructor is `create`
		// by convention, published with the birth guard.
		op := *opName
		if op == "" {
			op = "create"
		}
		ack, err := c.CreateWith(ctx, log, thing, op, []byte(*payload))
		if err != nil {
			if errors.Is(err, client.ErrUndefinedOperation) {
				return fmt.Errorf("%w\noperations on %s: %s\n(--op <operation> picks the constructor)",
					err, res.TypeName, strings.Join(operationNames(res.Record.Operations), ", "))
			}
			return err
		}
		fmt.Fprintf(out, "born: %s at seq %d (op %s, via %s)\n", thing, ack.Seq, ack.OpID, op)
	case contract.ResolvedUndeclared:
		return fmt.Errorf("undeclared aspect: %s", res.Detail)
	default:
		// An untyped tail keeps the raw snapshot birth; the payload is
		// its birth state.
		ack, err := c.CreateThing(ctx, log, thing, json.RawMessage(*payload))
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "born: %s at seq %d (op %s)\n", thing, ack.Seq, ack.OpID)
	}
	return nil
}

func doOperation(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle do", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	payload := fs.String("payload", "{}", "the op's payload")
	parents := fs.String("parents", "", "comma-separated parent op IDs")
	expectSeq := fs.Int64("expect-seq", -1, "expected-sequence guard: the last op seq observed on the thing (unguarded when absent)")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("do: <thing> <operation>")
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
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
	ack, err := c.Append(ctx, log, pos[0], pos[1], []byte(*payload), opts...)
	if err != nil {
		// The refusal teaches (0025 § 5): what the type does define.
		if errors.Is(err, client.ErrUndefinedOperation) {
			if res, rerr := c.Resolve(ctx, log, pos[0]); rerr == nil && res.Kind == contract.ResolvedTyped {
				return fmt.Errorf("%w\noperations on %s: %s",
					err, res.TypeName, strings.Join(operationNames(res.Record.Operations), ", "))
			}
		}
		return err
	}
	fmt.Fprintf(out, "done: seq %d (op %s)\n", ack.Seq, ack.OpID)
	return nil
}

func getState(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle get", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("get: exactly one thing")
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	sv, err := c.State(ctx, log, pos[0])
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "seq %d\n%s\n", sv.Seq, sv.State)
	return nil
}

func history(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle history", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("history: exactly one thing")
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	ops, err := c.Replay(ctx, log, pos[0])
	if err != nil {
		return err
	}
	for _, op := range ops {
		fmt.Fprintf(out, "seq %d  %s  by %s  op %s\n  %s\n", op.Seq, op.Type, op.Author, op.ID, op.Payload)
	}
	return nil
}

func rollup(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle rollup", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("rollup: exactly one thing")
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.RollupThing(ctx, log, pos[0])
	if err != nil {
		return err
	}
	// Declining is an answer, not a failure: the node names its reason.
	if !resp.Rolled {
		fmt.Fprintf(out, "not compacted: %s\n", resp.Reason)
		return nil
	}
	fmt.Fprintf(out, "compacted: %s at seq %d\n", pos[0], resp.Seq)
	return nil
}

func query(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle query", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	from := fs.String("from", "", "graph: the thing to start from")
	depth := fs.Int("depth", 0, "graph: walk this deep (absent: neighbors)")
	direction := fs.String("direction", "out", "graph: out, in, or both")
	label := fs.String("label", "", "graph neighbors: filter to one edge label")
	labels := fs.String("labels", "", "graph walk: comma-separated traversable labels")
	limit := fs.Int("limit", 0, "max hits (default 10, cap 100)")
	offset := fs.Int("offset", 0, "hits to skip")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return fmt.Errorf("query: <index> [text...]")
	}
	index := pos[0]
	text := strings.Join(pos[1:], " ")

	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	// The declared kind shapes the query (0025 § 6).
	decl, err := c.GetIndexDeclaration(ctx, log, index)
	if err != nil {
		if errors.Is(err, client.ErrNoIndex) {
			if infos, lerr := c.ListIndexes(ctx, log); lerr == nil && len(infos) > 0 {
				names := make([]string, 0, len(infos))
				for _, info := range infos {
					names = append(names, info.Name)
				}
				return fmt.Errorf("%w — declared indexes: %s", err, strings.Join(names, ", "))
			}
		}
		return err
	}
	switch decl.Kind {
	case contract.IndexKindSearch:
		if text == "" {
			fmt.Fprintf(out, "index %s is a search index: chronicle query %s <text...>\n", index, index)
			return nil
		}
		resp, err := c.QueryIndex(ctx, log, index, text, *limit, *offset)
		if err != nil {
			return err
		}
		for _, hit := range resp.Hits {
			fmt.Fprintf(out, "%s\t%.4f\n", hit.Thing, hit.Score)
		}
		fmt.Fprintf(out, "%d of %d\n", len(resp.Hits), resp.Total)
	case contract.IndexKindSemantic:
		if text == "" {
			fmt.Fprintf(out, "index %s is a semantic index: chronicle query %s <text...>\n", index, index)
			return nil
		}
		resp, err := c.QuerySemantic(ctx, log, index, text, *limit, *offset)
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
	case contract.IndexKindGraph:
		if *from == "" {
			fmt.Fprintf(out, "index %s is a graph index: chronicle query %s --from <thing> [--depth N] [--direction D] [--label L | --labels a,b]\n", index, index)
			return nil
		}
		// The depth argument decides the form (0025 § 6): absent is
		// neighbors, present is walk.
		if *depth > 0 {
			var labelList []string
			if *labels != "" {
				labelList = strings.Split(*labels, ",")
			}
			resp, err := c.GraphWalk(ctx, log, index, client.GraphQueryRequest{
				Thing: *from, Direction: *direction, Labels: labelList, Depth: *depth, Limit: *limit,
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
		resp, err := c.GraphNeighbors(ctx, log, index, client.GraphQueryRequest{
			Thing: *from, Direction: *direction, Label: *label, Limit: *limit, Offset: *offset,
		})
		if err != nil {
			return err
		}
		for _, e := range resp.Edges {
			fmt.Fprintf(out, "%s -[%s]-> %s\n", e.From, e.Label, e.To)
		}
		fmt.Fprintf(out, "%d of %d\n", len(resp.Edges), resp.Total)
	case contract.IndexKindState:
		return fmt.Errorf("the state index is read with: chronicle get <thing>")
	default:
		return fmt.Errorf("index %s has kind %q this build cannot query", index, decl.Kind)
	}
	return nil
}

func things(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle things", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := addConnectFlags(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) > 1 {
		return fmt.Errorf("things: at most one prefix")
	}
	prefix := ""
	if len(pos) == 1 {
		prefix = pos[0]
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	log, err := r.needLog()
	if err != nil {
		return err
	}
	c, err := r.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	names, err := c.ListThings(ctx, log, prefix)
	if err != nil {
		return err
	}
	for _, name := range names {
		fmt.Fprintln(out, name)
	}
	fmt.Fprintf(out, "%d things\n", len(names))
	return nil
}

// operationNames is the teaching list: a refusal that names an unknown
// operation says what the type does define.
func operationNames(ops map[string]contract.OpDef) []string {
	if len(ops) == 0 {
		return []string{"(none)"}
	}
	names := make([]string, 0, len(ops))
	for name := range ops {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
