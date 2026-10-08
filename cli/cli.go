// Package cli implements the chronicle CLI over the client package — an
// adapter on the one product surface, never a side door. The grammar is
// decision 0045's: one shape, noun then verb, for every sentence
// (chronicle-hq/02-DESIGN/07-the-cli.md), in the vocabulary of decision
// 0044 (chronicle-hq/00-META/vocabulary.md). The connection and the
// selected store come from a context instead of every invocation. The
// package carries the account sentences and signing in through an
// install's identity bridge — `login` and `account create` (decision 0043)
// — the open form (11-the-two-forms.md § the managed CLI); a build that
// owns more, the managed service's, adds its verbs through an Extension
// and ships the same binary as a superset.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/devdir"
	"github.com/impire-io/chronicle/internal/version"
)

// Verb is one top-level verb's handler: the arguments after the verb, and
// the writer every line goes to.
type Verb func(ctx context.Context, args []string, out io.Writer) error

// Extension is what a build adds to the open CLI. The open binary itself
// adds `up`; the managed build adds the operator's verbs and the
// operator's forms of `account`, `member` and `service-account`. Nothing
// here changes an account sentence.
type Extension struct {
	// Verbs are the top-level verbs the build adds, dispatched before the
	// open sentences by name; a name the open grammar already uses is
	// refused at run.
	Verbs map[string]Verb
	// Override wraps an open noun with the build's own handling, the open
	// handler passed in to delegate to: the managed build's `member` takes
	// `<account> <principal>` and issues a credential, and hands anything
	// else to the open sentence. A name outside the open grammar is
	// refused at run.
	Override map[string]func(open Verb) Verb
	// Usage is the help for the added verbs — one line per verb, printed
	// as its own section of the root help.
	Usage string
}

// ErrUsage is returned when a sentence is not in the grammar: the help was
// printed, and the exit status is 1. Asking for help (-h, --help, `help`)
// prints the same text and returns nil.
var ErrUsage = errors.New("usage")

// Run dispatches one CLI invocation with no extension: the open grammar.
func Run(ctx context.Context, args []string, out io.Writer) error {
	return RunWith(ctx, args, out, nil)
}

// RunWith dispatches one CLI invocation with a build's extension. A help
// request anywhere in the sentence prints the help and returns nil.
func RunWith(ctx context.Context, args []string, out io.Writer, ext *Extension) error {
	return done(runWith(ctx, args, out, ext))
}

func runWith(ctx context.Context, args []string, out io.Writer, ext *Extension) error {
	x := &runner{ext: ext}
	nouns := x.nouns()
	open := map[string]Verb{}
	for _, n := range nouns {
		open[n.name] = n.verb(x)
	}
	for name, v := range x.sessionVerbs() {
		open[name] = v
	}
	if ext != nil {
		for name := range ext.Verbs {
			if _, taken := open[name]; taken {
				return fmt.Errorf("extension verb %q shadows the open grammar", name)
			}
		}
		for name, wrap := range ext.Override {
			verb, ok := open[name]
			if !ok {
				return fmt.Errorf("extension overrides %q, which the open grammar does not have", name)
			}
			open[name] = wrap(verb)
		}
	}
	if len(args) == 0 || wantsHelp(args[:1]) {
		x.rootHelp(out)
		return nil
	}
	if args[0] == "help" {
		if len(args) == 1 {
			x.rootHelp(out)
			return nil
		}
		if n := x.noun(args[1]); n != nil {
			n.help(out)
			return nil
		}
		if _, ok := open[args[1]]; ok {
			return open[args[1]](ctx, []string{"--help"}, out)
		}
		x.rootHelp(out)
		return fmt.Errorf("%w: %q is not a chronicle command", ErrUsage, args[1])
	}
	if ext != nil {
		if verb, ok := ext.Verbs[args[0]]; ok {
			return verb(ctx, args[1:], out)
		}
	}
	if verb, ok := open[args[0]]; ok {
		return verb(ctx, args[1:], out)
	}
	x.rootHelp(out)
	return fmt.Errorf("%w: %q is not a chronicle command", ErrUsage, args[0])
}

// runner is one invocation's dispatch: the extension it carries decides
// how a sentence may dial.
type runner struct {
	ext *Extension
}

// wantsHelp says whether the arguments ask for help anywhere.
func wantsHelp(args []string) bool {
	for _, a := range args {
		if a == "-h" || a == "--help" || a == "-help" {
			return true
		}
	}
	return false
}

// command is one verb of a noun: its usage line, one-line summary, and
// handler. The handler receives the arguments after the verb.
type command struct {
	use     string
	summary string
	run     func(ctx context.Context, args []string, out io.Writer) error
}

// noun groups the verbs that act on one kind of object. The order of
// verbs is the order help prints them.
type noun struct {
	name    string
	summary string
	verbs   []command
}

func (n *noun) verb(_ *runner) Verb {
	return func(ctx context.Context, args []string, out io.Writer) error {
		if len(args) == 0 || wantsHelp(args[:1]) {
			n.help(out)
			if len(args) == 0 {
				return fmt.Errorf("%w: chronicle %s needs a verb", ErrUsage, n.name)
			}
			return nil
		}
		for _, c := range n.verbs {
			if verbName(c.use) == args[0] {
				return c.run(ctx, args[1:], out)
			}
		}
		n.help(out)
		return fmt.Errorf("%w: %q is not a verb of chronicle %s", ErrUsage, args[0], n.name)
	}
}

// verbName is the verb a usage line starts with: "list [flags]" → "list".
func verbName(use string) string {
	if i := strings.IndexByte(use, ' '); i >= 0 {
		return use[:i]
	}
	return use
}

func (n *noun) help(out io.Writer) {
	fmt.Fprintf(out, "chronicle %s — %s\n\nVerbs:\n", n.name, n.summary)
	width := 0
	for _, c := range n.verbs {
		if len(c.use) > width {
			width = len(c.use)
		}
	}
	for _, c := range n.verbs {
		fmt.Fprintf(out, "  chronicle %s %-*s  %s\n", n.name, width, c.use, c.summary)
	}
	if n.name == "context" {
		// The connection flags are documented once, here (0045 § 4).
		fmt.Fprint(out, "\n", connectionHelp)
		return
	}
	fmt.Fprintf(out, "\nRun \"chronicle %s <verb> --help\" for a verb's flags. Connection flags: chronicle context --help.\n", n.name)
}

// nouns is the open grammar, in help order.
func (x *runner) nouns() []*noun {
	return []*noun{
		x.storeNoun(), x.typeNoun(), x.opNoun(), x.instanceNoun(), x.indexNoun(),
		x.memberNoun(), x.serviceAccountNoun(), x.contextNoun(), x.accountNoun(),
	}
}

func (x *runner) noun(name string) *noun {
	for _, n := range x.nouns() {
		if n.name == name {
			return n
		}
	}
	return nil
}

// sessionVerbs are the words about the session, outside the noun-verb
// grammar: login, logout, version.
func (x *runner) sessionVerbs() map[string]Verb {
	return map[string]Verb{
		"login":  login,
		"logout": logout,
		"version": func(_ context.Context, args []string, out io.Writer) error {
			if wantsHelp(args) {
				fmt.Fprintln(out, "chronicle version — print this binary's version")
				return nil
			}
			fmt.Fprintln(out, version.Version)
			return nil
		},
	}
}

func (x *runner) rootHelp(out io.Writer) {
	fmt.Fprint(out, "chronicle — keeps the full history of everything your service does, and the state, search and relationships derived from it\n\n")
	fmt.Fprint(out, "Your account:\n")
	for _, n := range x.nouns() {
		fmt.Fprintf(out, "  %-16s %s\n", n.name, n.summary)
	}
	fmt.Fprint(out, "\nYour session:\n")
	fmt.Fprintf(out, "  %-16s %s\n", "login", "sign in with GitHub; ends inside your account")
	fmt.Fprintf(out, "  %-16s %s\n", "logout", "forget the sign-in the selected context uses")
	fmt.Fprintf(out, "  %-16s %s\n", "version", "print this binary's version")
	if x.ext != nil && x.ext.Usage != "" {
		fmt.Fprint(out, "\n", strings.TrimRight(x.ext.Usage, "\n"), "\n")
	}
	fmt.Fprint(out, "\nRun \"chronicle help <noun>\" or \"chronicle <noun> --help\" for its verbs. Connection flags: chronicle context --help.\n")
}

// flags opens a command's flag set with the usage its help prints.
func flags(name, use, summary string, out io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("chronicle "+name, flag.ContinueOnError)
	fs.SetOutput(out)
	fs.Usage = func() {
		fmt.Fprintf(out, "Usage: chronicle %s %s\n  %s\n", name, strings.TrimPrefix(use, verbName(use)+" "), summary)
		printFlags(out, fs)
	}
	return fs
}

// printFlags prints a command's own flags; the connection flags are
// documented once, under `context --help`.
func printFlags(out io.Writer, fs *flag.FlagSet) {
	var own []*flag.Flag
	fs.VisitAll(func(f *flag.Flag) {
		if !connectionFlagNames[f.Name] {
			own = append(own, f)
		}
	})
	if len(own) == 0 {
		return
	}
	fmt.Fprintln(out, "\nFlags:")
	for _, f := range own {
		def := ""
		if f.DefValue != "" && f.DefValue != "false" && f.DefValue != "0" {
			def = fmt.Sprintf(" (default %s)", f.DefValue)
		}
		fmt.Fprintf(out, "  --%s\n      %s%s\n", f.Name, f.Usage, def)
	}
}

// parse parses flags and positionals interleaved — the stdlib flag package
// stops at the first positional, so keep popping positionals and
// re-parsing until everything is consumed. Asking for help prints the
// usage and returns errHelp, which the caller turns into a clean exit.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	if wantsHelp(args) {
		fs.Usage()
		return nil, errHelp
	}
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

// errHelp is the clean end of a help request: the caller returns nil.
var errHelp = errors.New("help")

// done maps the help sentinel to a clean return.
func done(err error) error {
	if errors.Is(err, errHelp) {
		return nil
	}
	return err
}

// positionals checks a command received exactly the positionals its
// usage names, teaching the usage otherwise.
func positionals(fs *flag.FlagSet, pos []string, want int, names string) error {
	if len(pos) == want {
		return nil
	}
	fs.Usage()
	return fmt.Errorf("%w: expected %s", ErrUsage, names)
}

// The connection flags every account sentence shares, registered on each
// but documented once (decision 0045 § 4). Resolution is field-wise:
// explicit flags beat CHRONICLE_CONTEXT / CHRONICLE_STORE beat the
// selected context beat the quick start's data dir. Being someone is one
// of three ways, never two at once: a credential file (the JWT names the
// principal), an nkey seed with the principal stated (a user on your own
// NATS, or the quick start's), or the identity bridge where the install
// has one — a profile and the account it lands in, which `chronicle
// login` saves on a context so the sentences need neither flag from there.
type connectFlags struct {
	url       *string
	creds     *string
	nkey      *string
	principal *string
	dir       *string
	store     *string
	ctxName   *string
	bridge    *string
	account   *string
	verbose   *bool
}

var connectionFlagNames = map[string]bool{
	"url": true, "creds": true, "nkey": true, "principal": true, "dir": true,
	"store": true, "context": true, "bridge": true, "account": true, "verbose": true,
}

const connectionHelp = `Connection flags, accepted by every account sentence (default: the selected context):
  --context NAME     the context to speak through (default: CHRONICLE_CONTEXT, else the selection)
  --store NAME       the store to speak to (default: CHRONICLE_STORE, else the context's selected store)
  --url URL          the NATS url
  --creds FILE       a credential file; the principal is the one it names
  --nkey FILE        an nkey seed — a user on your own NATS; needs --principal
  --principal NAME   your principal, when the credential carries no name
  --bridge FILE      an install profile: dial through its identity bridge with your GitHub sign-in (see: chronicle login)
  --account NAME     the account a --bridge dial lands in
  --dir DIR          the quick start's data dir, the fallback when nothing else says where (default ~/.chronicle/dev)
  --verbose          print the connection a sentence resolved to before it speaks
`

func (x *runner) connect(fs *flag.FlagSet) connectFlags {
	return connectFlags{
		url:       fs.String("url", "", ""),
		creds:     fs.String("creds", "", ""),
		nkey:      fs.String("nkey", "", ""),
		principal: fs.String("principal", "", ""),
		dir:       fs.String("dir", devdir.Default(), ""),
		store:     fs.String("store", "", ""),
		ctxName:   fs.String("context", "", ""),
		bridge:    fs.String("bridge", "", ""),
		account:   fs.String("account", "", ""),
		verbose:   fs.Bool("verbose", false, ""),
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
	context   string
	url       string
	creds     string
	nkey      string
	principal string
	store     string // may be empty; verbs that need one call needStore
	bridge    string
	account   string
	verbose   bool
}

func (cf connectFlags) resolve() (resolved, error) {
	root, err := ConfigRoot()
	if err != nil {
		return resolved{}, err
	}
	var sc storedContext
	name := cf.contextName(root)
	if name != "" {
		sc, err = loadStoredContext(root, name)
		if err != nil {
			return resolved{}, err
		}
	}
	r := resolved{
		context: name,
		url:     *cf.url, creds: *cf.creds, nkey: *cf.nkey, principal: *cf.principal,
		store: *cf.store, bridge: *cf.bridge, account: *cf.account, verbose: *cf.verbose,
	}
	ways := 0
	for _, w := range []string{r.creds, r.nkey, r.bridge} {
		if w != "" {
			ways++
		}
	}
	if ways > 1 {
		return resolved{}, fmt.Errorf("--creds, --nkey and --bridge are three ways to be someone; pick one")
	}
	if ways == 0 {
		r.creds, r.nkey, r.principal, r.bridge = sc.Creds, sc.Nkey, sc.Principal, sc.Bridge
	}
	if r.account == "" {
		r.account = sc.Account
	}
	if r.store == "" {
		r.store = os.Getenv("CHRONICLE_STORE")
	}
	if r.store == "" {
		r.store = sc.Store
	}
	if r.url == "" {
		r.url = sc.URL
	}
	// The quick start's fallback: a running `chronicle up` recorded its
	// url and keeps its one user under --dir, and that user is the
	// account's admin.
	if r.creds == "" && r.nkey == "" && r.bridge == "" {
		if _, err := os.Stat(devdir.UserNkeyPath(*cf.dir)); err == nil {
			r.nkey, r.principal = devdir.UserNkeyPath(*cf.dir), devdir.LocalPrincipal
		}
	}
	if r.creds == "" && r.nkey == "" && r.bridge == "" {
		return resolved{}, errNoCreds
	}
	if r.nkey != "" && r.principal == "" {
		return resolved{}, errNoPrincipal
	}
	if r.url == "" && r.bridge == "" {
		r.url, err = devdir.ReadClientURL(*cf.dir)
		if err != nil {
			return resolved{}, err
		}
	}
	return r, nil
}

func (r resolved) needStore() (string, error) {
	if r.store == "" {
		return "", errNoStore
	}
	return r.store, nil
}

// dial connects as the resolved principal; with --verbose it first says
// what it resolved to.
func (r resolved) dial(out io.Writer) (*client.Client, error) {
	if r.verbose {
		who := r.creds
		if r.nkey != "" {
			who = r.principal + " (nkey " + r.nkey + ")"
		}
		if r.bridge != "" {
			who = "GitHub sign-in into account " + r.account
		}
		fmt.Fprintf(out, "connection: context %q, url %s, as %s, store %q\n", r.context, r.url, who, r.store)
	}
	switch {
	case r.bridge != "":
		if r.account == "" {
			return nil, fmt.Errorf("--bridge needs the account to land in: --account NAME, or the context chronicle login saved")
		}
		return dialThroughBridge(r.bridge, r.account)
	case r.nkey != "":
		return client.ConnectNkeyFile(r.url, r.nkey, r.principal)
	default:
		return client.ConnectFile(r.url, r.creds)
	}
}

// session is the preamble every account sentence runs: resolve, dial.
func (x *runner) session(cf connectFlags, out io.Writer) (*client.Client, resolved, error) {
	r, err := cf.resolve()
	if err != nil {
		return nil, resolved{}, err
	}
	c, err := r.dial(out)
	if err != nil {
		return nil, resolved{}, err
	}
	return c, r, nil
}

// storeSession is the preamble for a sentence that speaks to one store.
func (x *runner) storeSession(cf connectFlags, out io.Writer) (*client.Client, string, error) {
	r, err := cf.resolve()
	if err != nil {
		return nil, "", err
	}
	store, err := r.needStore()
	if err != nil {
		return nil, "", err
	}
	c, err := r.dial(out)
	if err != nil {
		return nil, "", err
	}
	return c, store, nil
}

// sortedKeys is the teaching list's order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
