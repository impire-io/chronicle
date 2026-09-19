package fleet

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/mint"
)

// The ceremonies of design 10 (chronicle-hq/02-DESIGN/10-custody.md § first
// boot), dispatched from cmd/chronicle like the operator verbs before them:
// custody operations on the offline root, not product-surface verbs.

// firstInstance is the bundle `init` issues.
const firstInstance = "instance-1"

// InitRoot is `chronicle operator init`: birth the offline root — operator
// identity and signing key, the bootstrap accounts, the first instance's
// bundle — or open the one already there.
func InitRoot(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operator init", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "the offline root (the data dir)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("operator init takes no positionals")
	}
	r, err := mint.InitRoot(*dir)
	if err != nil {
		return err
	}
	signing, err := r.B.OperatorSigningKeys()
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "root: %s\n", r.Dir)
	fmt.Fprintf(out, "  operator signing key: %s\n", strings.Join(signing, ", "))
	fmt.Fprintf(out, "  bundle:               %s (the first control instance)\n", r.BundleDir(firstInstance))
	if r.Manifest.Sealed != "" {
		fmt.Fprintf(out, "  sealed:               %s\n", r.Manifest.Sealed)
		return nil
	}
	fmt.Fprintf(out, "next: chronicle operator emit-cluster-config --dir %s --node <name>=<host> ...\n", r.Dir)
	fmt.Fprintf(out, "      start the nodes, then: chronicle operator seal --dir %s --url <cluster> --replicas 3\n", r.Dir)
	return nil
}

// Seal is `chronicle operator seal`: once the cluster serves, move the
// root's working keys into the AUTH bucket, read them back, export.
func Seal(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operator seal", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "the offline root (the data dir)")
	url := fs.String("url", "", "the cluster's client url (default: the root's recorded url)")
	replicas := fs.Int("replicas", 0, "replicas of the AUTH bucket: 3 on a cluster, 1 on a single server (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("operator seal takes no positionals")
	}
	if *replicas <= 0 {
		return fmt.Errorf("operator seal needs --replicas: 3 on a cluster, 1 on a single server")
	}
	r, conns, err := openRoot(*dir, *url, "chronicle-seal", "")
	if err != nil {
		return err
	}
	defer conns.close()
	rep, err := r.Seal(ctx, conns.ctrl, mint.SealOptions{Replicas: *replicas})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "sealed %s into %s\n", r.Dir, conns.url)
	fmt.Fprintf(out, "  written: %d  matched: %d\n", len(rep.Written), len(rep.Matched))
	if rep.Export != "" {
		fmt.Fprintf(out, "  export:  %s\n", rep.Export)
	}
	fmt.Fprintln(out, "the root keeps the operator identity, the node keys, its bundles and its exports — no working key; keep it offline")
	return nil
}

// Instance is `chronicle operator instance add|remove <name>`: issue an
// instance's users over the live bucket and write its bundle, or revoke
// them. Run from the root against any node of the cluster, with the
// root's own control-instance bundle.
func Instance(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 || (args[0] != "add" && args[0] != "remove") {
		return fmt.Errorf("operator instance wants add or remove")
	}
	verb := args[0]
	fs := flag.NewFlagSet("chronicle operator instance "+verb, flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "the offline root (the data dir)")
	url := fs.String("url", "", "the cluster's client url (default: the root's recorded url)")
	template := fs.String("template", string(mint.TemplateControlInstance), "add: the instance's role — control-instance (a chronicle-control peer), executor (one host; the name is its --id), workloads (a workload service), cli (an operator's CLI)")
	pos, err := parseInterleaved(fs, args[1:])
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("operator instance %s takes one positional: the instance name", verb)
	}
	name := pos[0]
	t, err := mint.ParseTemplate(*template)
	if err != nil {
		return err
	}
	except := ""
	if verb == "remove" {
		except = name
	}
	r, conns, err := openRoot(*dir, *url, "chronicle-instance-"+verb, except)
	if err != nil {
		return err
	}
	defer conns.close()
	custody, err := mint.OpenCustody(ctx, conns.ctrl)
	if err != nil {
		return err
	}
	driver, err := mint.NewJWTDriver(ctx, custody, conns.sys, conns.url)
	if err != nil {
		return err
	}

	if verb == "remove" {
		if err := driver.RemoveInstance(ctx, name); err != nil {
			return err
		}
		if err := r.RemoveBundle(name); err != nil {
			return err
		}
		fmt.Fprintf(out, "instance %s removed: its users are revoked, live connections evicted\n", name)
		return nil
	}
	bundle, err := driver.AddInstance(ctx, name, t)
	if err != nil {
		return err
	}
	path, err := r.WriteBundle(name, bundle)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "instance %s added (%s)\n", name, t)
	fmt.Fprintf(out, "  bundle: %s\n", path)
	creds := filepath.Join(path, devdir.ControlCredsFile)
	switch t {
	case mint.TemplateControlInstance:
		fmt.Fprintf(out, "copy the bundle to its host and run: chronicle-control --url %s --bundle <dir>\n", conns.url)
	case mint.TemplateExecutor:
		fmt.Fprintf(out, "copy %s to its host and run: chronicle-executor --url %s --creds <file> --id %s\n", creds, conns.url, name)
	case mint.TemplateWorkloads:
		fmt.Fprintf(out, "copy %s to its host: the workload service's --creds\n", creds)
	case mint.TemplateCLI:
		fmt.Fprintf(out, "%s is the CLI's control-plane credential\n", creds)
	}
	return nil
}

// Export is `chronicle operator export`: a dated export of the bucket into
// the root — the disaster-recovery root, refreshed at each rotation and
// each instance change.
func Export(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operator export", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "the offline root (the data dir)")
	url := fs.String("url", "", "the cluster's client url (default: the root's recorded url)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("operator export takes no positionals")
	}
	r, conns, err := openRoot(*dir, *url, "chronicle-export", "")
	if err != nil {
		return err
	}
	defer conns.close()
	c, err := mint.OpenCustody(ctx, conns.ctrl)
	if err != nil {
		return err
	}
	path, err := r.Export(ctx, c)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "exported to %s\n", path)
	return nil
}

// parseInterleaved parses flags and positionals in any order — the CLI's
// grammar, where the name comes first and the flags after: the stdlib
// flag package stops at the first positional, so pop it and parse on.
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
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

// ceremonyConns is what a ceremony holds open: the instance's two users.
type ceremonyConns struct {
	url       string
	sys, ctrl *nats.Conn
}

func (c *ceremonyConns) close() {
	if c.sys != nil {
		c.sys.Close()
	}
	if c.ctrl != nil {
		c.ctrl.Close()
	}
}

// openRoot loads the root and connects as one of its control instances —
// the first bundle it issued, skipping `except`. A root holds no other
// user.
func openRoot(dir, url, name, except string) (*mint.Root, *ceremonyConns, error) {
	r, err := mint.LoadRoot(dir)
	if err != nil {
		return nil, nil, err
	}
	target := url
	if target == "" {
		recorded, err := devdir.ReadClientURL(dir)
		if err != nil {
			return nil, nil, fmt.Errorf("no --url and no recorded url in %s: %w", dir, err)
		}
		target = recorded
	}
	_, bundle, err := r.ControlBundle(except)
	if err != nil {
		return nil, nil, err
	}
	conns := &ceremonyConns{url: target}
	conns.sys, err = mint.ConnectCreds(target, bundle.SysCreds, name+"-sys")
	if err != nil {
		return nil, nil, fmt.Errorf("connect %s as the system user: %w", target, err)
	}
	conns.ctrl, err = mint.ConnectCreds(target, bundle.ControlCreds, name)
	if err != nil {
		conns.close()
		return nil, nil, fmt.Errorf("connect %s as the control user: %w", target, err)
	}
	return r, conns, nil
}
