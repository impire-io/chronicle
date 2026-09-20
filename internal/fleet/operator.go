package fleet

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/mint"
)

// The service's ceremonies of design 10 (chronicle-hq/02-DESIGN/10-custody.md
// § first boot, § instances, § rotation; decisions 0031 and 0032),
// dispatched from cmd/chronicle: seal from the environment's material,
// instances by role, the export, the service's step of the rotation. The
// environment's half — the operator identity, the node configs, the node
// keys, the operator JWT roll — is chronicle-ops's and is not here. The
// dev dir plays the environment for `up` and the dev-shape rotation.

// firstInstance is the bundle seal issues.
const firstInstance = "instance-1"

// Seal is `chronicle operator seal`: the service's birth ceremony, handed
// the environment's seeds against a cluster that preloads the bare
// accounts (design 10 § first boot). It stamps the account shapes, births
// the bucket, issues the first instance's bundle, and writes the export.
func Seal(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operator seal", flag.ContinueOnError)
	fs.SetOutput(out)
	url := fs.String("url", "", "the cluster's client url (required)")
	replicas := fs.Int("replicas", 0, "replicas of the AUTH bucket: 3 on a cluster, 1 on a single server (required)")
	signingSeed := fs.String("signing-seed", "", "the operator signing seed the environment generated (required)")
	sysSeed := fs.String("sys-seed", "", "the SYS account seed the environment generated (required)")
	controlSeed := fs.String("control-seed", "", "the CONTROL account seed the environment generated (required)")
	outDir := fs.String("out", filepath.Join(devdir.BundlesDir, "instance-1"), "where the first instance's bundle is written")
	export := fs.String("export", "", "where the dated export is written (default: auth-export-<stamp>.json beside the bundle)")
	verifyWith := fs.String("bundle", "", "the first instance's bundle, to verify a cluster that is sealed already")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("operator seal takes no positionals")
	}
	if *url == "" {
		return fmt.Errorf("operator seal needs --url")
	}
	if *replicas <= 0 {
		return fmt.Errorf("operator seal needs --replicas: 3 on a cluster, 1 on a single server")
	}
	m, err := mint.ReadMaterial(*signingSeed, *sysSeed, *controlSeed)
	if err != nil {
		return err
	}
	sealOpts := mint.SealOptions{Replicas: *replicas}
	if *verifyWith != "" {
		if sealOpts.VerifyWith, err = mint.ReadBundle(*verifyWith); err != nil {
			return err
		}
	}
	rep, bundle, custody, err := mint.SealFromMaterial(ctx, *url, m, sealOpts)
	if err != nil {
		return err
	}
	if rep.Instance == "" {
		fmt.Fprintf(out, "%s is sealed with this material already: %d entries matched, nothing written\n", *url, len(rep.Matched))
		fmt.Fprintln(out, "the first instance's bundle was issued at the first seal; further instances: chronicle operator instance add")
		return nil
	}
	if err := os.MkdirAll(*outDir, 0o700); err != nil {
		return fmt.Errorf("create bundle dir: %w", err)
	}
	if err := writeBundleTo(*outDir, bundle); err != nil {
		return err
	}
	exportPath := *export
	if exportPath == "" {
		exportPath = filepath.Join(filepath.Dir(*outDir), "auth-export-"+time.Now().UTC().Format("20060102T150405Z")+".json")
	}
	data, err := mint.ExportJSON(ctx, custody)
	if err != nil {
		return err
	}
	if err := os.WriteFile(exportPath, data, 0o600); err != nil {
		return fmt.Errorf("write export: %w", err)
	}
	fmt.Fprintf(out, "sealed %s: %d entries written\n", *url, len(rep.Written))
	fmt.Fprintf(out, "  instance: %s\n  bundle:   %s\n  export:   %s\n", rep.Instance, *outDir, exportPath)
	fmt.Fprintln(out, "destroy the seeds you handed over; keep the operator identity, the node keys, and the export offline")
	fmt.Fprintf(out, "start the first instance: chronicle-control --url %s --bundle %s\n", *url, *outDir)
	return nil
}

// writeBundleTo writes a bundle's files into an existing directory.
func writeBundleTo(dir string, b mint.Bundle) error {
	if err := os.WriteFile(filepath.Join(dir, devdir.ControlCredsFile), b.ControlCreds, 0o600); err != nil {
		return fmt.Errorf("write bundle: %w", err)
	}
	if len(b.SysCreds) > 0 {
		if err := os.WriteFile(filepath.Join(dir, "sys.creds"), b.SysCreds, 0o600); err != nil {
			return fmt.Errorf("write bundle: %w", err)
		}
	}
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
	ceremony := ceremonyFlags(fs)
	template := fs.String("template", string(mint.TemplateControlInstance), "add: the instance's role — control-instance (a chronicle-control peer), executor (one host; the name is its --id), workloads (a workload service), cli (an operator's CLI)")
	outDir := fs.String("out", "", "add: where the bundle is written (default: bundles/<name> under --dir, or ./bundles/<name>)")
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
	r, conns, err := ceremony.open("chronicle-instance-"+verb, except)
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
		if r != nil {
			if err := r.RemoveBundle(name); err != nil {
				return err
			}
		}
		fmt.Fprintf(out, "instance %s removed: its users are revoked, live connections evicted\n", name)
		return nil
	}
	bundle, err := driver.AddInstance(ctx, name, t)
	if err != nil {
		return err
	}
	var path string
	switch {
	case r != nil && *outDir == "":
		path, err = r.WriteBundle(name, bundle)
	default:
		path = *outDir
		if path == "" {
			path = filepath.Join(devdir.BundlesDir, name)
		}
		if err = os.MkdirAll(path, 0o700); err == nil {
			err = writeBundleTo(path, bundle)
		}
	}
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

// Export is `chronicle operator export`: a dated export of the bucket —
// the disaster-recovery root the environment keeps, refreshed at each
// rotation and each instance change.
func Export(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operator export", flag.ContinueOnError)
	fs.SetOutput(out)
	ceremony := ceremonyFlags(fs)
	outFile := fs.String("out", "", "where the export is written (default: the dev dir's exports/ under --dir, or ./auth-export-<stamp>.json)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("operator export takes no positionals")
	}
	r, conns, err := ceremony.open("chronicle-export", "")
	if err != nil {
		return err
	}
	defer conns.close()
	c, err := mint.OpenCustody(ctx, conns.ctrl)
	if err != nil {
		return err
	}
	path := *outFile
	if path == "" && r != nil {
		path, err = r.Export(ctx, c)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "exported to %s\n", path)
		return nil
	}
	if path == "" {
		path = "auth-export-" + time.Now().UTC().Format("20060102T150405Z") + ".json"
	}
	data, err := mint.ExportJSON(ctx, c)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write export: %w", err)
	}
	fmt.Fprintf(out, "exported to %s\n", path)
	return nil
}

// RotateSigningKey is `chronicle operator rotate-signing-key`. With --url
// and --new-signing-seed it is the service's step of the clustered
// rotation (design 10 § rotation): the environment has added the new key
// to the operator JWT and rolled the nodes; this lands the seed in the
// bucket and re-signs every account by compare-and-set then push; the
// environment then removes the old key and rolls again. With --dir alone
// it is the dev shape: the dev dir plays the environment around the same
// step over its embedded server, which must be stopped.
func RotateSigningKey(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operator rotate-signing-key", flag.ContinueOnError)
	fs.SetOutput(out)
	ceremony := ceremonyFlags(fs)
	newSeed := fs.String("new-signing-seed", "", "the new operator signing seed the environment generated and the nodes already trust (the live ceremony)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("operator rotate-signing-key takes no positionals")
	}
	if *newSeed == "" {
		if ceremony.bundle != "" || ceremony.url != "" {
			return fmt.Errorf("the live ceremony needs --new-signing-seed; the dev shape takes --dir alone")
		}
		newPub, err := mint.RotateOperatorSigningKey(ceremony.dir)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "operator signing key rotated: %s\n", newPub)
		fmt.Fprintln(out, "every account re-signed and verified; start the fleet to serve under the new key")
		return nil
	}
	seed, err := os.ReadFile(*newSeed)
	if err != nil {
		return fmt.Errorf("read the new signing seed: %w", err)
	}
	_, conns, err := ceremony.open("chronicle-rotate", "")
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
	rep, err := mint.RotateOverBucket(ctx, driver, seed)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "the bucket names %s; re-signed and pushed: %s\n", rep.NewPublicKey, strings.Join(rep.Resigned, ", "))
	fmt.Fprintln(out, "next: remove the old key from the operator JWT, roll the nodes, and refresh the export")
	return nil
}

// ceremonyOpts is how a ceremony reaches the cluster: --bundle names a
// control instance's bundle (the hosted shape, --url required); --dir
// names the dev dir, whose first control bundle and recorded url serve
// (the dev shape).
type ceremonyOpts struct {
	dir, bundle, url string
}

func ceremonyFlags(fs *flag.FlagSet) *ceremonyOpts {
	o := &ceremonyOpts{}
	fs.StringVar(&o.dir, "dir", devdir.Default(), "the dev dir (the dev shape)")
	fs.StringVar(&o.bundle, "bundle", "", "a control instance's bundle to run the ceremony as (the hosted shape)")
	fs.StringVar(&o.url, "url", "", "the cluster's client url (required with --bundle; default with --dir: the dev dir's recorded url)")
	return o
}

// open connects the ceremony's two users. The root comes back for the dev
// shape and is nil for the hosted one.
func (o *ceremonyOpts) open(name, except string) (*mint.Root, *ceremonyConns, error) {
	if o.bundle == "" {
		return openRoot(o.dir, o.url, name, except)
	}
	if o.url == "" {
		return nil, nil, fmt.Errorf("--bundle needs --url")
	}
	bundle, err := mint.ReadBundle(o.bundle)
	if err != nil {
		return nil, nil, err
	}
	if !bundle.IsControlInstance() {
		return nil, nil, fmt.Errorf("%s is not a control instance's bundle (no sys.creds)", o.bundle)
	}
	instance, err := mint.InstanceOf(bundle.ControlCreds)
	if err != nil {
		return nil, nil, err
	}
	if instance == except {
		return nil, nil, fmt.Errorf("the ceremony runs as %s and cannot remove it; run it as another control instance", instance)
	}
	conns, err := connectCeremony(o.url, bundle, name)
	return nil, conns, err
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
	conns, err := connectCeremony(target, bundle, name)
	return r, conns, err
}

func connectCeremony(url string, bundle mint.Bundle, name string) (*ceremonyConns, error) {
	conns := &ceremonyConns{url: url}
	var err error
	conns.sys, err = mint.ConnectCreds(url, bundle.SysCreds, name+"-sys")
	if err != nil {
		return nil, fmt.Errorf("connect %s as the system user: %w", url, err)
	}
	conns.ctrl, err = mint.ConnectCreds(url, bundle.ControlCreds, name)
	if err != nil {
		conns.close()
		return nil, fmt.Errorf("connect %s as the control user: %w", url, err)
	}
	return conns, nil
}
