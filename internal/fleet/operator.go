package fleet

import (
	"context"
	"flag"
	"fmt"
	"io"

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
	pub, err := mint.PublicKeyOfSeed(r.B.OperatorSigningSeed)
	if err != nil {
		return fmt.Errorf("operator signing seed: %w", err)
	}
	fmt.Fprintf(out, "root: %s\n", r.Dir)
	fmt.Fprintf(out, "  operator signing key: %s\n", pub)
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
	r, target, nc, err := openRoot(*dir, *url, "chronicle-seal")
	if err != nil {
		return err
	}
	defer nc.Close()
	rep, err := r.Seal(ctx, nc, mint.SealOptions{Replicas: *replicas})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "sealed %s into %s\n", r.Dir, target)
	fmt.Fprintf(out, "  written: %d  matched: %d\n", len(rep.Written), len(rep.Matched))
	fmt.Fprintf(out, "  export:  %s\n", rep.Export)
	fmt.Fprintln(out, "the root keeps the operator identity, the node keys, and its exports; keep it offline")
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
	r, _, nc, err := openRoot(*dir, *url, "chronicle-export")
	if err != nil {
		return err
	}
	defer nc.Close()
	c, err := mint.OpenCustody(ctx, nc)
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

// openRoot loads the root and connects as its first instance — the bundle
// init issued — falling back to the bootstrap control user on a root that
// predates bundles.
func openRoot(dir, url, name string) (*mint.Root, string, *nats.Conn, error) {
	r, err := mint.LoadRoot(dir)
	if err != nil {
		return nil, "", nil, err
	}
	target := url
	if target == "" {
		recorded, err := devdir.ReadClientURL(dir)
		if err != nil {
			return nil, "", nil, fmt.Errorf("no --url and no recorded url in %s: %w", dir, err)
		}
		target = recorded
	}
	creds := r.B.ControlCreds
	if b, err := mint.ReadBundle(r.BundleDir(firstInstance)); err == nil {
		creds = b.ControlCreds
	}
	nc, err := mint.ConnectCreds(target, creds, name)
	if err != nil {
		return nil, "", nil, fmt.Errorf("connect %s: %w", target, err)
	}
	return r, target, nc, nil
}
