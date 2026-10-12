package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/impire-io/chronicle/bridge"
	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// The service-account sentences in their two forms (chronicle-hq decision
// 0052, design 07 § members and service accounts). With a sign-in context
// — the one `login` saved, a bridge profile and an account — `create`,
// `rotate` and `revoke` ask the install, as the account's admin, to issue,
// rotate or kill the machine's credential, and the credential is shown
// exactly once. With a credential or an nkey (self-hosted) the sentences
// write the registry only: the credential is your NATS's to issue and to
// kill, and --public-key records the key.

// IdentityPlane is what the sign-in form of the service-account sentences
// speaks to: the three verbs an account's admin has there. Exported for a
// build that wraps the sentences, and for tests.
type IdentityPlane interface {
	IssueServiceAccount(ctx context.Context, account, name, role string) (bridge.ServiceAccountCredential, error)
	RotateServiceAccount(ctx context.Context, account, name string) (bridge.ServiceAccountCredential, error)
	RevokeServiceAccount(ctx context.Context, account, name string) (bridge.ServiceAccountRevokeResponse, error)
	Close()
}

// dialIdentityThroughBridge is how a service-account sentence reaches the
// install as the signed-in identity; a test swaps it.
var dialIdentityThroughBridge = func(profilePath string) (IdentityPlane, error) {
	id, _, err := identityDial(profilePath)
	if err != nil {
		return nil, err
	}
	return id, nil
}

func (x *runner) serviceAccountCreate(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("service-account create", "create NAME [--role admin|writer|reader] [--public-key KEY] [--out FILE]", "create a service account; hosted, its credential is shown once", out)
	cf := x.connect(fs)
	role := fs.String("role", contract.RoleWriter, "admin, writer or reader")
	publicKey := fs.String("public-key", "", "the NATS user public key, where your NATS names users by key (self-hosted)")
	outFile := fs.String("out", "", "write the credential here instead of printing it (hosted)")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one name"); err != nil {
		return err
	}
	if !contract.KnownRole(*role) {
		return fmt.Errorf("--role %q: admin, writer or reader", *role)
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	if r.bridge != "" {
		id, err := x.identity(r, out)
		if err != nil {
			return err
		}
		defer id.Close()
		cred, err := id.IssueServiceAccount(ctx, r.account, pos[0], *role)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "service account %s created (role %s) in account %s\n", cred.Name, cred.Role, cred.Account)
		return handDown(out, cred, *outFile)
	}
	c, err := r.dial(out)
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.AddMember(ctx, pos[0], *role, client.WithPublicKey(*publicKey), client.AsServiceAccount())
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "service account %s created (role %s)\n", resp.Member, resp.Role)
	if *publicKey == "" {
		fmt.Fprintf(out, "its credential is your NATS's to issue: a user named %s with the member baseline (see the README); --public-key records the key\n", resp.Member)
	}
	return nil
}

func (x *runner) serviceAccountRotate(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("service-account rotate", "rotate NAME [--public-key KEY] [--out FILE]", "a new credential for the service account; the old one no longer connects", out)
	cf := x.connect(fs)
	publicKey := fs.String("public-key", "", "the new NATS user public key (self-hosted)")
	outFile := fs.String("out", "", "write the new credential here instead of printing it (hosted)")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one name"); err != nil {
		return err
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	if r.bridge != "" {
		id, err := x.identity(r, out)
		if err != nil {
			return err
		}
		defer id.Close()
		cred, err := id.RotateServiceAccount(ctx, r.account, pos[0])
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "service account %s rotated (role %s) in account %s: the old credential no longer connects\n", cred.Name, cred.Role, cred.Account)
		return handDown(out, cred, *outFile)
	}
	if *publicKey == "" {
		return fmt.Errorf("self-hosted, the credential is your NATS's to issue: issue the new user, then record its key with --public-key KEY")
	}
	c, err := r.dial(out)
	if err != nil {
		return err
	}
	defer c.Close()
	// The registry has no update verb: a rotate self-hosted is the old
	// record retired and a new one recorded with the same role.
	current, err := findServiceAccount(ctx, c, pos[0])
	if err != nil {
		return err
	}
	revoked, err := c.RevokeMember(ctx, pos[0])
	if err != nil {
		return err
	}
	added, err := c.AddMember(ctx, pos[0], current.Role, client.WithPublicKey(*publicKey), client.AsServiceAccount())
	if err != nil {
		return fmt.Errorf("the old record is retired, but the new key is not recorded: %w (record it: chronicle service-account create %s --role %s --public-key %s)", err, pos[0], current.Role, *publicKey)
	}
	fmt.Fprintf(out, "service account %s rotated (role %s): key %s recorded", added.Member, added.Role, *publicKey)
	if revoked.PublicKey != "" {
		fmt.Fprintf(out, "; the old credential (%s) is your NATS's to revoke", revoked.PublicKey)
	}
	fmt.Fprintln(out)
	return nil
}

func (x *runner) serviceAccountRevoke(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("service-account revoke", "revoke NAME", "revoke a service account; it can act no more", out)
	cf := x.connect(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one name"); err != nil {
		return err
	}
	r, err := cf.resolve()
	if err != nil {
		return err
	}
	if r.bridge != "" {
		id, err := x.identity(r, out)
		if err != nil {
			return err
		}
		defer id.Close()
		resp, err := id.RevokeServiceAccount(ctx, r.account, pos[0])
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "service account %s revoked from account %s: its credential no longer connects\n", resp.Name, resp.Account)
		return nil
	}
	c, err := r.dial(out)
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.RevokeMember(ctx, pos[0])
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "service account %s revoked", resp.Member)
	if resp.PublicKey != "" {
		fmt.Fprintf(out, "; its credential (%s) is your NATS's to revoke", resp.PublicKey)
	}
	fmt.Fprintln(out)
	return nil
}

// identity reaches the install as the signed-in identity of a sign-in
// context; the account the sentence speaks about is the context's.
func (x *runner) identity(r resolved, out io.Writer) (IdentityPlane, error) {
	if r.account == "" {
		return nil, fmt.Errorf("the sign-in needs the account to speak about: --account NAME, or the context chronicle login saved")
	}
	if r.verbose {
		fmt.Fprintf(out, "connection: context %q, as your GitHub sign-in, about account %s\n", r.context, r.account)
	}
	return dialIdentityThroughBridge(r.bridge)
}

// handDown shows a credential exactly once: printed, or written to the
// file asked for and never printed.
func handDown(out io.Writer, cred bridge.ServiceAccountCredential, outFile string) error {
	if outFile != "" {
		if err := os.WriteFile(outFile, []byte(cred.Creds), 0o600); err != nil {
			return fmt.Errorf("write the credential: %w", err)
		}
		fmt.Fprintf(out, "credential written to %s (the only copy; it will not be shown again)\n", outFile)
		return nil
	}
	fmt.Fprintf(out, "credential (shown once; it will not be shown again):\n%s\n", cred.Creds)
	return nil
}

// findServiceAccount reads the registry for one service account.
func findServiceAccount(ctx context.Context, c *client.Client, name string) (client.MemberInfo, error) {
	for m, err := range c.ListMembers(ctx) {
		if err != nil {
			return client.MemberInfo{}, err
		}
		if m.Name != name {
			continue
		}
		if m.Kind != contract.PrincipalKindService {
			return client.MemberInfo{}, fmt.Errorf("%s is a member, not a service account", name)
		}
		return m, nil
	}
	return client.MemberInfo{}, errors.New("service account " + name + " does not exist")
}
