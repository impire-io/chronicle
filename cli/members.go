package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// The two nouns for the registry's entries (decision 0044): a member is a
// person, a service account is a machine. Both are memberships on the
// node ([11-the-two-forms.md] § membership, without custody); the
// credential is not these sentences' — self-hosted it is your NATS's to
// issue and to kill, and --public-key records it; hosted, the managed
// build issues it first and then speaks the same sentence.

func (x *runner) memberNoun() *noun {
	return &noun{
		name:    "member",
		summary: "the people in your account, each with a role",
		verbs: []command{
			{"list", "the members and their roles", x.memberList},
			{"add NAME [--role admin|writer|reader] [--public-key KEY] [--github-id N]", "add a person to the account", x.memberAdd},
			{"remove NAME", "remove a person from the account; their history stays attributed to them", x.memberRemove},
			{"set-role NAME ROLE", "change a member's role", x.memberSetRole},
		},
	}
}

func (x *runner) serviceAccountNoun() *noun {
	return &noun{
		name:    "service-account",
		summary: "the machines in your account: a service that connects with a credential",
		verbs: []command{
			{"list", "the service accounts and their roles", x.serviceAccountList},
			{"create NAME [--role admin|writer|reader] [--public-key KEY] [--out FILE]", "create a service account; hosted, its credential is shown once", x.serviceAccountCreate},
			{"rotate NAME [--public-key KEY] [--out FILE]", "a new credential for the service account; the old one no longer connects", x.serviceAccountRotate},
			{"revoke NAME", "revoke a service account; it can act no more", x.serviceAccountRevoke},
			{"set-role NAME ROLE", "change a service account's role", x.serviceAccountSetRole},
		},
	}
}

func (x *runner) listPrincipals(ctx context.Context, args []string, out io.Writer, kind, name, summary string) error {
	fs := flags(name+" list", "list", summary, out)
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
	c, _, err := x.session(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	var rows [][]string
	var items []any
	for m, err := range c.ListMembers(ctx) {
		if err != nil {
			return err
		}
		if m.Kind != kind {
			continue
		}
		identity := m.PublicKey
		if m.GithubID != 0 {
			identity = fmt.Sprintf("github:%d", m.GithubID)
		}
		rows = append(rows, []string{m.Name, m.Role, identity})
		items = append(items, m)
	}
	header := []string{"NAME", "ROLE", "IDENTITY"}
	if kind == contract.PrincipalKindService {
		header = []string{"NAME", "ROLE", "KEY"}
	}
	return printList(out, *format, header, rows, items)
}

func (x *runner) memberList(ctx context.Context, args []string, out io.Writer) error {
	return x.listPrincipals(ctx, args, out, contract.PrincipalKindMember, "member", "the members and their roles")
}

func (x *runner) serviceAccountList(ctx context.Context, args []string, out io.Writer) error {
	return x.listPrincipals(ctx, args, out, contract.PrincipalKindService, "service-account", "the service accounts and their roles")
}

func (x *runner) memberAdd(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("member add", "add NAME [--role admin|writer|reader] [--public-key KEY] [--github-id N]", "add a person to the account", out)
	cf := x.connect(fs)
	role := fs.String("role", contract.RoleWriter, "admin, writer or reader")
	publicKey := fs.String("public-key", "", "the NATS user public key, where your NATS names users by key (self-hosted)")
	githubID := fs.Int64("github-id", 0, "the person's GitHub user id, for signing in with GitHub (hosted)")
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
	c, _, err := x.session(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.AddMember(ctx, pos[0], *role, client.WithPublicKey(*publicKey), client.WithGithubID(*githubID))
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "member %s added (role %s)\n", resp.Member, resp.Role)
	return nil
}

func (x *runner) memberRemove(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("member remove", "remove NAME", "remove a person from the account; their history stays attributed to them", out)
	cf := x.connect(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one name"); err != nil {
		return err
	}
	c, _, err := x.session(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.RevokeMember(ctx, pos[0])
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "member %s removed", resp.Member)
	if resp.PublicKey != "" {
		fmt.Fprintf(out, "; its credential (%s) is your NATS's to revoke", resp.PublicKey)
	}
	fmt.Fprintln(out)
	return nil
}

// memberSetRole changes a member's role in place (decision 0062):
// effective from their next request, the credential untouched; the
// account's only admin cannot be demoted, and the node says so.
func (x *runner) memberSetRole(ctx context.Context, args []string, out io.Writer) error {
	return x.setRole(ctx, args, out, "member", "change a member's role")
}

// serviceAccountSetRole is the same verb for the other noun.
func (x *runner) serviceAccountSetRole(ctx context.Context, args []string, out io.Writer) error {
	return x.setRole(ctx, args, out, "service-account", "change a service account's role")
}

func (x *runner) setRole(ctx context.Context, args []string, out io.Writer, noun, summary string) error {
	fs := flags(noun+" set-role", "set-role NAME ROLE", summary, out)
	cf := x.connect(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 2, "NAME ROLE"); err != nil {
		return err
	}
	if !contract.KnownRole(pos[1]) {
		return fmt.Errorf("role %q: admin, writer or reader", pos[1])
	}
	c, _, err := x.session(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.SetRole(ctx, pos[0], pos[1])
	if err != nil {
		return err
	}
	what := "member"
	if noun == "service-account" {
		what = "service account"
	}
	fmt.Fprintf(out, "%s %s is now %s (from its next request on; its credential is unchanged)\n", what, resp.Member, resp.Role)
	return nil
}

// accountNoun is the open `account`: `create NAME`, a further account of
// your own through the identity bridge you signed in with. A build that
// mints accounts itself (the managed service's operator form) wraps it
// through Extension.Override.
func (x *runner) accountNoun() *noun {
	return &noun{
		name:    "account",
		summary: "create a further account of your own (hosted; after chronicle login)",
		verbs: []command{
			{"create NAME [--bridge FILE]", "create an account of your own and land in it", accountCreate},
		},
	}
}
