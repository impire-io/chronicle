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
			{"create NAME [--role admin|writer|reader] [--public-key KEY]", "create a service account", x.serviceAccountCreate},
			{"revoke NAME", "revoke a service account; it can act no more", x.serviceAccountRevoke},
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

func (x *runner) addPrincipal(ctx context.Context, args []string, out io.Writer, kind string) error {
	name, use, summary := "member add", "add NAME [--role admin|writer|reader] [--public-key KEY] [--github-id N]", "add a person to the account"
	if kind == contract.PrincipalKindService {
		name, use, summary = "service-account create", "create NAME [--role admin|writer|reader] [--public-key KEY]", "create a service account"
	}
	fs := flags(name, use, summary, out)
	cf := x.connect(fs)
	role := fs.String("role", contract.RoleWriter, "admin, writer or reader")
	publicKey := fs.String("public-key", "", "the NATS user public key, where your NATS names users by key (self-hosted)")
	var githubID *int64
	if kind == contract.PrincipalKindMember {
		githubID = fs.Int64("github-id", 0, "the person's GitHub user id, for signing in with GitHub (hosted)")
	}
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
	opts := []client.MemberOpt{client.WithPublicKey(*publicKey)}
	if githubID != nil {
		opts = append(opts, client.WithGithubID(*githubID))
	}
	if kind == contract.PrincipalKindService {
		opts = append(opts, client.AsServiceAccount())
	}
	resp, err := c.AddMember(ctx, pos[0], *role, opts...)
	if err != nil {
		return err
	}
	if kind == contract.PrincipalKindService {
		fmt.Fprintf(out, "service account %s created (role %s)\n", resp.Member, resp.Role)
		if *publicKey == "" {
			fmt.Fprintf(out, "its credential is your NATS's to issue: a user named %s with the member baseline (see the README); --public-key records the key\n", resp.Member)
		}
		return nil
	}
	fmt.Fprintf(out, "member %s added (role %s)\n", resp.Member, resp.Role)
	return nil
}

func (x *runner) memberAdd(ctx context.Context, args []string, out io.Writer) error {
	return x.addPrincipal(ctx, args, out, contract.PrincipalKindMember)
}

func (x *runner) serviceAccountCreate(ctx context.Context, args []string, out io.Writer) error {
	return x.addPrincipal(ctx, args, out, contract.PrincipalKindService)
}

func (x *runner) revokePrincipal(ctx context.Context, args []string, out io.Writer, kind string) error {
	name, use, summary := "member remove", "remove NAME", "remove a person from the account; their history stays attributed to them"
	if kind == contract.PrincipalKindService {
		name, use, summary = "service-account revoke", "revoke NAME", "revoke a service account; it can act no more"
	}
	fs := flags(name, use, summary, out)
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
	if kind == contract.PrincipalKindService {
		fmt.Fprintf(out, "service account %s revoked", resp.Member)
	} else {
		fmt.Fprintf(out, "member %s removed", resp.Member)
	}
	if resp.PublicKey != "" {
		fmt.Fprintf(out, "; its credential (%s) is your NATS's to revoke", resp.PublicKey)
	}
	fmt.Fprintln(out)
	return nil
}

func (x *runner) memberRemove(ctx context.Context, args []string, out io.Writer) error {
	return x.revokePrincipal(ctx, args, out, contract.PrincipalKindMember)
}

func (x *runner) serviceAccountRevoke(ctx context.Context, args []string, out io.Writer) error {
	return x.revokePrincipal(ctx, args, out, contract.PrincipalKindService)
}

// memberSetRole waits on the role-change verb (design 13 § members, design
// 07 § members): the CLI says so rather than pretending.
func (x *runner) memberSetRole(_ context.Context, args []string, out io.Writer) error {
	fs := flags("member set-role", "set-role NAME ROLE", "change a member's role", out)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 2, "NAME ROLE"); err != nil {
		return err
	}
	return fmt.Errorf("changing a role is not available yet: remove the member and add them again with the new role (chronicle member remove %s; chronicle member add %s --role %s)", pos[0], pos[0], pos[1])
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
