package cli

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

func (x *runner) storeNoun() *noun {
	return &noun{
		name:    "store",
		summary: "the stores in your account: each holds types, instances and indexes",
		verbs: []command{
			{"list", "the stores in the account; * marks the selected one", x.storeList},
			{"get NAME", "one store's settings", x.storeGet},
			{"create NAME [--history compactable|full] [--schedules] [--verified-authors] [--description TEXT]", "create a store and select it", x.storeCreate},
			{"select NAME", "make a store the one commands speak to", x.storeSelect},
		},
	}
}

func (x *runner) storeList(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("store list", "list", "the stores in the account; * marks the selected one", out)
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
	c, r, err := x.session(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	var names []string
	for name, err := range c.ListStores(ctx) {
		if err != nil {
			return err
		}
		names = append(names, name)
	}
	sort.Strings(names)
	var rows [][]string
	var items []any
	for _, name := range names {
		cfg, err := c.GetStore(ctx, name)
		if err != nil {
			return err
		}
		marker := ""
		if name == r.store {
			marker = "*"
		}
		rows = append(rows, []string{marker, name, contract.NormalizeHistory(cfg.History), cfg.Description})
		items = append(items, map[string]any{"name": name, "selected": name == r.store, "history": contract.NormalizeHistory(cfg.History), "description": cfg.Description})
	}
	return printList(out, *format, []string{"", "NAME", "HISTORY", "DESCRIPTION"}, rows, items)
}

func (x *runner) storeGet(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("store get", "get NAME", "one store's settings", out)
	cf := x.connect(fs)
	format := outputFlag(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one store name"); err != nil {
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
	cfg, err := c.GetStore(ctx, pos[0])
	if err != nil {
		return err
	}
	usage, err := c.StoreUsage(ctx, pos[0])
	if err != nil {
		return err
	}
	return printValue(out, *format, func() {
		fmt.Fprintf(out, "store:        %s\n", pos[0])
		fmt.Fprintf(out, "history:      %s\n", describeHistory(cfg.History))
		if cfg.Description != "" {
			fmt.Fprintf(out, "description:  %s\n", cfg.Description)
		}
		fmt.Fprintf(out, "size:         %d of %d bytes used%s\n", usage.Bytes, usage.MaxBytes, describeBudget(cfg.MaxBytes))
		if cfg.Schedules {
			fmt.Fprintf(out, "schedules:    allowed (a schedule registered here has its ticks written by the server)\n")
		}
		if cfg.VerifiedAuthors {
			fmt.Fprintf(out, "authors:      verified required (writes go through chronicle; an unverified operation is kept but takes no effect)\n")
		}
	}, map[string]any{"name": pos[0], "history": contract.NormalizeHistory(cfg.History), "description": cfg.Description, "max_bytes": usage.MaxBytes, "bytes": usage.Bytes, "schedules": cfg.Schedules, "verified_authors": cfg.VerifiedAuthors})
}

// describeBudget says where a store's byte budget comes from: its own
// setting, or the default every store is held to.
func describeBudget(maxBytes int64) string {
	if maxBytes > 0 {
		return ""
	}
	return " (the default budget)"
}

// describeHistory says what a history policy means, beside its value.
func describeHistory(history string) string {
	switch contract.NormalizeHistory(history) {
	case contract.HistoryFull:
		return "full (every operation is kept; snapshots are declined)"
	default:
		return "compactable (a snapshot may replace older history)"
	}
}

func (x *runner) storeCreate(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("store create", "create NAME [--history compactable|full] [--schedules] [--verified-authors] [--description TEXT]", "create a store and select it", out)
	cf := x.connect(fs)
	history := fs.String("history", contract.HistoryCompactable, "the history policy: compactable (a snapshot may replace older history) or full (every operation is kept)")
	desc := fs.String("description", "", "what the store is for")
	schedules := fs.Bool("schedules", false, "allow NATS message schedules in this store: a schedule registered in it has its ticks written by the server")
	verified := fs.Bool("verified-authors", false, "require verified authors: every write goes through chronicle, which vouches for who wrote it; an operation written any other way is kept but takes no effect")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one store name"); err != nil {
		return err
	}
	if h := contract.NormalizeHistory(*history); h != contract.HistoryCompactable && h != contract.HistoryFull {
		return fmt.Errorf("--history %q: compactable or full", *history)
	}
	c, r, err := x.session(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	opts := []client.StoreOpt{client.WithHistory(*history)}
	if *schedules {
		opts = append(opts, client.WithSchedules())
	}
	if *verified {
		opts = append(opts, client.WithVerifiedAuthors())
	}
	if _, err := c.CreateStore(ctx, pos[0], *desc, opts...); err != nil {
		return err
	}
	created := fmt.Sprintf("store %s created (history %s", pos[0], contract.NormalizeHistory(*history))
	if *schedules {
		created += ", schedules allowed"
	}
	if *verified {
		created += ", verified authors required"
	}
	fmt.Fprintln(out, created+")")

	// The store just made becomes the selected one — on the context that
	// made it, which `up`, `login` and `context add` make sure exists.
	root, rerr := ConfigRoot()
	if rerr != nil {
		return nil
	}
	if r.context != "" {
		if err := updateSelectedStore(root, r.context, pos[0]); err == nil {
			fmt.Fprintf(out, "store %s selected\n", pos[0])
		}
	} else {
		fmt.Fprintf(out, "not selected: no context holds the selection — chronicle context add NAME --creds FILE, or pass --store %s\n", pos[0])
	}
	return nil
}

func (x *runner) storeSelect(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("store select", "select NAME", "make a store the one commands speak to", out)
	cf := x.connect(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one store name"); err != nil {
		return err
	}
	root, err := ConfigRoot()
	if err != nil {
		return err
	}
	name := cf.contextName(root)
	if name == "" {
		return errNoContext
	}
	c, _, err := x.session(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	var stores []string
	for s, err := range c.ListStores(ctx) {
		if err != nil {
			return err
		}
		stores = append(stores, s)
	}
	found := false
	for _, s := range stores {
		if s == pos[0] {
			found = true
			break
		}
	}
	if !found {
		if len(stores) == 0 {
			return fmt.Errorf("store %q does not exist — no stores yet (chronicle store create NAME)", pos[0])
		}
		sort.Strings(stores)
		return fmt.Errorf("store %q does not exist — stores: %s", pos[0], strings.Join(stores, ", "))
	}
	if err := updateSelectedStore(root, name, pos[0]); err != nil {
		return err
	}
	fmt.Fprintf(out, "store %s selected (context %s)\n", pos[0], name)
	return nil
}
