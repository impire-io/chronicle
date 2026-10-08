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
			{"create NAME [--history compactable|full] [--description TEXT]", "create a store and select it", x.storeCreate},
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
	return printValue(out, *format, func() {
		fmt.Fprintf(out, "store:        %s\n", pos[0])
		fmt.Fprintf(out, "history:      %s\n", describeHistory(cfg.History))
		if cfg.Description != "" {
			fmt.Fprintf(out, "description:  %s\n", cfg.Description)
		}
		if cfg.MaxBytes > 0 {
			fmt.Fprintf(out, "size limit:   %d bytes\n", cfg.MaxBytes)
		}
	}, map[string]any{"name": pos[0], "history": contract.NormalizeHistory(cfg.History), "description": cfg.Description, "max_bytes": cfg.MaxBytes})
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
	fs := flags("store create", "create NAME [--history compactable|full] [--description TEXT]", "create a store and select it", out)
	cf := x.connect(fs)
	history := fs.String("history", contract.HistoryCompactable, "the history policy: compactable (a snapshot may replace older history) or full (every operation is kept)")
	desc := fs.String("description", "", "what the store is for")
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
	if _, err := c.CreateStore(ctx, pos[0], *desc, client.WithHistory(*history)); err != nil {
		return err
	}
	fmt.Fprintf(out, "store %s created (history %s)\n", pos[0], contract.NormalizeHistory(*history))

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
