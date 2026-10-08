package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

func (x *runner) indexNoun() *noun {
	return &noun{
		name:    "index",
		summary: "the indexes a store declares — search, graph, semantic — and querying them",
		verbs: []command{
			{"list", "the indexes in the store", x.indexList},
			{"get NAME", "one index's declaration", x.indexGet},
			{"create NAME --kind search|graph|semantic [--source state|history] [--config FILE]", "declare an index; it is built from the history and kept current", x.indexCreate},
			{"delete NAME", "delete an index; it was derived, nothing of record is lost", x.indexDelete},
			{"query NAME [TEXT...] [--from PATH [--depth N]] [--limit N]", "query an index: text for search and semantic, --from a path for graph", x.indexQuery},
		},
	}
}

// sourceOf reads the source out of a declaration's config.
func sourceOf(cfg json.RawMessage) string {
	var c struct {
		Source string `json:"source"`
	}
	_ = json.Unmarshal(cfg, &c)
	return contract.NormalizeSource(c.Source)
}

func (x *runner) indexList(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("index list", "list", "the indexes in the store", out)
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
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	var rows [][]string
	var items []any
	for info, err := range c.ListIndexes(ctx, store) {
		if err != nil {
			return err
		}
		// State is read as each instance's state, not listed as an index.
		if info.Kind == contract.IndexKindState {
			continue
		}
		rows = append(rows, []string{info.Name, info.Kind, sourceOf(info.Config)})
		items = append(items, map[string]any{"name": info.Name, "kind": info.Kind, "source": sourceOf(info.Config), "config": info.Config})
	}
	if err := printList(out, *format, []string{"NAME", "KIND", "SOURCE"}, rows, items); err != nil {
		return err
	}
	if *format == formatTable && len(rows) == 0 {
		fmt.Fprintln(out, "(no indexes — chronicle index create NAME --kind search)")
	}
	return nil
}

func (x *runner) indexGet(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("index get", "get NAME", "one index's declaration", out)
	cf := x.connect(fs)
	format := outputFlag(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one index name"); err != nil {
		return err
	}
	if err := checkFormat(*format); err != nil {
		return err
	}
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	decl, err := c.GetIndexDeclaration(ctx, store, pos[0])
	if err != nil {
		return teachNoIndex(ctx, c, store, err)
	}
	return printValue(out, *format, func() {
		fmt.Fprintf(out, "index %s  kind %s  source %s\n", pos[0], decl.Kind, sourceOf(decl.Config))
		if len(decl.Config) > 0 {
			fmt.Fprintf(out, "config: %s\n", pretty(decl.Config))
		}
	}, map[string]any{"name": pos[0], "kind": decl.Kind, "source": sourceOf(decl.Config), "config": decl.Config})
}

func teachNoIndex(ctx context.Context, c *client.Client, store string, err error) error {
	if !errors.Is(err, client.ErrNoIndex) {
		return err
	}
	var names []string
	for info, lerr := range c.ListIndexes(ctx, store) {
		if lerr != nil {
			break
		}
		if info.Kind != contract.IndexKindState {
			names = append(names, info.Name)
		}
	}
	if len(names) == 0 {
		return fmt.Errorf("%w — no indexes in store %s yet (chronicle index create NAME --kind search)", err, store)
	}
	return fmt.Errorf("%w — indexes: %s", err, strings.Join(sortedStrings(names), ", "))
}

func (x *runner) indexCreate(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("index create", "create NAME --kind search|graph|semantic [--source state|history] [--config FILE]", "declare an index; it is built from the history and kept current", out)
	cf := x.connect(fs)
	kind := fs.String("kind", "", "search (full text over state), graph (edges declared from fields), or semantic (meaning, through an embedding model) (required)")
	source := fs.String("source", "", "what the index reads: state (default) or history (every operation; search and semantic only)")
	config := fs.String("config", "", "the kind's config, a YAML or JSON file or inline JSON (graph: the edge rules; semantic: model and fields)")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one index name"); err != nil {
		return err
	}
	if *kind == "" {
		fs.Usage()
		return fmt.Errorf("%w: --kind is required: search, graph or semantic", ErrUsage)
	}
	if !contract.KnownIndexKind(*kind) {
		return fmt.Errorf("--kind %q: search, graph or semantic", *kind)
	}
	var cfgRaw json.RawMessage
	if *config != "" {
		if cfgRaw, err = fileOrInline(*config); err != nil {
			return fmt.Errorf("--config: %w", err)
		}
	}
	if *source != "" {
		if cfgRaw, err = withSource(cfgRaw, *source); err != nil {
			return err
		}
	}
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.DeclareIndex(ctx, store, pos[0], *kind, cfgRaw); err != nil {
		return err
	}
	fmt.Fprintf(out, "index %s created in store %s (%s, from %s): it serves once it has read the history\n", pos[0], store, *kind, contract.NormalizeSource(*source))
	return nil
}

// withSource sets the source on a config document.
func withSource(cfg json.RawMessage, source string) (json.RawMessage, error) {
	if s := contract.NormalizeSource(source); s != contract.SourceState && s != contract.SourceHistory {
		return nil, fmt.Errorf("--source %q: state or history", source)
	}
	m := map[string]any{}
	if len(cfg) > 0 {
		if err := json.Unmarshal(cfg, &m); err != nil {
			return nil, fmt.Errorf("--config: %w", err)
		}
	}
	m["source"] = source
	return json.Marshal(m)
}

func (x *runner) indexDelete(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("index delete", "delete NAME", "delete an index; it was derived, nothing of record is lost", out)
	cf := x.connect(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if err := positionals(fs, pos, 1, "one index name"); err != nil {
		return err
	}
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.DeleteIndex(ctx, store, pos[0]); err != nil {
		return err
	}
	fmt.Fprintf(out, "index %s deleted from store %s\n", pos[0], store)
	return nil
}

func (x *runner) indexQuery(ctx context.Context, args []string, out io.Writer) error {
	fs := flags("index query", "query NAME [TEXT...] [--from PATH [--depth N]] [--limit N]", "query an index: text for search and semantic, --from a path for graph", out)
	cf := x.connect(fs)
	format := outputFlag(fs)
	from := fs.String("from", "", "graph: the instance to start from")
	depth := fs.Int("depth", 0, "graph: walk this many steps (without it: the instance's neighbours)")
	direction := fs.String("direction", "out", "graph: out, in, or both")
	label := fs.String("label", "", "graph neighbours: one edge label")
	labels := fs.String("labels", "", "graph walk: comma-separated edge labels to follow")
	limit := fs.Int("limit", 0, "stop after this many hits (0: all)")
	pos, err := parse(fs, args)
	if err != nil {
		return done(err)
	}
	if len(pos) < 1 {
		return positionals(fs, pos, 1, "an index name, then the text to search for")
	}
	if err := checkFormat(*format); err != nil {
		return err
	}
	index := pos[0]
	text := strings.Join(pos[1:], " ")
	c, store, err := x.storeSession(cf, out)
	if err != nil {
		return err
	}
	defer c.Close()

	// The declared kind shapes the query.
	decl, err := c.GetIndexDeclaration(ctx, store, index)
	if err != nil {
		return teachNoIndex(ctx, c, store, err)
	}
	trailer := func(human string, t any) error {
		if *format == formatTable {
			fmt.Fprintln(out, human)
			return nil
		}
		return jsonLine(out, map[string]any{"trailer": t})
	}
	switch decl.Kind {
	case contract.IndexKindSearch, contract.IndexKindSemantic:
		if text == "" {
			fmt.Fprintf(out, "index %s is a %s index: chronicle index query %s TEXT...\n", index, decl.Kind, index)
			return nil
		}
		if decl.Kind == contract.IndexKindSearch {
			s := c.QueryIndex(ctx, store, index, text, *limit)
			if *format == formatTable {
				table(out, []string{"PATH", "SCORE"}, nil)
			}
			for hit, err := range s.Items() {
				if err != nil {
					return err
				}
				if err := printHit(out, *format, hit.Instance, hit.Score, "", hit); err != nil {
					return err
				}
			}
			t, _ := s.Trailer()
			return trailer(fmt.Sprintf("%d of %d", s.Count(), t.Total), t)
		}
		s := c.QuerySemantic(ctx, store, index, text, *limit)
		if *format == formatTable {
			table(out, []string{"PATH", "SCORE", "FIELD"}, nil)
		}
		for h, err := range s.Items() {
			if err != nil {
				return err
			}
			if err := printHit(out, *format, h.Instance, h.Score, h.Field, h); err != nil {
				return err
			}
		}
		t, _ := s.Trailer()
		human := fmt.Sprintf("%d of %d", s.Count(), t.Total)
		if t.Unembedded > 0 {
			human += fmt.Sprintf(" (%d not yet embedded)", t.Unembedded)
		}
		return trailer(human, t)
	case contract.IndexKindGraph:
		if *from == "" {
			fmt.Fprintf(out, "index %s is a graph index: chronicle index query %s --from PATH [--depth N] [--direction out|in|both] [--label L | --labels a,b]\n", index, index)
			return nil
		}
		// The depth decides the form: without it the neighbours, with it
		// a walk.
		if *depth > 0 {
			var labelList []string
			if *labels != "" {
				labelList = strings.Split(*labels, ",")
			}
			s := c.GraphWalk(ctx, store, index, client.GraphQueryRequest{Instance: *from, Direction: *direction, Labels: labelList, Depth: *depth, Limit: *limit})
			if *format == formatTable {
				table(out, []string{"PATH", "DEPTH", "VIA"}, nil)
			}
			for v, err := range s.Items() {
				if err != nil {
					return err
				}
				if *format == formatTable {
					fmt.Fprintf(out, "%s  %d  %s\n", v.Instance, v.Depth, v.Via)
					continue
				}
				if err := jsonLine(out, v); err != nil {
					return err
				}
			}
			t, _ := s.Trailer()
			human := fmt.Sprintf("%d instances", t.Total)
			if t.DepthCapped {
				human += fmt.Sprintf(" (depth capped at %d)", contract.GraphWalkMaxDepth)
			}
			if t.Truncated {
				human += " (stopped at the limit)"
			}
			return trailer(human, t)
		}
		s := c.GraphNeighbors(ctx, store, index, client.GraphQueryRequest{Instance: *from, Direction: *direction, Label: *label, Limit: *limit})
		if *format == formatTable {
			table(out, []string{"FROM", "LABEL", "TO"}, nil)
		}
		for e, err := range s.Items() {
			if err != nil {
				return err
			}
			if *format == formatTable {
				fmt.Fprintf(out, "%s  %s  %s\n", e.From, e.Label, e.To)
				continue
			}
			if err := jsonLine(out, e); err != nil {
				return err
			}
		}
		t, _ := s.Trailer()
		return trailer(fmt.Sprintf("%d of %d", s.Count(), t.Total), t)
	case contract.IndexKindState:
		return fmt.Errorf("state is read per instance: chronicle instance get PATH, or chronicle instance list")
	default:
		return fmt.Errorf("index %s has kind %q, which this build cannot query", index, decl.Kind)
	}
}

func printHit(out io.Writer, format, path string, score float64, field string, item any) error {
	if format != formatTable {
		return jsonLine(out, item)
	}
	if field != "" {
		fmt.Fprintf(out, "%s  %.4f  %s\n", path, score, field)
		return nil
	}
	fmt.Fprintf(out, "%s  %.4f\n", path, score)
	return nil
}
