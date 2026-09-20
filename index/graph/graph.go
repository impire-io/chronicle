// Package graph runs chronicle-index-graph: the graph-kind materializer
// of design 05-indexes.md (decision 0015) — declared edges from one
// log's thing state, never inferred. It folds the log through the shared
// projection spine, recomputes each thing's out-edges wholesale from its
// current state under the declaration's edge rules, and answers
// neighbors and walk on CHRON.API.INDEX.QUERY.<log>.<index> once caught
// up. Targets are opaque thing tails; the index is never authority.
package graph

import (
	"encoding/json"
	"log/slog"
	"sort"
	"sync"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// graphRun is one fold pass's engine: forward and reverse adjacency, in
// memory, rebuilt by replay — the maps a graph store would be overkill
// beside. One lock serves the fold's writes and the queries' reads.
type graphRun struct {
	rules  []contract.GraphEdgeRule
	log    string
	logger *slog.Logger

	mu  sync.RWMutex
	fwd map[string][]contract.GraphEdge
	rev map[string][]contract.GraphEdge
}

func newGraphRun(log string, rules []contract.GraphEdgeRule, logger *slog.Logger) *graphRun {
	return &graphRun{
		rules:  rules,
		log:    log,
		logger: logger,
		fwd:    map[string][]contract.GraphEdge{},
		rev:    map[string][]contract.GraphEdge{},
	}
}

// Upsert recomputes a thing's out-edges from its freshly folded state —
// a pure function of current state (0015): the old edges go wholesale,
// the new ones land, and the reverse index follows.
func (r *graphRun) Upsert(thing string, state json.RawMessage) {
	edges := r.extract(thing, state)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, old := range r.fwd[thing] {
		r.rev[old.To] = removeEdge(r.rev[old.To], old)
		if len(r.rev[old.To]) == 0 {
			delete(r.rev, old.To)
		}
	}
	if len(edges) == 0 {
		delete(r.fwd, thing)
		return
	}
	r.fwd[thing] = edges
	for _, e := range edges {
		r.rev[e.To] = append(r.rev[e.To], e)
	}
}

func removeEdge(edges []contract.GraphEdge, gone contract.GraphEdge) []contract.GraphEdge {
	kept := edges[:0]
	for _, e := range edges {
		if e != gone {
			kept = append(kept, e)
		}
	}
	return kept
}

// extract evaluates the edge rules against state. Non-string leaves,
// missing paths, and unparseable state are no edges — skipped quietly at
// debug, never errored (the fold's own tolerance).
func (r *graphRun) extract(thing string, state json.RawMessage) []contract.GraphEdge {
	if len(state) == 0 {
		return nil
	}
	var doc any
	if err := json.Unmarshal(state, &doc); err != nil {
		r.logger.Debug("graph index: state not JSON; no edges", "log", r.log, "thing", thing, "err", err)
		return nil
	}
	var edges []contract.GraphEdge
	seen := map[contract.GraphEdge]struct{}{}
	for _, rule := range r.rules {
		for _, target := range evalPath(doc, splitPath(rule.Field)) {
			e := contract.GraphEdge{From: thing, To: target, Label: rule.EdgeLabel()}
			if _, dup := seen[e]; dup {
				continue
			}
			seen[e] = struct{}{}
			edges = append(edges, e)
		}
	}
	return edges
}

func splitPath(path string) []string {
	var segs []string
	start := 0
	for i := 0; i <= len(path); i++ {
		if i == len(path) || path[i] == '.' {
			segs = append(segs, path[start:i])
			start = i + 1
		}
	}
	return segs
}

// evalPath walks one dotted path: objects by field, arrays element-wise,
// strings at the leaf. Anything else yields nothing.
func evalPath(v any, segs []string) []string {
	if len(segs) == 0 {
		switch leaf := v.(type) {
		case string:
			if leaf == "" {
				return nil
			}
			return []string{leaf}
		case []any:
			var out []string
			for _, el := range leaf {
				out = append(out, evalPath(el, nil)...)
			}
			return out
		}
		return nil
	}
	switch node := v.(type) {
	case map[string]any:
		return evalPath(node[segs[0]], segs[1:])
	case []any:
		var out []string
		for _, el := range node {
			out = append(out, evalPath(el, segs)...)
		}
		return out
	}
	return nil
}

// neighbors answers the degree-one read: edges at a thing, filtered by
// direction and label, in stable order for honest pagination.
func (r *graphRun) neighbors(req client.GraphQueryRequest) client.GraphNeighborsResponse {
	r.mu.RLock()
	var edges []contract.GraphEdge
	if req.Direction == contract.GraphDirectionOut || req.Direction == contract.GraphDirectionBoth || req.Direction == "" {
		edges = append(edges, r.fwd[req.Thing]...)
	}
	if req.Direction == contract.GraphDirectionIn || req.Direction == contract.GraphDirectionBoth {
		edges = append(edges, r.rev[req.Thing]...)
	}
	r.mu.RUnlock()

	if req.Label != "" {
		kept := edges[:0]
		for _, e := range edges {
			if e.Label == req.Label {
				kept = append(kept, e)
			}
		}
		edges = kept
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].Label != edges[j].Label {
			return edges[i].Label < edges[j].Label
		}
		if edges[i].From != edges[j].From {
			return edges[i].From < edges[j].From
		}
		return edges[i].To < edges[j].To
	})

	total := uint64(len(edges))
	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	offset := max(req.Offset, 0)
	if offset > len(edges) {
		offset = len(edges)
	}
	end := min(offset+limit, len(edges))
	return client.GraphNeighborsResponse{Edges: append([]contract.GraphEdge{}, edges[offset:end]...), Total: total}
}

// walk answers the bounded traversal: breadth-first, cycle-safe, depth
// capped — and the cap stated in the reply when it bit.
func (r *graphRun) walk(req client.GraphQueryRequest) client.GraphWalkResponse {
	depth := req.Depth
	if depth <= 0 {
		depth = 1
	}
	capped := depth > contract.GraphWalkMaxDepth
	if capped {
		depth = contract.GraphWalkMaxDepth
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	var labels map[string]struct{}
	if len(req.Labels) > 0 {
		labels = map[string]struct{}{}
		for _, l := range req.Labels {
			labels[l] = struct{}{}
		}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	type frontier struct {
		thing string
		depth int
	}
	visited := map[string]struct{}{req.Thing: {}}
	queue := []frontier{{req.Thing, 0}}
	var visits []contract.GraphVisit
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur.depth == depth {
			continue
		}
		var edges []contract.GraphEdge
		if req.Direction == contract.GraphDirectionOut || req.Direction == contract.GraphDirectionBoth || req.Direction == "" {
			edges = append(edges, r.fwd[cur.thing]...)
		}
		if req.Direction == contract.GraphDirectionIn || req.Direction == contract.GraphDirectionBoth {
			edges = append(edges, r.rev[cur.thing]...)
		}
		sort.Slice(edges, func(i, j int) bool {
			if edges[i].Label != edges[j].Label {
				return edges[i].Label < edges[j].Label
			}
			return edges[i].To+edges[i].From < edges[j].To+edges[j].From
		})
		for _, e := range edges {
			if labels != nil {
				if _, ok := labels[e.Label]; !ok {
					continue
				}
			}
			next := e.To
			if next == cur.thing {
				next = e.From // an in-edge walked backward
			}
			if _, been := visited[next]; been {
				continue
			}
			visited[next] = struct{}{}
			visits = append(visits, contract.GraphVisit{Thing: next, Depth: cur.depth + 1, Via: e.Label})
			if len(visits) >= limit {
				return client.GraphWalkResponse{Things: visits, Total: uint64(len(visits)), DepthCapped: capped, Truncated: true}
			}
			queue = append(queue, frontier{next, cur.depth + 1})
		}
	}
	return client.GraphWalkResponse{Things: visits, Total: uint64(len(visits)), DepthCapped: capped}
}
