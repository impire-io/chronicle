package contract

import (
	"encoding/json"
	"fmt"
	"strings"
)

// The graph index kind (decision 0015): declared edges from thing state,
// never inferred. The declaration's config carries the edge rules; the
// index materializes forward and reverse adjacency, targets stay opaque
// thing tails, and out-edges are a pure function of current state.

// IndexKindGraph is the second index kind.
const IndexKindGraph = "graph"

// GraphEdgeRule extracts edges from one field of thing state. Field is a
// dotted path; arrays along the path are traversed element-wise; string
// values at the leaf are edges (one per element for arrays of strings);
// anything else is no edge — counted, never errored. Label names the edge
// type and defaults to the field path.
type GraphEdgeRule struct {
	Field string `json:"field"`
	Label string `json:"label,omitempty"`
}

// EdgeLabel is the rule's effective label.
func (r GraphEdgeRule) EdgeLabel() string {
	if r.Label != "" {
		return r.Label
	}
	return r.Field
}

// GraphConfig is the graph kind's declaration config.
type GraphConfig struct {
	Edges []GraphEdgeRule `json:"edges"`
}

// ParseGraphConfig validates a graph declaration's config write-side
// strict (0015): rules must exist, paths and labels must be well-formed.
// Whether a field exists in any thing's state stays soft — schemas are
// per-type and optional, and the fold's own tolerance is the model.
func ParseGraphConfig(raw json.RawMessage) (GraphConfig, error) {
	var zero GraphConfig
	if len(raw) == 0 {
		return zero, fmt.Errorf("the graph kind needs config with edge rules")
	}
	var cfg GraphConfig
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return zero, fmt.Errorf("graph config: %w", err)
	}
	if len(cfg.Edges) == 0 {
		return zero, fmt.Errorf("graph config: at least one edge rule is required")
	}
	seen := map[string]struct{}{}
	for i, rule := range cfg.Edges {
		if err := validateFieldPath(rule.Field); err != nil {
			return zero, fmt.Errorf("graph config: edge %d: %w", i, err)
		}
		label := rule.EdgeLabel()
		if _, dup := seen[rule.Field+"\x00"+label]; dup {
			return zero, fmt.Errorf("graph config: edge %d duplicates field %q with label %q", i, rule.Field, label)
		}
		seen[rule.Field+"\x00"+label] = struct{}{}
	}
	return cfg, nil
}

// validateFieldPath checks a dotted path: non-empty segments, no leading
// or trailing dots. Segment content is the customer's domain — state
// fields are theirs to name.
func validateFieldPath(path string) error {
	if path == "" {
		return fmt.Errorf("field path is empty")
	}
	for _, seg := range strings.Split(path, ".") {
		if seg == "" {
			return fmt.Errorf("field path %q has an empty segment", path)
		}
	}
	return nil
}

// The graph query contract (05-indexes.md § the graph kind): one subject,
// the kind's payload, the op field naming the verb.
const (
	GraphOpNeighbors = "neighbors"
	GraphOpWalk      = "walk"
)

// The direction vocabulary. In-edges cover this log's things pointing at
// the target; cross-log in-edges need the other log's index asked.
const (
	GraphDirectionOut  = "out"
	GraphDirectionIn   = "in"
	GraphDirectionBoth = "both"
)

// GraphWalkMaxDepth caps a walk; the reply says when the cap bit.
const GraphWalkMaxDepth = 6

// GraphEdge is one edge as answered: from and to are thing tails, to
// possibly dangling — data, not corruption; resolution is the caller's
// state-bucket read.
type GraphEdge struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Label string `json:"label"`
}

// GraphVisit is one thing reached by a walk: at which depth, via which
// label it was first discovered.
type GraphVisit struct {
	Thing string `json:"thing"`
	Depth int    `json:"depth"`
	Via   string `json:"via"`
}
