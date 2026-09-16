package contract

import (
	"encoding/json"
	"fmt"
	"strings"
)

// The index source vocabulary (decision 0020): which of the log's two
// faces an index reads. Source is a config option orthogonal to kind,
// never a new kind. Write-side strict at INDEX.DECLARE.
const (
	// SourceState: the index materializes folded thing state — 0012's
	// answer, the default.
	SourceState = "state"
	// SourceOps: the index materializes per-op documents — history as it
	// is, every op, unknown types included. Schemas and effects shape
	// state, never history's visibility; hits stay thing-level.
	SourceOps = "ops"
)

// NormalizeSource maps the unset declaration to its meaning.
func NormalizeSource(source string) string {
	if source == "" {
		return SourceState
	}
	return source
}

// validateSource refuses values outside the vocabulary.
func validateSource(source string) error {
	if s := NormalizeSource(source); s != SourceState && s != SourceOps {
		return fmt.Errorf("source %q: %q or %q", source, SourceState, SourceOps)
	}
	return nil
}

// validateSourcedTypes checks a types narrowing against its source: it
// belongs to an ops-sourced index (state materializes things, not ops),
// and every entry must be a non-empty op type, no duplicates.
func validateSourcedTypes(source string, types []string) error {
	if len(types) == 0 {
		return nil
	}
	if NormalizeSource(source) != SourceOps {
		return fmt.Errorf("types narrows an ops-sourced index; this one reads state")
	}
	seen := map[string]struct{}{}
	for i, t := range types {
		if t == "" {
			return fmt.Errorf("types entry %d is empty", i)
		}
		if _, dup := seen[t]; dup {
			return fmt.Errorf("types entry %d duplicates %q", i, t)
		}
		seen[t] = struct{}{}
	}
	return nil
}

// SearchConfig is the search kind's declaration config (0020): the
// source, and for an ops-sourced index an optional narrowing of which op
// types are read. Nothing else — search stays no-knobs: source names
// what is read, never how it is analyzed.
type SearchConfig struct {
	Source string   `json:"source,omitempty"`
	Types  []string `json:"types,omitempty"`
}

// ParseSearchConfig validates a search declaration's config write-side
// strict. Absent config is valid — the state source, every op type.
func ParseSearchConfig(raw json.RawMessage) (SearchConfig, error) {
	var zero SearchConfig
	if len(raw) == 0 {
		return zero, nil
	}
	var cfg SearchConfig
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return zero, fmt.Errorf("search config: %w", err)
	}
	if err := validateSource(cfg.Source); err != nil {
		return zero, fmt.Errorf("search config: %w", err)
	}
	if err := validateSourcedTypes(cfg.Source, cfg.Types); err != nil {
		return zero, fmt.Errorf("search config: %w", err)
	}
	return cfg, nil
}
