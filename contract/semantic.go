package contract

import (
	"encoding/json"
	"fmt"
	"strings"
)

// The semantic index kind (decision 0016): meaning through an
// operator-configured, OpenAI-compatible embedding provider — a pure
// function, not a store, degrading semantic only. The provider is
// install configuration; the declaration carries what the index *means*:
// the model it was embedded under and the fields that carry meaning.

// IndexKindSemantic is the third index kind.
const IndexKindSemantic = "semantic"

// SemanticConfig is the semantic kind's declaration config. Model names
// what the index is embedded under (empty means the install default;
// changing it is delete + declare, and the rebuild re-embeds). Fields
// narrows which state fields carry meaning — dotted paths, the graph
// kind's grammar — defaulting to every string field, search's own rule.
type SemanticConfig struct {
	Model  string   `json:"model,omitempty"`
	Fields []string `json:"fields,omitempty"`
}

// ParseSemanticConfig validates a semantic declaration's config
// write-side strict. Absent config is valid — every default stands.
func ParseSemanticConfig(raw json.RawMessage) (SemanticConfig, error) {
	var zero SemanticConfig
	if len(raw) == 0 {
		return zero, nil
	}
	var cfg SemanticConfig
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return zero, fmt.Errorf("semantic config: %w", err)
	}
	for i, f := range cfg.Fields {
		if err := validateFieldPath(f); err != nil {
			return zero, fmt.Errorf("semantic config: field %d: %w", i, err)
		}
	}
	return cfg, nil
}

// SemanticChunkBytes is the default chunk budget: comfortably under
// every common model's token window for prose. The operator corrects it
// per model at the install (a byte budget, deliberately not a tokenizer
// — weight nothing carries yet).
const SemanticChunkBytes = 2048
