// Package semantic runs chronicle-index-semantic: the semantic-kind
// materializer of design 05-indexes.md (decision 0016) — meaning over
// one log's thing state through an operator-configured,
// OpenAI-compatible embedding provider. The provider is a pure function,
// not a store; its absence degrades semantic only, and every reply says
// how much of the corpus is folded but not yet embedded.
package semantic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ProviderConfig is the install-level provider: endpoint, key, default
// model. The key is a secret and travels the workload contract's secret
// channel — never env, never META (0016).
type ProviderConfig struct {
	BaseURL string
	APIKey  string
	// Model is the install default; a declaration's config overrides it.
	Model string
}

// Configured reports whether an install carries a provider at all — the
// backend's Supports answer for the semantic kind.
func (p *ProviderConfig) Configured() bool {
	return p != nil && p.BaseURL != "" && p.Model != ""
}

// provider calls the OpenAI-compatible embeddings endpoint.
type provider struct {
	cfg  ProviderConfig
	http *http.Client
}

func newProvider(cfg ProviderConfig) *provider {
	return &provider{cfg: cfg, http: &http.Client{Timeout: 30 * time.Second}}
}

type embedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
}

// Embed turns texts into vectors, order-preserving.
func (p *provider) Embed(ctx context.Context, model string, inputs []string) ([][]float64, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(embedRequest{Model: model, Input: inputs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.BaseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("embed: %s answered %d: %s", p.cfg.BaseURL, resp.StatusCode, snippet)
	}
	var er embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
		return nil, fmt.Errorf("embed: decode response: %w", err)
	}
	if len(er.Data) != len(inputs) {
		return nil, fmt.Errorf("embed: %d inputs answered with %d vectors", len(inputs), len(er.Data))
	}
	vecs := make([][]float64, len(inputs))
	for _, d := range er.Data {
		if d.Index < 0 || d.Index >= len(vecs) {
			return nil, fmt.Errorf("embed: vector index %d out of range", d.Index)
		}
		vecs[d.Index] = d.Embedding
	}
	return vecs, nil
}

// Probe embeds one word — the say-it-plainly startup check the hits
// fleet's lessons demand: a provider hides nothing, so ask it early.
func (p *provider) Probe(ctx context.Context, model string) error {
	_, err := p.Embed(ctx, model, []string{"chronicle"})
	return err
}
