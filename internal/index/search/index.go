// Package search runs chronicle-index-search: the search-kind
// materializer of design 05-indexes.md — a full-text projection of one
// log's thing state. It folds the log under the current declarations
// through the shared judge, indexes each thing's state as one document
// keyed by the thing's subject tail, and answers
// CHRON.API.INDEX.QUERY.<log>.<index> once caught up. Hits name things;
// the index is never authority — state is the state bucket's, history
// the log's.
package search

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"

	"github.com/impire-io/chronicle/client"
)

// newBleveIndex is the engine, by contract (0012): embedded, in-memory,
// rebuilt by replay — losing it is a replay, not an incident. The default
// dynamic mapping indexes every string field of state under the default
// analyzer; there is no per-field configuration.
func newBleveIndex() (bleve.Index, error) {
	idx, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		return nil, fmt.Errorf("open bleve index: %w", err)
	}
	return idx, nil
}

// serving is the swap point between the query handler and the fold: the
// index being served is replaced whole when a re-fold catches up, so
// queries never see a half-rebuilt index.
type serving struct {
	mu  sync.RWMutex
	idx bleve.Index
}

func (v *serving) set(idx bleve.Index) {
	v.mu.Lock()
	v.idx = idx
	v.mu.Unlock()
}

func (v *serving) query(r client.IndexQueryRequest) (client.IndexQueryResponse, error) {
	v.mu.RLock()
	idx := v.idx
	v.mu.RUnlock()
	if idx == nil {
		// Unreachable once Start has returned: the endpoint registers only
		// after the first fold catches up and swaps its index in.
		return client.IndexQueryResponse{}, errors.New("index not caught up")
	}

	var q query.Query
	if r.Query == "" {
		q = bleve.NewMatchAllQuery()
	} else {
		q = bleve.NewMatchQuery(r.Query)
	}
	limit := r.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	offset := max(r.Offset, 0)

	res, err := idx.Search(bleve.NewSearchRequestOptions(q, limit, offset, false))
	if err != nil {
		return client.IndexQueryResponse{}, fmt.Errorf("search: %w", err)
	}
	reply := client.IndexQueryResponse{Hits: []client.IndexHit{}, Total: res.Total}
	for _, hit := range res.Hits {
		reply.Hits = append(reply.Hits, client.IndexHit{Thing: hit.ID, Score: hit.Score})
	}
	return reply, nil
}

// docFor turns a thing's folded state into the indexed document. State is
// the customer's JSON; whatever shape it is, it is indexed as-is under
// the dynamic mapping.
func docFor(state json.RawMessage) (any, error) {
	if len(state) == 0 {
		return map[string]any{}, nil
	}
	var doc any
	if err := json.Unmarshal(state, &doc); err != nil {
		return nil, fmt.Errorf("state is not JSON: %w", err)
	}
	if doc == nil {
		return map[string]any{}, nil
	}
	return doc, nil
}
