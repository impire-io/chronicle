// Package search runs chronicle-index-search: the search-kind
// materializer of design 05-indexes.md — a full-text projection of one
// log's thing state. It folds the log under the current declarations
// through the shared projection spine, indexes each thing's state as one
// document keyed by the thing's subject tail, and answers
// CHRON.API.INDEX.QUERY.<log>.<index> once caught up. Hits name things;
// the index is never authority — state is the state bucket's, history
// the log's.
package search

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"

	"github.com/impire-io/chronicle/client"
)

// searchRun is one fold pass's engine: an embedded, in-memory bleve
// index, by contract (0012) — rebuilt by replay, losing it is a replay,
// not an incident. The default dynamic mapping indexes every string
// field of state under the default analyzer; there is no per-field
// configuration.
type searchRun struct {
	idx    bleve.Index
	log    string
	logger *slog.Logger
}

func newSearchRun(log string, logger *slog.Logger) (*searchRun, error) {
	idx, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		return nil, fmt.Errorf("open bleve index: %w", err)
	}
	return &searchRun{idx: idx, log: log, logger: logger}, nil
}

// Upsert indexes a thing's freshly folded state — latest state wins.
func (r *searchRun) Upsert(thing string, state json.RawMessage) {
	doc, err := docFor(state)
	if err != nil {
		r.logger.Warn("search index: state not indexable", "log", r.log, "thing", thing, "err", err)
		return
	}
	if err := r.idx.Index(thing, doc); err != nil {
		r.logger.Warn("search index: index thing", "log", r.log, "thing", thing, "err", err)
	}
}

// query answers one search over this run's index. Bleve indexes are safe
// for concurrent search and index.
func (r *searchRun) query(req client.IndexQueryRequest) (client.IndexQueryResponse, error) {
	var q query.Query
	if req.Query == "" {
		q = bleve.NewMatchAllQuery()
	} else {
		q = bleve.NewMatchQuery(req.Query)
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	offset := max(req.Offset, 0)

	res, err := r.idx.Search(bleve.NewSearchRequestOptions(q, limit, offset, false))
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
