package semantic

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// chunk is one embeddable piece of a thing's state: which field it came
// from, and the text — split to the byte budget, because whole documents
// silently overflow model windows (0016, the hits item-21 lesson).
type chunk struct {
	field string
	text  string
}

// chunksFor selects and splits a thing's meaningful text. Fields empty
// means every string field, recursively, dotted paths as names —
// search's own rule.
func chunksFor(state json.RawMessage, fields []string, budget int) []chunk {
	if len(state) == 0 {
		return nil
	}
	var doc any
	if err := json.Unmarshal(state, &doc); err != nil {
		return nil
	}
	var texts []chunk
	if len(fields) == 0 {
		collectStrings("", doc, &texts)
	} else {
		for _, f := range fields {
			for _, s := range evalStrings(doc, strings.Split(f, ".")) {
				texts = append(texts, chunk{field: f, text: s})
			}
		}
	}
	var out []chunk
	for _, t := range texts {
		for _, piece := range split(t.text, budget) {
			out = append(out, chunk{field: t.field, text: piece})
		}
	}
	return out
}

// collectStrings walks every string field, naming it by dotted path.
func collectStrings(path string, v any, out *[]chunk) {
	switch node := v.(type) {
	case string:
		if node != "" {
			*out = append(*out, chunk{field: path, text: node})
		}
	case map[string]any:
		for k, child := range node {
			p := k
			if path != "" {
				p = path + "." + k
			}
			collectStrings(p, child, out)
		}
	case []any:
		for _, el := range node {
			collectStrings(path, el, out)
		}
	}
}

// evalStrings walks one dotted path: objects by field, arrays
// element-wise, strings at the leaf — the graph kind's grammar.
func evalStrings(v any, segs []string) []string {
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
				out = append(out, evalStrings(el, nil)...)
			}
			return out
		}
		return nil
	}
	switch node := v.(type) {
	case map[string]any:
		return evalStrings(node[segs[0]], segs[1:])
	case []any:
		var out []string
		for _, el := range node {
			out = append(out, evalStrings(el, segs)...)
		}
		return out
	}
	return nil
}

// split cuts text to the byte budget on rune boundaries, preferring the
// last whitespace inside the window so words survive whole.
func split(text string, budget int) []string {
	if budget <= 0 {
		budget = contract.SemanticChunkBytes
	}
	var out []string
	for len(text) > 0 {
		if len(text) <= budget {
			out = append(out, text)
			break
		}
		cut := budget
		for cut > 0 && (text[cut]&0xC0) == 0x80 {
			cut-- // never split a rune
		}
		if ws := strings.LastIndexAny(text[:cut], " \t\n"); ws > budget/2 {
			cut = ws
		}
		out = append(out, strings.TrimSpace(text[:cut]))
		text = strings.TrimSpace(text[cut:])
	}
	return out
}

// chunkVec is one embedded chunk.
type chunkVec struct {
	field string
	vec   []float64
}

// semanticRun is one fold pass's engine: folded text queued for the
// embed worker, embedded vectors served — the two stages 0016 separates
// so the fold never waits on the provider. Previous vectors keep serving
// while a re-embed is queued: an outage stalls freshness of meaning,
// never the index's grip on the log.
type semanticRun struct {
	provider *provider
	model    string
	fields   []string
	budget   int
	log      string
	logger   *slog.Logger

	mu       sync.Mutex
	embedded map[string][]chunkVec
	pending  map[string][]chunk
	gen      map[string]uint64

	notify chan struct{}
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newSemanticRun(log string, p *provider, model string, fields []string, budget int, logger *slog.Logger) *semanticRun {
	ctx, cancel := context.WithCancel(context.Background())
	r := &semanticRun{
		provider: p,
		model:    model,
		fields:   fields,
		budget:   budget,
		log:      log,
		logger:   logger,
		embedded: map[string][]chunkVec{},
		pending:  map[string][]chunk{},
		gen:      map[string]uint64{},
		notify:   make(chan struct{}, 1),
		cancel:   cancel,
	}
	r.wg.Add(1)
	go r.worker(ctx)
	return r
}

// Close stops the embed worker; the projection closes a run it retires.
func (r *semanticRun) Close() error {
	r.cancel()
	r.wg.Wait()
	return nil
}

// Upsert queues a thing's freshly folded text for embedding. The fold
// never waits: the provider is asked by the worker, later.
func (r *semanticRun) Upsert(thing string, state json.RawMessage) {
	chunks := chunksFor(state, r.fields, r.budget)
	r.mu.Lock()
	r.gen[thing]++
	if len(chunks) == 0 {
		delete(r.pending, thing)
		delete(r.embedded, thing)
	} else {
		r.pending[thing] = chunks
	}
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

// worker drains pending things one at a time, with backoff on provider
// trouble. A thing re-folded mid-embed keeps its queue slot: the
// generation guard drops the stale vectors.
func (r *semanticRun) worker(ctx context.Context) {
	defer r.wg.Done()
	backoff := time.Second
	for {
		thing, chunks, gen, ok := r.next()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-r.notify:
				continue
			}
		}
		texts := make([]string, len(chunks))
		for i, c := range chunks {
			texts[i] = c.text
		}
		embedCtx, cancel := context.WithTimeout(ctx, time.Minute)
		vecs, err := r.provider.Embed(embedCtx, r.model, texts)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Warn("semantic index: embed failed; retrying", "log", r.log, "thing", thing, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second

		cvs := make([]chunkVec, len(chunks))
		for i, c := range chunks {
			cvs[i] = chunkVec{field: c.field, vec: vecs[i]}
		}
		r.mu.Lock()
		if r.gen[thing] == gen {
			r.embedded[thing] = cvs
			delete(r.pending, thing)
		}
		// A newer fold re-queued the thing meanwhile: its pending entry
		// stands and these vectors are stale — dropped.
		r.mu.Unlock()
	}
}

// next takes one pending thing, or reports none.
func (r *semanticRun) next() (string, []chunk, uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for thing, chunks := range r.pending {
		return thing, chunks, r.gen[thing], true
	}
	return "", nil, 0, false
}

// unembedded is the honest-degradation count every reply carries.
func (r *semanticRun) unembedded() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}

// query embeds the text once and scores things by their best chunk —
// max, not mean (0016) — carrying the field the meaning matched in.
func (r *semanticRun) query(ctx context.Context, req client.SemanticQueryRequest) (client.SemanticQueryResponse, error) {
	vecs, err := r.provider.Embed(ctx, r.model, []string{req.Text})
	if err != nil {
		return client.SemanticQueryResponse{}, err
	}
	qv := vecs[0]

	r.mu.Lock()
	hits := make([]client.SemanticHit, 0, len(r.embedded))
	for thing, cvs := range r.embedded {
		best := client.SemanticHit{Thing: thing, Score: math.Inf(-1)}
		for _, cv := range cvs {
			if s := cosine(qv, cv.vec); s > best.Score {
				best.Score = s
				best.Field = cv.field
			}
		}
		if !math.IsInf(best.Score, -1) {
			hits = append(hits, best)
		}
	}
	pending := len(r.pending)
	r.mu.Unlock()

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].Thing < hits[j].Thing
	})
	total := uint64(len(hits))
	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	offset := max(req.Offset, 0)
	if offset > len(hits) {
		offset = len(hits)
	}
	end := min(offset+limit, len(hits))
	return client.SemanticQueryResponse{Hits: hits[offset:end], Total: total, Unembedded: pending}, nil
}

// cosine is the similarity of two vectors; mismatched or zero vectors
// score zero.
func cosine(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}
