package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
)

// The streamed reply's headers (design 12 § the streamed reply): one
// request, chunks and a trailer on the reply inbox from one responder.
const (
	// HdrChunk is a message's position in the stream, from 1, increasing
	// by one; the trailer continues the numbering. A gap is an error.
	HdrChunk = "Chron-Chunk"
	// HdrEnd marks the trailer and carries the count of items sent across
	// every chunk before it. The trailer is the only normal end.
	HdrEnd = "Chron-End"
	// ChunkByteBudget bounds a chunk's payload — well below the server's
	// default payload cap.
	ChunkByteBudget = 256 * 1024
)

// ChunkWriter answers one request as a streamed reply: items buffered
// into chunks under the byte budget, then the trailer. The responder
// supplies the publish — a micro request's Respond with headers.
type ChunkWriter struct {
	respond func(data []byte, header nats.Header) error
	budget  int
	chunk   uint64
	items   uint64
	buf     bytes.Buffer
	n       int
}

// NewChunkWriter binds a writer to one request's reply path.
func NewChunkWriter(respond func(data []byte, header nats.Header) error) *ChunkWriter {
	return &ChunkWriter{respond: respond, budget: ChunkByteBudget}
}

// Item marshals one item into the current chunk, flushing the chunk first
// when the item would overflow the budget.
func (w *ChunkWriter) Item(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal item: %w", err)
	}
	if w.n > 0 && w.buf.Len()+len(b)+2 > w.budget {
		if err := w.flush(); err != nil {
			return err
		}
	}
	if w.n == 0 {
		w.buf.WriteByte('[')
	} else {
		w.buf.WriteByte(',')
	}
	w.buf.Write(b)
	w.n++
	w.items++
	return nil
}

func (w *ChunkWriter) flush() error {
	if w.n == 0 {
		return nil
	}
	w.buf.WriteByte(']')
	w.chunk++
	h := nats.Header{}
	h.Set(HdrChunk, strconv.FormatUint(w.chunk, 10))
	data := append([]byte(nil), w.buf.Bytes()...)
	w.buf.Reset()
	w.n = 0
	return w.respond(data, h)
}

// End flushes the last chunk and sends the trailer: HdrEnd with the item
// count and the endpoint's trailer object as payload — nil for none.
func (w *ChunkWriter) End(trailer any) error {
	if err := w.flush(); err != nil {
		return err
	}
	w.chunk++
	h := nats.Header{}
	h.Set(HdrChunk, strconv.FormatUint(w.chunk, 10))
	h.Set(HdrEnd, strconv.FormatUint(w.items, 10))
	var data []byte
	if trailer != nil {
		b, err := json.Marshal(trailer)
		if err != nil {
			return fmt.Errorf("marshal trailer: %w", err)
		}
		data = b
	}
	return w.respond(data, h)
}
