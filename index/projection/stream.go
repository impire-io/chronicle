package projection

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
)

// Stream answers one query as a streamed reply (design 12 § the streamed
// reply): the items in chunks under the byte budget, then the trailer.
// Every index kind's query endpoint answers through it, so the wire form
// exists once. A failure to publish mid-stream ends the stream with the
// micro error headers, which the client raises after the items it
// already yielded.
func Stream[T any](req micro.Request, items []T, trailer any) {
	w := contract.NewChunkWriter(func(data []byte, h nats.Header) error {
		return req.Respond(data, micro.WithHeaders(micro.Headers(h)))
	})
	for _, item := range items {
		if err := w.Item(item); err != nil {
			_ = req.Error(contract.CodeInternal, err.Error(), nil)
			return
		}
	}
	if err := w.End(trailer); err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
	}
}
