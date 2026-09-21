package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
)

// ErrNoResponder is a request nobody answered: the node, or the indexer
// serving the index, is not running for this account.
var ErrNoResponder = errors.New("no responder")

// ErrStreamStalled is a streamed reply whose next message did not arrive
// within the contract's stall — no trailer came, so the result is an
// error, never an empty one.
var ErrStreamStalled = errors.New("streamed reply stalled")

// ErrStreamGap is a streamed reply whose chunks did not arrive in order,
// or whose trailer counted more items than were received: a lost chunk,
// reported rather than repaired. Re-request.
var ErrStreamGap = errors.New("streamed reply gap")

// Streamed is one streamed reply (design 12 § the streamed reply): the
// items as they arrive, and the trailer once they have all arrived.
type Streamed[Item, Trailer any] struct {
	items   iter.Seq2[Item, error]
	trailer Trailer
	done    bool
	count   uint64
}

// Items iterates the reply's items in order. It ends when the trailer
// arrives, or yields the error that ended the stream — after every item
// already received. Iterate it once.
func (s *Streamed[Item, Trailer]) Items() iter.Seq2[Item, error] { return s.items }

// Trailer is the reply's trailer — the endpoint's totals — readable once
// Items has ended normally; ok is false before that.
func (s *Streamed[Item, Trailer]) Trailer() (trailer Trailer, ok bool) { return s.trailer, s.done }

// Count is how many items Items has yielded so far.
func (s *Streamed[Item, Trailer]) Count() uint64 { return s.count }

// RequestStream asks one streamed-reply endpoint and reads its chunks: the
// client half of the contract's form — an inbox with the shared pending
// limits, the stall as the wait for every message, no responder as a
// typed error, the micro error headers raised as a ServiceError, the
// chunk numbering verified, the trailer closing the stream. Exported so a
// build that adds streamed verbs speaks the same way.
func RequestStream[Req, Item, Trailer any](ctx context.Context, nc *nats.Conn, subject string, req Req) *Streamed[Item, Trailer] {
	s := &Streamed[Item, Trailer]{}
	s.items = func(yield func(Item, error) bool) {
		var zero Item
		fail := func(err error) { yield(zero, fmt.Errorf("%s: %w", subject, err)) }

		data, err := json.Marshal(req)
		if err != nil {
			fail(fmt.Errorf("marshal request: %w", err))
			return
		}
		inbox := nc.NewRespInbox()
		sub, err := nc.SubscribeSync(inbox)
		if err != nil {
			fail(fmt.Errorf("subscribe reply inbox: %w", err))
			return
		}
		defer func() { _ = sub.Unsubscribe() }()
		if err := sub.SetPendingLimits(contract.StreamPendingMsgs, contract.StreamPendingBytes); err != nil {
			fail(fmt.Errorf("size reply inbox: %w", err))
			return
		}
		if err := nc.PublishRequest(subject, inbox, data); err != nil {
			fail(fmt.Errorf("publish request: %w", err))
			return
		}

		var expect uint64 = 1
		for {
			msg, err := nextWithin(ctx, sub)
			if err != nil {
				fail(err)
				return
			}
			if msg.Header.Get("Status") == "503" {
				// Defensive: a status reply that reached us as a message.
				fail(fmt.Errorf("%w (is the node running for this account?)", ErrNoResponder))
				return
			}
			if code := msg.Header.Get(micro.ErrorCodeHeader); code != "" {
				yield(zero, &ServiceError{Code: code, Desc: msg.Header.Get(micro.ErrorHeader)})
				return
			}
			n, perr := strconv.ParseUint(msg.Header.Get(contract.HdrChunk), 10, 64)
			if perr != nil || n != expect {
				fail(fmt.Errorf("%w: got chunk %q, want %d", ErrStreamGap, msg.Header.Get(contract.HdrChunk), expect))
				return
			}
			expect++
			if end := msg.Header.Get(contract.HdrEnd); end != "" {
				sent, _ := strconv.ParseUint(end, 10, 64)
				if sent != s.count {
					fail(fmt.Errorf("%w: the trailer counts %d items, %d arrived", ErrStreamGap, sent, s.count))
					return
				}
				if len(msg.Data) > 0 {
					if err := json.Unmarshal(msg.Data, &s.trailer); err != nil {
						fail(fmt.Errorf("decode trailer: %w", err))
						return
					}
				}
				s.done = true
				return
			}
			var page []Item
			if err := json.Unmarshal(msg.Data, &page); err != nil {
				fail(fmt.Errorf("decode chunk %d: %w", n, err))
				return
			}
			for _, item := range page {
				s.count++
				if !yield(item, nil) {
					return
				}
			}
		}
	}
	return s
}

// nextWithin reads the inbox's next message within the contract's stall,
// the caller's context bounding the whole.
func nextWithin(ctx context.Context, sub *nats.Subscription) (*nats.Msg, error) {
	wait, cancel := context.WithTimeout(ctx, contract.StreamStall)
	defer cancel()
	msg, err := sub.NextMsgWithContext(wait)
	if err == nil {
		return msg, nil
	}
	if errors.Is(err, nats.ErrNoResponders) {
		// The client library turns the server's 503 status reply into
		// this error before the message reaches us.
		return nil, fmt.Errorf("%w (is the node running for this account?)", ErrNoResponder)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, fmt.Errorf("%w after %s", ErrStreamStalled, contract.StreamStall)
	}
	return nil, err
}
