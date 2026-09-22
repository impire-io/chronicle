package contract

import "time"

// ContractVersion is the version of the SDK contract this build speaks —
// the artifact's version, declared by every SDK that implements it
// (design 12 § the artifact). Additive changes bump minor; a changed
// meaning bumps major. Unrelated to the envelope's Op-Version.
const ContractVersion = "1.0.0"

// The interaction shapes an SDK may implement (design 12 § the shapes).
// Every shape is describable in the artifact; an SDK implements the ones
// the wire uses and refuses, by name, an interaction whose shape it does
// not.
const (
	// ShapeRequestReply — one request, one reply: the control verbs.
	ShapeRequestReply = "request-reply"
	// ShapeStreamedReply — one request, chunks and a trailer from one
	// responder: the index queries.
	ShapeStreamedReply = "streamed-reply"
	// ShapeScatterGather — one request, any responder may answer, ends on
	// a deadline or a count: an application's own, never chronicle's wire.
	ShapeScatterGather = "scatter-gather"
	// ShapeBatchPublish — many messages, one commit ack: the server's
	// atomic batch publish, described, unbuilt until a consumer exists.
	ShapeBatchPublish = "batch-publish"
	// ShapePublish — a guarded JetStream publish: every append.
	ShapePublish = "publish"
	// ShapeSubscribe — a resumable JetStream read, never a core
	// subscription: history, the fold, the live surface.
	ShapeSubscribe = "subscribe"
)

// Shapes is the vocabulary in the artifact's order.
var Shapes = []string{ShapeRequestReply, ShapeStreamedReply, ShapeScatterGather, ShapeBatchPublish, ShapePublish, ShapeSubscribe}

// The client defaults every SDK shares (design 12 § the streamed reply):
// neither is an SDK's own choice.
const (
	// StreamStall is the longest a client waits between the messages of a
	// streamed reply — and for its first — before the stream is an error.
	StreamStall = 5 * time.Second
	// StreamPendingMsgs and StreamPendingBytes size the reply inbox so a
	// burst of chunks does not drop.
	StreamPendingMsgs  = 65536
	StreamPendingBytes = 64 * 1024 * 1024
)
