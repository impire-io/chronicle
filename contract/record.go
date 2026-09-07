package contract

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// The operation record lives in the headers; the payload is data only
// (pattern § 3, 0008 point 3). The Op- prefix is reserved: anything that
// forwards messages must pass Op-* headers through untouched.
const (
	// HdrMsgID is the op ID and the dedup key, made once by the writer and
	// kept across retries.
	HdrMsgID = "Nats-Msg-Id"
	// HdrOpType is the operation's kind, in the log's vocabulary.
	HdrOpType = "Op-Type"
	// HdrOpAuthor is the principal ID, stamped by the SDK from the
	// credentials it runs with.
	HdrOpAuthor = "Op-Author"
	// HdrOpParents carries the op IDs the writer had seen — the DAG edges,
	// recorded from day one. One header value per parent.
	HdrOpParents = "Op-Parents"
	// HdrOpTs is the author-claimed clock. Informational only; never used
	// for order.
	HdrOpTs = "Op-Ts"
	// HdrOpVersion is the envelope version — chronicle's own.
	HdrOpVersion = "Op-Version"

	// EnvelopeVersion is the current Op-Version value, expected to stay "1"
	// for a long time. Vocabulary growth never touches it.
	EnvelopeVersion = "1"

	// HdrExpectedLastSubjSeq is JetStream's per-subject CAS guard. Birth
	// sets it to zero: creating a thing is publishing its snapshot
	// create-if-absent, server-enforced, no lock (pattern § 5.1).
	HdrExpectedLastSubjSeq = "Nats-Expected-Last-Subject-Sequence"
	// HdrRollup marks a snapshot that replaces its subject's history in one
	// write. Only ever "sub" on a shared stream (pattern § 5.4).
	HdrRollup = "Nats-Rollup"
	// RollupSubject is the one sanctioned rollup scope.
	RollupSubject = "sub"
)

// Op is one operation as read back from a log.
type Op struct {
	ID      string    // Nats-Msg-Id
	Type    string    // Op-Type
	Author  string    // Op-Author
	Parents []string  // Op-Parents, one value per parent
	Ts      time.Time // Op-Ts, informational only
	Version string    // Op-Version

	Subject string // the exact subject the op lives on
	Seq     uint64 // the stream sequence — the only order
	Payload []byte // data only
}

// Header builds the operation record's headers.
func (o Op) Header() nats.Header {
	h := nats.Header{}
	h.Set(HdrMsgID, o.ID)
	h.Set(HdrOpType, o.Type)
	h.Set(HdrOpAuthor, o.Author)
	for _, p := range o.Parents {
		h.Add(HdrOpParents, p)
	}
	if !o.Ts.IsZero() {
		h.Set(HdrOpTs, o.Ts.UTC().Format(time.RFC3339Nano))
	}
	v := o.Version
	if v == "" {
		v = EnvelopeVersion
	}
	h.Set(HdrOpVersion, v)
	return h
}

// ParseOp reads the operation record out of a message's headers. A missing
// or malformed Op-Ts stays zero: timestamps are testimony, not evidence.
func ParseOp(subject string, seq uint64, header nats.Header, payload []byte) Op {
	op := Op{
		ID:      header.Get(HdrMsgID),
		Type:    header.Get(HdrOpType),
		Author:  header.Get(HdrOpAuthor),
		Parents: header.Values(HdrOpParents),
		Version: header.Get(HdrOpVersion),
		Subject: subject,
		Seq:     seq,
		Payload: payload,
	}
	if ts := header.Get(HdrOpTs); ts != "" {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			op.Ts = t
		}
	}
	return op
}

// Snapshot is the payload of a snapshot op: the full materialised state and
// the frontier — the op IDs a new op should use as parents (pattern § 5.1).
type Snapshot struct {
	State    json.RawMessage `json:"state"`
	Frontier []string        `json:"frontier"`
}

// StateValue is a state bucket entry: the materialised state plus the stream
// sequence it was folded to (03-meta-and-state.md § state buckets). A reader
// that must be exact reads the value, then folds the log from Seq+1.
type StateValue struct {
	Seq   uint64          `json:"seq"`
	State json.RawMessage `json:"state"`
}

// ParseSnapshot decodes a snapshot op's payload.
func ParseSnapshot(payload []byte) (Snapshot, error) {
	var s Snapshot
	if err := json.Unmarshal(payload, &s); err != nil {
		return Snapshot{}, fmt.Errorf("snapshot payload: %w", err)
	}
	return s, nil
}
