package contract

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
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

	// HdrOpAttester is the public key of the node that attested the
	// author (decision 0057): present only on an operation written through
	// the node, whose subject the server bound to the caller. HdrOpSignature
	// is that node's Ed25519 signature over the record's signed form,
	// base64. An operation without the two carries the writer's word —
	// asserted authorship, which is every operation written directly.
	HdrOpAttester  = "Op-Attester"
	HdrOpSignature = "Op-Signature"

	// EnvelopeVersion is the current Op-Version value, expected to stay "1"
	// for a long time. Vocabulary growth never touches it, and neither did
	// the attestation headers: an operation without them is the same record.
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

	// Attester and Signature are the attestation headers (0057): the
	// node's public key and its signature over SignedForm. Empty on an
	// operation written directly.
	Attester  string // Op-Attester
	Signature string // Op-Signature, base64

	Subject string // the exact subject the op lives on
	Seq     uint64 // the stream sequence — the only order
	Payload []byte // data only

	// Verified is a reader's finding, never a header: the signature
	// verified against an attester key the store's META lists. False for
	// every operation written directly, and for every one written before
	// the attestation existed.
	Verified bool
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
	if o.Attester != "" && o.Signature != "" {
		h.Set(HdrOpAttester, o.Attester)
		h.Set(HdrOpSignature, o.Signature)
	}
	return h
}

// SignedForm is what the attester signs (0057): the subject, the message
// id, the five record headers and the payload's SHA-256, one per line.
// The subject is in it so an attested operation cannot be moved to
// another instance; the id and the digest so it cannot be altered. Op-Ts
// is in its header form and the version defaulted, so a reader rebuilds
// the form from the headers alone.
func (o Op) SignedForm() []byte {
	sum := sha256.Sum256(o.Payload)
	v := o.Version
	if v == "" {
		v = EnvelopeVersion
	}
	ts := ""
	if !o.Ts.IsZero() {
		ts = o.Ts.UTC().Format(time.RFC3339Nano)
	}
	return []byte(strings.Join([]string{
		o.Subject, o.ID, o.Type, o.Author, strings.Join(o.Parents, ","), ts, v, hex.EncodeToString(sum[:]),
	}, "\n"))
}

// Attest signs the operation with the attester's key pair and stamps the
// two attestation values on it — the node's act, after it has bound the
// author to the caller.
func (o *Op) Attest(kp nkeys.KeyPair) error {
	pub, err := kp.PublicKey()
	if err != nil {
		return fmt.Errorf("attester public key: %w", err)
	}
	sig, err := kp.Sign(o.SignedForm())
	if err != nil {
		return fmt.Errorf("sign operation: %w", err)
	}
	o.Attester = pub
	o.Signature = base64.StdEncoding.EncodeToString(sig)
	return nil
}

// VerifyAttestation reports whether the signature verifies against the
// attester public key the operation names. It checks the signature only:
// whether that key is one to trust — an attester key in the store's META
// — is the reader's question, answered before this one.
func (o Op) VerifyAttestation() bool {
	if o.Attester == "" || o.Signature == "" {
		return false
	}
	pub, err := nkeys.FromPublicKey(o.Attester)
	if err != nil {
		return false
	}
	sig, err := base64.StdEncoding.DecodeString(o.Signature)
	if err != nil {
		return false
	}
	return pub.Verify(o.SignedForm(), sig) == nil
}

// ParseOp reads the operation record out of a message's headers. A missing
// or malformed Op-Ts stays zero: timestamps are testimony, not evidence.
func ParseOp(subject string, seq uint64, header nats.Header, payload []byte) Op {
	op := Op{
		ID:        header.Get(HdrMsgID),
		Type:      header.Get(HdrOpType),
		Author:    header.Get(HdrOpAuthor),
		Parents:   header.Values(HdrOpParents),
		Version:   header.Get(HdrOpVersion),
		Attester:  header.Get(HdrOpAttester),
		Signature: header.Get(HdrOpSignature),
		Subject:   subject,
		Seq:       seq,
		Payload:   payload,
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
