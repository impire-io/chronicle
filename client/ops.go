package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/impire-io/chronicle/contract"
)

// ErrThingExists is a birth refused because the thing already has history —
// and not from a retry of this very op.
var ErrThingExists = errors.New("thing already exists")

// ErrStaleVersion is a save refused because the log moved past the version:
// something landed on the subject after upTo, so the saved state no longer
// covers the history it would destroy. Re-read, re-fold, retry.
var ErrStaleVersion = errors.New("the log moved past this version")

// ErrThingMoved is a guarded append refused because the thing's history
// moved past the guard: something landed on the subject after the expected
// seq, so whatever the writer validated no longer holds. Re-read,
// re-validate, retry (0018).
var ErrThingMoved = errors.New("the thing moved past the guard")

// Ack is where an accepted op landed.
type Ack struct {
	OpID string
	Seq  uint64
}

// AppendOpt adjusts one append.
type AppendOpt func(*appendOpts)

type appendOpts struct {
	parents     []string
	opID        string
	expectedSeq *uint64
}

// WithParents records the op IDs the writer had seen — the DAG edges.
func WithParents(parents ...string) AppendOpt {
	return func(o *appendOpts) { o.parents = parents }
}

// WithOpID pins the op ID — the retry key. Callers that may retry across
// process restarts make it once and keep it.
func WithOpID(id string) AppendOpt {
	return func(o *appendOpts) { o.opID = id }
}

// WithExpectedSeq arms an append with the expected-sequence guard (0018):
// the publish carries the last stream sequence the writer observed on the
// thing's subject, and the server refuses it if anything landed since —
// CAS per exact subject, server-enforced. A refusal is ErrThingMoved:
// re-read, re-validate, retry. The exactness recipe yields the value
// (State's seq, advanced by FoldTail). Append only: birth guards at 0 by
// definition and SaveVersion guards at upTo.
func WithExpectedSeq(seq uint64) AppendOpt {
	return func(o *appendOpts) { o.expectedSeq = &seq }
}

// CreateThing births a thing: publishing its first snapshot with the
// create-if-absent guard, server-enforced, no lock (pattern § 5.1). A
// retried birth whose op already landed reports success; a birth of a
// thing someone else created reports ErrThingExists.
func (c *Client) CreateThing(ctx context.Context, log, thing string, state json.RawMessage, opts ...AppendOpt) (Ack, error) {
	if err := contract.ValidateLogName(log); err != nil {
		return Ack{}, err
	}
	if err := contract.ValidateThing(thing); err != nil {
		return Ack{}, err
	}
	o := applyOpts(opts)
	if o.expectedSeq != nil {
		return Ack{}, errors.New("WithExpectedSeq: a birth guards at 0 by definition")
	}
	if state == nil {
		state = json.RawMessage(`{}`)
	}
	if err := c.types.preflightSnapshot(ctx, log, thing, state); err != nil {
		return Ack{}, err
	}
	payload, err := json.Marshal(contract.Snapshot{State: state, Frontier: []string{}})
	if err != nil {
		return Ack{}, fmt.Errorf("snapshot payload: %w", err)
	}

	subject := contract.OpsSubject(log, thing)
	msg := nats.NewMsg(subject)
	msg.Header = contract.Op{
		ID:      o.opID,
		Type:    contract.OpTypeSnapshot,
		Author:  c.author,
		Parents: o.parents,
		Ts:      time.Now(),
	}.Header()
	msg.Header.Set(contract.HdrExpectedLastSubjSeq, "0")
	msg.Data = payload

	lock := c.subjectLock(subject)
	lock.Lock()
	defer lock.Unlock()
	ack, err := c.js.PublishMsg(ctx, msg)
	if err == nil {
		return Ack{OpID: o.opID, Seq: ack.Sequence}, nil
	}

	// The guard is evaluated before dedup (verified at build time, per
	// 0008): a retried birth also surfaces wrong-last-sequence. Read the
	// subject's last op and compare IDs to tell "my birth landed" from
	// "someone else was first".
	if guardRefused(err) {
		landed, ok, lerr := c.ownOpLanded(ctx, log, subject, o.opID)
		if lerr != nil {
			return Ack{}, fmt.Errorf("birth refused and %w", errors.Join(lerr, err))
		}
		if ok {
			return landed, nil
		}
		return Ack{}, fmt.Errorf("%w: %s in %s", ErrThingExists, thing, log)
	}
	return Ack{}, fmt.Errorf("birth %s: %w", subject, err)
}

// CreateWith births a thing through a declared operation (0025): the
// constructor is an ordinary operation of the thing's type — the payload
// judged by that operation's schema — published with the same
// create-if-absent guard a snapshot birth carries. Birth is an invocation
// mode, not a marker on the operation. A retried birth whose op already
// landed reports success; a birth of a thing someone else created reports
// ErrThingExists.
func (c *Client) CreateWith(ctx context.Context, log, thing, opType string, payload []byte, opts ...AppendOpt) (Ack, error) {
	if err := contract.ValidateLogName(log); err != nil {
		return Ack{}, err
	}
	if err := contract.ValidateThing(thing); err != nil {
		return Ack{}, err
	}
	if opType == "" {
		return Ack{}, errors.New("op type: must not be empty")
	}
	if opType == contract.OpTypeSnapshot {
		return Ack{}, errors.New("create with snapshot: CreateThing is the snapshot birth")
	}
	o := applyOpts(opts)
	if o.expectedSeq != nil {
		return Ack{}, errors.New("WithExpectedSeq: a birth guards at 0 by definition")
	}
	if payload == nil {
		payload = []byte(`{}`)
	}
	if err := c.types.preflightAppend(ctx, log, thing, opType, payload); err != nil {
		return Ack{}, err
	}

	subject := contract.OpsSubject(log, thing)
	msg := nats.NewMsg(subject)
	msg.Header = contract.Op{
		ID:      o.opID,
		Type:    opType,
		Author:  c.author,
		Parents: o.parents,
		Ts:      time.Now(),
	}.Header()
	msg.Header.Set(contract.HdrExpectedLastSubjSeq, "0")
	msg.Data = payload

	lock := c.subjectLock(subject)
	lock.Lock()
	defer lock.Unlock()
	ack, err := c.js.PublishMsg(ctx, msg)
	if err == nil {
		return Ack{OpID: o.opID, Seq: ack.Sequence}, nil
	}
	if guardRefused(err) {
		landed, ok, lerr := c.ownOpLanded(ctx, log, subject, o.opID)
		if lerr != nil {
			return Ack{}, fmt.Errorf("birth refused and %w", errors.Join(lerr, err))
		}
		if ok {
			return landed, nil
		}
		return Ack{}, fmt.Errorf("%w: %s in %s", ErrThingExists, thing, log)
	}
	return Ack{}, fmt.Errorf("birth %s: %w", subject, err)
}

// SaveVersion publishes an app-materialised snapshot that replaces the
// thing's history in one write (pattern § 5.2) — the app-initiated rollup.
// This is the application's call, so it may compact a history holding
// effect-none ops the node's own triggers refuse to touch: the app folded
// that history with its own semantics and supplies the resulting state,
// the frontier (the op IDs new ops should use as parents), and upTo — the
// stream seq of the last op on the subject the state covers. The
// expected-sequence guard makes it race-safe: if anything landed after
// upTo the server refuses, nothing changes, and the caller re-reads and
// retries (ErrStaleVersion). A retried save whose op already landed
// reports success, like CreateThing.
func (c *Client) SaveVersion(ctx context.Context, log, thing string, state json.RawMessage, frontier []string, upTo uint64, opts ...AppendOpt) (Ack, error) {
	if err := contract.ValidateLogName(log); err != nil {
		return Ack{}, err
	}
	if err := contract.ValidateThing(thing); err != nil {
		return Ack{}, err
	}
	if upTo == 0 {
		return Ack{}, errors.New("upTo: the seq of the last op the state covers; birth is CreateThing")
	}
	o := applyOpts(opts)
	if o.expectedSeq != nil {
		return Ack{}, errors.New("WithExpectedSeq: a save guards at upTo")
	}
	if state == nil {
		state = json.RawMessage(`{}`)
	}
	if frontier == nil {
		frontier = []string{}
	}
	if err := c.types.preflightSnapshot(ctx, log, thing, state); err != nil {
		return Ack{}, err
	}
	payload, err := json.Marshal(contract.Snapshot{State: state, Frontier: frontier})
	if err != nil {
		return Ack{}, fmt.Errorf("snapshot payload: %w", err)
	}

	subject := contract.OpsSubject(log, thing)
	msg := nats.NewMsg(subject)
	msg.Header = contract.Op{
		ID:      o.opID,
		Type:    contract.OpTypeSnapshot,
		Author:  c.author,
		Parents: o.parents,
		Ts:      time.Now(),
	}.Header()
	msg.Header.Set(contract.HdrRollup, contract.RollupSubject)
	msg.Header.Set(contract.HdrExpectedLastSubjSeq, strconv.FormatUint(upTo, 10))
	msg.Data = payload

	lock := c.subjectLock(subject)
	lock.Lock()
	defer lock.Unlock()
	ack, err := c.js.PublishMsg(ctx, msg)
	if err == nil {
		return Ack{OpID: o.opID, Seq: ack.Sequence}, nil
	}

	// The guard fires before dedup, so a retried save also surfaces
	// wrong-last-sequence: read the subject's last op and compare IDs to
	// tell "my save landed" from "the log moved".
	if guardRefused(err) {
		landed, ok, lerr := c.ownOpLanded(ctx, log, subject, o.opID)
		if lerr != nil {
			return Ack{}, fmt.Errorf("save refused and %w", errors.Join(lerr, err))
		}
		if ok {
			return landed, nil
		}
		return Ack{}, fmt.Errorf("%w: %s in %s", ErrStaleVersion, thing, log)
	}
	return Ack{}, fmt.Errorf("save version %s: %w", subject, err)
}

// Append publishes one operation — a direct JetStream publish, nothing in
// between. Pre-flight validates the payload against the log's declared
// schema for the op type, when one exists; the log's vocabulary is
// discoverable, not enforced at the wire (0008 point 1). WithExpectedSeq
// arms the optional expected-sequence guard (0018) — read-validate-append
// with the server as the only arbiter; unguarded stays the default.
func (c *Client) Append(ctx context.Context, log, thing, opType string, payload []byte, opts ...AppendOpt) (Ack, error) {
	if err := contract.ValidateLogName(log); err != nil {
		return Ack{}, err
	}
	if err := contract.ValidateThing(thing); err != nil {
		return Ack{}, err
	}
	if opType == "" {
		return Ack{}, errors.New("op type: must not be empty")
	}
	o := applyOpts(opts)

	if err := c.types.preflightAppend(ctx, log, thing, opType, payload); err != nil {
		return Ack{}, err
	}

	subject := contract.OpsSubject(log, thing)
	msg := nats.NewMsg(subject)
	msg.Header = contract.Op{
		ID:      o.opID,
		Type:    opType,
		Author:  c.author,
		Parents: o.parents,
		Ts:      time.Now(),
	}.Header()
	if o.expectedSeq != nil {
		msg.Header.Set(contract.HdrExpectedLastSubjSeq, strconv.FormatUint(*o.expectedSeq, 10))
	}
	msg.Data = payload

	lock := c.subjectLock(subject)
	lock.Lock()
	defer lock.Unlock()
	ack, err := c.js.PublishMsg(ctx, msg)
	if err == nil {
		return Ack{OpID: o.opID, Seq: ack.Sequence}, nil
	}

	// The guard fires before dedup, so a retried guarded append also
	// surfaces wrong-last-sequence: read the subject's last op and compare
	// IDs to tell "my op landed" from "the thing moved".
	if o.expectedSeq != nil && guardRefused(err) {
		landed, ok, lerr := c.ownOpLanded(ctx, log, subject, o.opID)
		if lerr != nil {
			return Ack{}, fmt.Errorf("append refused and %w", errors.Join(lerr, err))
		}
		if ok {
			return landed, nil
		}
		return Ack{}, fmt.Errorf("%w: %s in %s", ErrThingMoved, thing, log)
	}
	return Ack{}, fmt.Errorf("append %s: %w", subject, err)
}

// guardRefused reports whether err is the server's wrong-last-sequence
// refusal — the expected-sequence guard firing.
func guardRefused(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}

// ownOpLanded tells a retry from a conflict after a guard refusal: the
// guard fires before dedup, so a retried guarded publish surfaces
// wrong-last-sequence too. If the subject's last op carries opID, the
// earlier attempt landed and the refusal was dedup's echo.
func (c *Client) ownOpLanded(ctx context.Context, log, subject, opID string) (Ack, bool, error) {
	stream, err := c.js.Stream(ctx, contract.StreamName(log))
	if err != nil {
		return Ack{}, false, fmt.Errorf("stream unreadable: %w", err)
	}
	last, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return Ack{}, false, fmt.Errorf("last op unreadable: %w", err)
	}
	if last.Header.Get(contract.HdrMsgID) == opID {
		return Ack{OpID: opID, Seq: last.Sequence}, true, nil
	}
	return Ack{}, false, nil
}

func applyOpts(opts []AppendOpt) appendOpts {
	o := appendOpts{}
	for _, apply := range opts {
		apply(&o)
	}
	if o.opID == "" {
		o.opID = nuid.Next()
	}
	return o
}

// ErrSchemaViolation is a pre-flight refusal: the payload does not satisfy
// the declared schema.
var ErrSchemaViolation = errors.New("payload fails the declared schema")

// ErrUndeclaredAspect is a pre-flight refusal: the thing's prefix resolves
// to a parent type that does not declare the created type as an aspect
// (0022 § 3). The fold would mark the subject; the SDK refuses first.
var ErrUndeclaredAspect = errors.New("undeclared aspect")

// ErrUndefinedOperation is a pre-flight refusal: the thing's type defines
// no such operation (0021 § 2).
var ErrUndefinedOperation = errors.New("operation not defined on the type")

// typeCache serves pre-flight: type records are read fresh from META —
// latest declaration wins — and their compiled schemas kept per
// (log, type, revision, op). It reads through the KV bucket — the member
// baseline's read side — and never writes.
type typeCache struct {
	js jetstream.JetStream

	mu       sync.Mutex
	meta     jetstream.KeyValue
	compiled map[string]*jsonschema.Schema
}

func newTypeCache(js jetstream.JetStream) *typeCache {
	return &typeCache{js: js, compiled: map[string]*jsonschema.Schema{}}
}

func (t *typeCache) bucket(ctx context.Context) (jetstream.KeyValue, error) {
	t.mu.Lock()
	meta := t.meta
	t.mu.Unlock()
	if meta != nil {
		return meta, nil
	}
	meta, err := t.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return nil, fmt.Errorf("open META: %w", err)
	}
	t.mu.Lock()
	t.meta = meta
	t.mu.Unlock()
	return meta, nil
}

// resolve walks the thing tail against the log's declared types (0021
// § 4, 0022 § 2) — the same pair walk every projection runs.
func (t *typeCache) resolve(ctx context.Context, log, thing string) (contract.Resolution, error) {
	meta, err := t.bucket(ctx)
	if err != nil {
		return contract.Resolution{}, err
	}
	return contract.ResolveTail(thing, func(name string) (*contract.TypeRecord, bool, error) {
		entry, err := meta.Get(ctx, contract.MetaLogType(log, name))
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("read type record %s: %w", name, err)
		}
		var rec contract.TypeRecord
		if err := json.Unmarshal(entry.Value(), &rec); err != nil {
			return nil, false, fmt.Errorf("decode type record %s: %w", name, err)
		}
		return &rec, true, nil
	})
}

func (t *typeCache) compile(key string, raw json.RawMessage) (*jsonschema.Schema, error) {
	t.mu.Lock()
	cached, ok := t.compiled[key]
	t.mu.Unlock()
	if ok {
		return cached, nil
	}
	compiled, err := contract.CompileSchema(raw)
	if err != nil {
		return nil, fmt.Errorf("compile schema %s: %w", key, err)
	}
	t.mu.Lock()
	t.compiled[key] = compiled
	t.mu.Unlock()
	return compiled, nil
}

// preflightAppend refuses an op a typed thing's type does not define and
// validates the payload where it does; an untyped thing publishes as-is —
// readers stay tolerant either way.
func (t *typeCache) preflightAppend(ctx context.Context, log, thing, opType string, payload []byte) error {
	if opType == contract.OpTypeSnapshot {
		// The pattern's own vocabulary, not the type's: any member may
		// publish a snapshot. Its state is validated like a birth's;
		// a malformed envelope is marked read-side, never gated.
		if snap, err := contract.ParseSnapshot(payload); err == nil {
			return t.preflightSnapshot(ctx, log, thing, snap.State)
		}
		return nil
	}
	res, err := t.resolve(ctx, log, thing)
	if err != nil {
		return err
	}
	switch res.Kind {
	case contract.ResolvedUndeclared:
		return fmt.Errorf("%w: %s", ErrUndeclaredAspect, res.Detail)
	case contract.ResolvedTyped:
		def, ok := res.Record.Operations[opType]
		if !ok {
			return fmt.Errorf("%w: type %q defines no operation %q", ErrUndefinedOperation, res.TypeName, opType)
		}
		sch, err := t.compile(fmt.Sprintf("%s/%s@%d/%s", log, res.TypeName, res.Record.Revision, opType), def.Schema)
		if err != nil {
			return err
		}
		var v any
		if err := json.Unmarshal(payload, &v); err != nil {
			return fmt.Errorf("%w: payload is not JSON: %v", ErrSchemaViolation, err)
		}
		if err := sch.Validate(v); err != nil {
			return fmt.Errorf("%w: %s %s: %v", ErrSchemaViolation, log, opType, err)
		}
	}
	return nil
}

// preflightSnapshot refuses a birth or save under a prefix whose parent
// type does not declare the created type (0022 § 3) and validates a typed
// thing's state against its thing schema (0021 § 3).
func (t *typeCache) preflightSnapshot(ctx context.Context, log, thing string, state json.RawMessage) error {
	res, err := t.resolve(ctx, log, thing)
	if err != nil {
		return err
	}
	switch res.Kind {
	case contract.ResolvedUndeclared:
		return fmt.Errorf("%w: %s", ErrUndeclaredAspect, res.Detail)
	case contract.ResolvedTyped:
		if len(res.Record.Schema) == 0 {
			return nil
		}
		sch, err := t.compile(fmt.Sprintf("%s/%s@%d", log, res.TypeName, res.Record.Revision), res.Record.Schema)
		if err != nil {
			return err
		}
		var v any
		if err := json.Unmarshal(state, &v); err != nil {
			return fmt.Errorf("%w: state is not JSON: %v", ErrSchemaViolation, err)
		}
		if err := sch.Validate(v); err != nil {
			return fmt.Errorf("%w: %s %s: state fails the thing schema: %v", ErrSchemaViolation, log, res.TypeName, err)
		}
	}
	return nil
}
