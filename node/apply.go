package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// handleApply is the verified write (decision 0057): one operation,
// carried exactly as a direct publish carries it — the record's headers,
// the guard, the payload — on CHRON.API.<principal>.APPLY.<store>.<tail>,
// a subject the member baseline lets that principal alone publish on. The
// node reads the author from the subject and refuses a record naming
// another, runs the SDK's own pre-flight and refuses what fails it — the
// hard refusal 0022 left additive by verb, so nothing invalid enters the
// log by this route — signs the record with its own key, publishes on the
// instance's exact subject with the caller's message id and guard, and
// answers with the sequence. Dedup and the CAS stay the server's; a guard
// miss is judged as the SDK judges it and returned as the conflict.
func (n *node) handleApply(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	caller, store, tail, err := client.ParseApplySubject(req.Subject())
	if err != nil {
		_ = req.Error(contract.CodeBadRequest, err.Error(), nil)
		return
	}
	if err := n.requireRole(ctx, caller, contract.RoleAdmin, contract.RoleWriter); err != nil {
		_ = req.Error(contract.CodeForbidden, err.Error(), nil)
		return
	}
	if err := contract.ValidateStoreName(store); err != nil {
		_ = req.Error(contract.CodeBadStoreName, err.Error(), nil)
		return
	}
	if err := contract.ValidateInstance(tail); err != nil {
		_ = req.Error(contract.CodeBadInstance, err.Error(), nil)
		return
	}
	if _, err := n.meta.Get(ctx, contract.MetaStoreConfig(store)); err != nil {
		_ = req.Error(contract.CodeNoSuchStore, fmt.Sprintf("store %q does not exist", store), nil)
		return
	}

	subject := contract.OpsSubject(store, tail)
	headers := req.Headers()
	op := contract.ParseOp(subject, 0, nats.Header(headers), req.Data())
	if op.ID == "" || op.Type == "" {
		_ = req.Error(contract.CodeBadRequest, "a verified write carries Nats-Msg-Id and Op-Type", nil)
		return
	}
	// The author is the caller's: a record naming another is refused, a
	// record naming nobody is stamped.
	if op.Author != "" && op.Author != caller {
		_ = req.Error(contract.CodeNotTheCaller, fmt.Sprintf("the record names author %q; the subject's caller is %q", op.Author, caller), nil)
		return
	}
	op.Author = caller
	op.Attester, op.Signature = "", ""

	if err := n.preflight.Preflight(ctx, store, tail, op.Type, op.Payload); err != nil {
		var code string
		switch {
		case errors.Is(err, client.ErrUndefinedOperation):
			code = contract.CodeUndefinedOperation
		case errors.Is(err, client.ErrUndeclaredChild):
			code = contract.CodeUndeclaredChild
		case errors.Is(err, client.ErrSchemaViolation):
			code = contract.CodeSchemaViolation
		default:
			_ = req.Error(contract.CodeInternal, fmt.Sprintf("pre-flight: %v", err), nil)
			return
		}
		_ = req.Error(code, err.Error(), nil)
		return
	}
	if err := op.Attest(n.signer); err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return
	}

	// The record goes out as the caller built it — headers, guard, rollup
	// scope — with the author stamped and the attestation added.
	msg := nats.NewMsg(subject)
	msg.Header = op.Header()
	if guard := headers.Get(contract.HdrExpectedLastSubjSeq); guard != "" {
		msg.Header.Set(contract.HdrExpectedLastSubjSeq, guard)
	}
	if rollup := headers.Get(contract.HdrRollup); rollup != "" {
		msg.Header.Set(contract.HdrRollup, rollup)
	}
	msg.Data = op.Payload

	ack, err := n.js.PublishMsg(ctx, msg)
	if err == nil {
		n.reply(req, client.ApplyResponse{Seq: ack.Sequence})
		return
	}
	if client.GuardRefused(err) {
		// The guard fires before dedup: the caller's retry surfaces here
		// too, and the same judgement tells it from a conflict.
		landed, ok, lerr := n.preflight.OpLanded(ctx, store, subject, op.ID)
		if lerr != nil {
			_ = req.Error(contract.CodeInternal, fmt.Sprintf("write refused and %v", errors.Join(lerr, err)), nil)
			return
		}
		if ok {
			n.reply(req, client.ApplyResponse{Seq: landed.Seq})
			return
		}
		if headers.Get(contract.HdrExpectedLastSubjSeq) == "0" {
			_ = req.Error(contract.CodeInstanceExists, fmt.Sprintf("instance %q already exists in store %s", contract.TailPath(tail), store), nil)
			return
		}
		_ = req.Error(contract.CodeInstanceMoved, fmt.Sprintf("instance %q moved past the expected sequence", contract.TailPath(tail)), nil)
		return
	}
	_ = req.Error(contract.CodeInternal, fmt.Sprintf("publish: %v", err), nil)
}

// reply encodes one answer.
func (n *node) reply(req micro.Request, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return
	}
	_ = req.Respond(data)
}
