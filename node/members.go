package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// The member verbs (11-the-two-forms.md § membership, without custody):
// the registry — who is a member, in which role — lives in the tenant's
// META bucket and the node enforces it, so the node is the one place that
// writes it in both forms. The credential is not the node's business: in
// the open form it is the operator's NATS's, and the managed service
// composes its issuance with these same verbs, acting as the tenant's
// service principal.

// handleMemberAdd records a membership: the principal record
// create-if-absent (the durable identity may survive from a since-revoked
// membership), the membership Create as the dedup gate — two adds of one
// principal cannot both land.
func (n *node) handleMemberAdd(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var r client.MemberAddRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error(contract.CodeBadRequest, err.Error(), nil)
		return
	}
	if err := n.requireRole(ctx, r.Principal, contract.RoleAdmin); err != nil {
		_ = req.Error(contract.CodeForbidden, err.Error(), nil)
		return
	}
	if err := contract.ValidatePrincipalName(r.Member); err != nil {
		_ = req.Error(contract.CodeBadPrincipalName, err.Error(), nil)
		return
	}
	role := r.Role
	if role == "" {
		role = contract.RoleWriter
	}
	if !contract.KnownRole(role) {
		_ = req.Error(contract.CodeBadRole, fmt.Sprintf("role %q: one of %v", role, contract.Roles), nil)
		return
	}

	principal, err := json.Marshal(contract.Principal{ID: r.Member})
	if err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return
	}
	if _, err := n.meta.Create(ctx, contract.MetaPrincipal(r.Member), principal); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		_ = req.Error(contract.CodeInternal, fmt.Sprintf("record principal: %v", err), nil)
		return
	}
	membership, err := json.Marshal(contract.Membership{PublicKey: r.PublicKey, Role: role, GithubID: r.GithubID})
	if err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return
	}
	if _, err := n.meta.Create(ctx, contract.MetaMember(r.Member), membership); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			_ = req.Error(contract.CodeMemberExists, fmt.Sprintf("principal %q is already a member", r.Member), nil)
			return
		}
		_ = req.Error(contract.CodeInternal, fmt.Sprintf("record membership: %v", err), nil)
		return
	}

	reply, err := json.Marshal(client.MemberAddResponse{Member: r.Member, Role: role})
	if err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

// handleMemberRevoke retires a membership: the record leaves the registry
// and the role check refuses the principal from the next request on. The
// principal record stays. The reply names the public key the membership
// held, for an issuer that kills it on the wire.
func (n *node) handleMemberRevoke(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var r client.MemberRevokeRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error(contract.CodeBadRequest, err.Error(), nil)
		return
	}
	if err := n.requireRole(ctx, r.Principal, contract.RoleAdmin); err != nil {
		_ = req.Error(contract.CodeForbidden, err.Error(), nil)
		return
	}
	if err := contract.ValidatePrincipalName(r.Member); err != nil {
		_ = req.Error(contract.CodeBadPrincipalName, err.Error(), nil)
		return
	}
	entry, err := n.meta.Get(ctx, contract.MetaMember(r.Member))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		_ = req.Error(contract.CodeNotAMember, fmt.Sprintf("principal %q is not a member", r.Member), nil)
		return
	}
	if err != nil {
		_ = req.Error(contract.CodeInternal, fmt.Sprintf("read membership: %v", err), nil)
		return
	}
	var m contract.Membership
	if err := json.Unmarshal(entry.Value(), &m); err != nil {
		_ = req.Error(contract.CodeInternal, fmt.Sprintf("decode membership: %v", err), nil)
		return
	}
	if err := n.meta.Delete(ctx, contract.MetaMember(r.Member)); err != nil {
		_ = req.Error(contract.CodeInternal, fmt.Sprintf("retire membership: %v", err), nil)
		return
	}

	reply, err := json.Marshal(client.MemberRevokeResponse{Member: r.Member, PublicKey: m.PublicKey})
	if err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}
