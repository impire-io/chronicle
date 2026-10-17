package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// membershipElsewhere is the refusal every member verb answers under any
// resolver but the registry (decision 0061 point 4): the roles come from
// the environment, and the registry has nothing to record.
const membershipElsewhere = "membership is managed where the roles live: this node asks the environment's role responder, and the registry has nothing to record"

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
	if !n.membershipHere {
		_ = req.Error(contract.CodeMembershipElsewhere, membershipElsewhere, nil)
		return
	}
	if err := n.requireCaller(ctx, req, contract.RoleAdmin); err != nil {
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
	kind := contract.NormalizePrincipalKind(r.Kind)
	if !contract.KnownPrincipalKind(kind) {
		_ = req.Error(contract.CodeBadRequest, fmt.Sprintf("kind %q: %q (a person) or %q (a service account)", r.Kind, contract.PrincipalKindMember, contract.PrincipalKindService), nil)
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
	membership, err := json.Marshal(contract.Membership{PublicKey: r.PublicKey, Role: role, Kind: kind, GithubID: r.GithubID})
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
	if !n.membershipHere {
		_ = req.Error(contract.CodeMembershipElsewhere, membershipElsewhere, nil)
		return
	}
	if err := n.requireCaller(ctx, req, contract.RoleAdmin); err != nil {
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
	// The sole admin keeps the role (decision 0062): an account with no
	// admin can neither be administered nor repaired by its members.
	if m.Role == contract.RoleAdmin {
		sole, err := n.soleAdmin(ctx, r.Member)
		if err != nil {
			_ = req.Error(contract.CodeInternal, fmt.Sprintf("count admins: %v", err), nil)
			return
		}
		if sole {
			_ = req.Error(contract.CodeSoleAdmin, fmt.Sprintf("%q is the account's only admin and cannot be removed: make another member admin first", r.Member), nil)
			return
		}
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

// handleMemberSetRole changes a membership's role in place (decision
// 0062): one KV update under the entry's revision, nothing else touched —
// the credential keeps connecting and the next request the principal
// makes is judged by the new role, because roles are read at request time
// and placed nowhere (0007 point 3). A member and a service account are
// changed the same way; the role already held is answered, not refused;
// the account's only admin cannot be demoted.
func (n *node) handleMemberSetRole(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var r client.MemberSetRoleRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error(contract.CodeBadRequest, err.Error(), nil)
		return
	}
	if !n.membershipHere {
		_ = req.Error(contract.CodeMembershipElsewhere, membershipElsewhere, nil)
		return
	}
	if err := n.requireCaller(ctx, req, contract.RoleAdmin); err != nil {
		return
	}
	if err := contract.ValidatePrincipalName(r.Member); err != nil {
		_ = req.Error(contract.CodeBadPrincipalName, err.Error(), nil)
		return
	}
	if !contract.KnownRole(r.Role) {
		_ = req.Error(contract.CodeBadRole, fmt.Sprintf("role %q: one of %v", r.Role, contract.Roles), nil)
		return
	}
	for {
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
		if m.Role == r.Role {
			break
		}
		if m.Role == contract.RoleAdmin {
			sole, err := n.soleAdmin(ctx, r.Member)
			if err != nil {
				_ = req.Error(contract.CodeInternal, fmt.Sprintf("count admins: %v", err), nil)
				return
			}
			if sole {
				_ = req.Error(contract.CodeSoleAdmin, fmt.Sprintf("%q is the account's only admin and keeps the role: make another member admin first", r.Member), nil)
				return
			}
		}
		m.Role = r.Role
		value, err := json.Marshal(m)
		if err != nil {
			_ = req.Error(contract.CodeInternal, err.Error(), nil)
			return
		}
		_, err = n.meta.Update(ctx, contract.MetaMember(r.Member), value, entry.Revision())
		if err == nil {
			break
		}
		if !errors.Is(err, jetstream.ErrKeyExists) {
			_ = req.Error(contract.CodeInternal, fmt.Sprintf("record role: %v", err), nil)
			return
		}
		// Lost the revision race to another write of the same record:
		// read again and judge again.
	}
	reply, err := json.Marshal(client.MemberSetRoleResponse(r))
	if err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

// soleAdmin reports whether the principal is the only admin the registry
// holds — the one membership a demotion or a revoke must not reach.
func (n *node) soleAdmin(ctx context.Context, principal string) (bool, error) {
	keys, err := n.meta.Keys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return false, fmt.Errorf("list META keys: %w", err)
	}
	prefix := contract.MetaMember("")
	for _, k := range keys {
		name, ok := strings.CutPrefix(k, prefix)
		if !ok || name == "" || name == principal {
			continue
		}
		entry, err := n.meta.Get(ctx, k)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("read membership %s: %w", name, err)
		}
		var m contract.Membership
		if err := json.Unmarshal(entry.Value(), &m); err != nil {
			return false, fmt.Errorf("decode membership %s: %w", name, err)
		}
		if m.Role == contract.RoleAdmin {
			return false, nil
		}
	}
	return true, nil
}
