// Package registry answers the role question every API-serving component
// asks before it serves a verb or a query (04-fleet.md, 05-indexes.md): a
// resolver takes a principal and returns its role in the account, or
// that it is not a member. Two resolvers exist (decision 0061). Meta
// reads the membership registry of decision 0007 — the identity slice of
// META — and is the default the managed form and the quick start run.
// Callout asks the environment's own responder on CHRON.ROLE.<principal>
// and takes its answer, for an install inside a platform that already
// keeps its members and their roles. The package also seeds the registry
// where no minter does, and finds a membership by its GitHub binding for
// the bridge. Every API-serving component runs the same check through
// Require; a component carrying its own copy is a drift hazard.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
)

// Resolver answers one question: the principal's role in the account.
type Resolver interface {
	// Role returns the role the principal holds, or ErrNotAMember. Any
	// other error means the question could not be answered, and the
	// caller refuses the request rather than guessing.
	Role(ctx context.Context, principal string) (string, error)
}

// ErrNotAMember is a resolver's answer for a principal the account does
// not hold.
var ErrNotAMember = errors.New("not a member")

// ErrRoleSourceUnavailable wraps a callout that got no usable answer: no
// responder, no reply within the bound, a reply that does not decode, or
// a role outside the vocabulary. The check fails closed — never an
// answer from META in its place (decision 0061 point 3).
var ErrRoleSourceUnavailable = errors.New("the role responder did not answer")

// Meta is the default resolver: the membership registry in META, read as
// decision 0007 rules.
func Meta(meta jetstream.KeyValue) Resolver { return metaResolver{meta: meta} }

type metaResolver struct{ meta jetstream.KeyValue }

func (r metaResolver) Role(ctx context.Context, principal string) (string, error) {
	entry, err := r.meta.Get(ctx, contract.MetaMember(principal))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return "", ErrNotAMember
	}
	if err != nil {
		return "", fmt.Errorf("read membership: %w", err)
	}
	var m contract.Membership
	if err := json.Unmarshal(entry.Value(), &m); err != nil {
		return "", fmt.Errorf("decode membership: %w", err)
	}
	return m.Role, nil
}

// DefaultCalloutTimeout bounds a role callout. A responder answers from
// what it already holds; one that takes longer than this is not there.
const DefaultCalloutTimeout = 2 * time.Second

// Callout is the resolver for an environment that keeps the roles itself
// (decision 0061): each question is one request on CHRON.ROLE.<principal>
// with an empty payload, answered by the environment's responder with a
// contract.RoleReply. A reply without a role is a non-member. A role
// outside the vocabulary, a reply that does not decode, no responder, and
// no reply within the timeout (zero means DefaultCalloutTimeout) are each
// ErrRoleSourceUnavailable.
func Callout(nc *nats.Conn, timeout time.Duration) Resolver {
	if timeout <= 0 {
		timeout = DefaultCalloutTimeout
	}
	return calloutResolver{nc: nc, timeout: timeout}
}

type calloutResolver struct {
	nc      *nats.Conn
	timeout time.Duration
}

func (r calloutResolver) Role(ctx context.Context, principal string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	subject := contract.RoleSubject(principal)
	msg, err := r.nc.RequestWithContext(ctx, subject, nil)
	if err != nil {
		return "", fmt.Errorf("%w: %s: %v", ErrRoleSourceUnavailable, subject, err)
	}
	var reply contract.RoleReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return "", fmt.Errorf("%w: the reply on %s does not decode: %v", ErrRoleSourceUnavailable, subject, err)
	}
	if reply.Role == "" {
		return "", ErrNotAMember
	}
	if !contract.KnownRole(reply.Role) {
		return "", fmt.Errorf("%w: the reply on %s names role %q, outside %v", ErrRoleSourceUnavailable, subject, reply.Role, contract.Roles)
	}
	return reply.Role, nil
}

// The two sources a process is started with (--roles).
const (
	SourceMeta    = "meta"
	SourceCallout = "callout"
)

// ForSource turns a flag's value into a resolver: nil for meta — the
// component builds the default over the META handle it opens itself —
// and the callout for callout. Anything else is refused with both names.
func ForSource(source string, nc *nats.Conn) (Resolver, error) {
	switch source {
	case "", SourceMeta:
		return nil, nil //nolint:nilnil // nil is the default the component composes itself
	case SourceCallout:
		return Callout(nc, 0), nil
	}
	return nil, fmt.Errorf("roles %q: %s (the account's own registry) or %s (the environment's responder)", source, SourceMeta, SourceCallout)
}

// Require checks the resolver's answer against the roles a verb accepts.
// The principal is the caller's, read from the subject (0057). The
// tenant's own service is the operator: it holds every role and no
// registry entry (contract.ServicePrincipal), whichever resolver answers,
// and is asked of nobody.
func Require(ctx context.Context, r Resolver, principal string, roles ...string) error {
	if principal == "" {
		return errors.New("principal: must not be empty")
	}
	if principal == contract.ServicePrincipal {
		return nil
	}
	role, err := r.Role(ctx, principal)
	if errors.Is(err, ErrNotAMember) {
		return fmt.Errorf("principal %q is %w", principal, ErrNotAMember)
	}
	if err != nil {
		return err
	}
	if !slices.Contains(roles, role) {
		return fmt.Errorf("principal %q holds role %q; one of %v required", principal, role, roles)
	}
	return nil
}

// Code is the wire code a refusal from Require carries: the source's
// silence is its own code, everything else is forbidden.
func Code(err error) string {
	if errors.Is(err, ErrRoleSourceUnavailable) {
		return contract.CodeRoleSourceUnavailable
	}
	return contract.CodeForbidden
}

// SeedOpt adjusts the admin membership Seed records.
type SeedOpt func(*contract.Membership)

// WithGithubID binds the seeded admin to a GitHub identity — the numeric
// user id — for the browser bridge: a self-service account's admin logs
// in through the bridge and may hold no issued credential at all
// (decision 0035). Zero binds nothing.
func WithGithubID(id int64) SeedOpt {
	return func(m *contract.Membership) { m.GithubID = id }
}

// Seed births an account's registry where no service minted it — the
// open form (11-the-two-forms.md § membership, without custody): META
// create-if-absent, and the one admin principal and membership, each
// create-if-absent, so a second run against the same account changes
// nothing. The public key is the admin's NATS user where a minter issued
// one; in the open form it is empty — the identity is the operator's NATS
// user, and the principal is asserted. The managed service seeds the same
// way at every mint, binding the admin to its GitHub identity where the
// account is the identity's own.
func Seed(ctx context.Context, js jetstream.JetStream, admin, publicKey string, opts ...SeedOpt) (jetstream.KeyValue, error) {
	if err := contract.ValidatePrincipalName(admin); err != nil {
		return nil, fmt.Errorf("seed: %w", err)
	}
	meta, err := js.KeyValue(ctx, contract.MetaBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		meta, err = js.CreateKeyValue(ctx, contract.MetaBucketConfig())
	}
	if err != nil {
		return nil, fmt.Errorf("provision META: %w", err)
	}
	principal, err := json.Marshal(contract.Principal{ID: admin, Name: admin})
	if err != nil {
		return nil, err
	}
	if _, err := meta.Create(ctx, contract.MetaPrincipal(admin), principal); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return nil, fmt.Errorf("seed principal %s: %w", admin, err)
	}
	m := contract.Membership{PublicKey: publicKey, Role: contract.RoleAdmin}
	for _, opt := range opts {
		opt(&m)
	}
	membership, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if _, err := meta.Create(ctx, contract.MetaMember(admin), membership); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return nil, fmt.Errorf("seed membership %s: %w", admin, err)
	}
	return meta, nil
}

// LookupByGithubID finds the one membership bound to a GitHub identity —
// the browser bridge's question (decision 0026). The registry is small by
// construction (one entry per member), so a scan is the honest index.
func LookupByGithubID(ctx context.Context, meta jetstream.KeyValue, githubID int64) (string, contract.Membership, error) {
	if githubID == 0 {
		return "", contract.Membership{}, errors.New("github id: must not be zero")
	}
	lister, err := meta.ListKeys(ctx)
	if err != nil {
		return "", contract.Membership{}, fmt.Errorf("list registry keys: %w", err)
	}
	defer func() { _ = lister.Stop() }()
	const prefix = "identity.member."
	for key := range lister.Keys() {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		entry, err := meta.Get(ctx, key)
		if err != nil {
			return "", contract.Membership{}, fmt.Errorf("read %s: %w", key, err)
		}
		var m contract.Membership
		if err := json.Unmarshal(entry.Value(), &m); err != nil {
			return "", contract.Membership{}, fmt.Errorf("decode %s: %w", key, err)
		}
		if m.GithubID == githubID {
			return strings.TrimPrefix(key, prefix), m, nil
		}
	}
	return "", contract.Membership{}, fmt.Errorf("no membership bound to github id %d", githubID)
}
