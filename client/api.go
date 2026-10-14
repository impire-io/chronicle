package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
)

// The API verbs. Every verb is spoken on CHRON.API.<principal>.<VERB…>
// (decision 0057): the caller's name after API, which the member baseline
// lets that member alone publish on, so the responder reads the caller
// from the subject and no request carries a principal in its payload.
// The concrete verbs are protocol tokens, uppercase per the case rule,
// served in-account by chronicle-node and the indexers with registry role
// checks on that caller. The managed service's CHRON.CTRL.> surface is not
// tenant wire contract and lives with the service (chronicle-service).
const (
	verbStoreCreate      = "LOG.CREATE"
	verbTypeDefine       = "TYPE.DEFINE"
	verbInstanceSnapshot = "THING.ROLLUP"
	verbIndexDeclare     = "INDEX.DECLARE"
	verbIndexDelete      = "INDEX.DELETE"
	verbMemberAdd        = "MEMBER.ADD"
	verbMemberRevoke     = "MEMBER.REVOKE"
	verbPing             = "PING"
	verbIndexQuery       = "INDEX.QUERY"
	verbApply            = "APPLY"
)

// StoreCreateSubject is the subject a caller creates a store on.
func StoreCreateSubject(principal string) string {
	return contract.APISubject(principal, verbStoreCreate)
}

// TypeDefineSubject is the subject a caller defines a type on.
func TypeDefineSubject(principal string) string {
	return contract.APISubject(principal, verbTypeDefine)
}

// InstanceSnapshotSubject is the subject a caller asks for a snapshot on.
func InstanceSnapshotSubject(principal string) string {
	return contract.APISubject(principal, verbInstanceSnapshot)
}

// IndexDeclareSubject is the subject a caller declares an index on.
func IndexDeclareSubject(principal string) string {
	return contract.APISubject(principal, verbIndexDeclare)
}

// IndexDeleteSubject is the subject a caller deletes an index on.
func IndexDeleteSubject(principal string) string {
	return contract.APISubject(principal, verbIndexDelete)
}

// MemberAddSubject is the subject a caller adds a member on.
func MemberAddSubject(principal string) string {
	return contract.APISubject(principal, verbMemberAdd)
}

// MemberRevokeSubject is the subject a caller revokes a member on.
func MemberRevokeSubject(principal string) string {
	return contract.APISubject(principal, verbMemberRevoke)
}

// PingSubject is the subject a caller pings the node on.
func PingSubject(principal string) string { return contract.APISubject(principal, verbPing) }

// The endpoints a responder serves each verb on, whoever calls.
var (
	StoreCreateEndpoint      = contract.APIEndpoint(verbStoreCreate)
	TypeDefineEndpoint       = contract.APIEndpoint(verbTypeDefine)
	InstanceSnapshotEndpoint = contract.APIEndpoint(verbInstanceSnapshot)
	IndexDeclareEndpoint     = contract.APIEndpoint(verbIndexDeclare)
	IndexDeleteEndpoint      = contract.APIEndpoint(verbIndexDelete)
	MemberAddEndpoint        = contract.APIEndpoint(verbMemberAdd)
	MemberRevokeEndpoint     = contract.APIEndpoint(verbMemberRevoke)
	PingEndpoint             = contract.APIEndpoint(verbPing)
	// ApplyEndpoint is the verified write's: CHRON.API.*.APPLY.>, the
	// store and the instance's tail after the verb.
	ApplyEndpoint = contract.APIEndpoint(verbApply + ".>")
)

// IndexQuerySubject is the endpoint one index serves, in-account, as a
// caller speaks it — CHRON.API.<principal>.INDEX.QUERY.<store>.<index>
// (05-indexes.md § the query surface). It is answered by that index's own
// chronicle-index-* service, not the node, and only once the index has
// caught up with the log: no responder means the indexer is not running
// or still replaying.
func IndexQuerySubject(principal, store, index string) string {
	return contract.APISubject(principal, verbIndexQuery+"."+store+"."+index)
}

// IndexQueryEndpoint is the subject one index's service serves, whoever
// calls: CHRON.API.*.INDEX.QUERY.<store>.<index>.
func IndexQueryEndpoint(store, index string) string {
	return contract.APIEndpoint(verbIndexQuery + "." + store + "." + index)
}

// ApplySubject is the verified write (decision 0057): the caller asks the
// node to write one operation on CHRON.API.<principal>.APPLY.<store>.<tail>,
// carrying the record exactly as a direct publish would — the headers and
// the payload — and the node, reading the caller from the subject, stamps
// and attests the author, publishes on the instance's exact subject with
// the caller's id and guard, and answers with the sequence.
func ApplySubject(principal, store, tail string) string {
	return contract.APISubject(principal, verbApply+"."+store+"."+tail)
}

// ParseApplySubject reads the caller, the store and the instance's tail
// out of a verified write's subject.
func ParseApplySubject(subject string) (principal, store, tail string, err error) {
	principal, err = contract.CallerFromSubject(subject)
	if err != nil {
		return "", "", "", err
	}
	rest := strings.TrimPrefix(subject, contract.APISubject(principal, verbApply)+".")
	if rest == subject {
		return "", "", "", fmt.Errorf("subject %q is not a verified write", subject)
	}
	store, tail, ok := strings.Cut(rest, ".")
	if !ok || store == "" || tail == "" {
		return "", "", "", fmt.Errorf("subject %q names no store and instance", subject)
	}
	return principal, store, tail, nil
}

// ApplyResponse is the verified write's answer: where the operation landed.
type ApplyResponse struct {
	Seq uint64 `json:"seq"`
}

// StoreCreateRequest creates a log: a stream and META entries — no key
// operations, no JWT pushes. The caller is the subject's; the request
// names none.
type StoreCreateRequest struct {
	Store       string `json:"store"`
	Description string `json:"description,omitempty"`
	// MaxBytes overrides the stream's default byte budget; zero means the
	// decided default (1 GiB).
	MaxBytes int64 `json:"max_bytes,omitempty"`
	// History is the log's 0019 declaration: "compactable" (the default
	// when unset) or "full". Set at creation, immutable for now.
	History string `json:"history,omitempty"`
	// Schedules asks for a store whose stream allows NATS message
	// schedules and per-message TTLs (decision 0050), so a schedule
	// registered in it has its ticks written by the server.
	Schedules bool `json:"schedules,omitempty"`
	// VerifiedAuthors asks for a store that requires verified authors
	// (decision 0057): the fold marks every operation the node did not
	// attest, and the SDKs write to it through the node.
	VerifiedAuthors bool `json:"verified_authors,omitempty"`
}

// StoreCreateResponse names the created stream.
type StoreCreateResponse struct {
	Stream string `json:"stream"`
}

// TypeDefineRequest defines a type — the unit of definition (decision
// 0021): one act sets all facets, and re-defining bumps the revision.
// Evolution is additive: revisions are recorded, never rewritten in
// place. Operations carry each op's payload schema and its effect on
// state (decision 0011, unchanged in substance).
type TypeDefineRequest struct {
	Store      string                    `json:"store"`
	Type       string                    `json:"type"`
	Schema     json.RawMessage           `json:"schema"`
	History    string                    `json:"history,omitempty"`
	Children   map[string]string         `json:"children,omitempty"`
	Operations map[string]contract.OpDef `json:"operations,omitempty"`
}

// TypeDefineResponse carries the recorded revision.
type TypeDefineResponse struct {
	Revision uint64 `json:"revision"`
}

// InstanceSnapshotRequest asks the node to compact one thing's history into a
// fresh snapshot — the on-demand rollup trigger (04-fleet.md § the node's
// duties). The node applies the effect gate (decision 0011): history its
// fold has not fully captured into state is refused with the reason, and
// compaction stays the application's call (SaveSnapshot).
type InstanceSnapshotRequest struct {
	Store    string `json:"store"`
	Instance string `json:"instance"`
}

// InstanceSnapshotResponse says what happened: Taken with the new snapshot's
// stream seq, or the reason the node declined — a gate veto, nothing to
// compact, or a lost race. Declining is an answer, not an error.
type InstanceSnapshotResponse struct {
	Taken  bool   `json:"taken"`
	Seq    uint64 `json:"seq,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// IndexDeclareRequest declares an index on a log: one META key, realized
// by the fleet placing a chronicle-index-* workload. The kind is checked
// against the node's vocabulary — write-side strict, like effects — and
// config belongs to the kind (0015): graph requires edge rules, search
// refuses config.
type IndexDeclareRequest struct {
	Store  string          `json:"store"`
	Index  string          `json:"index"`
	Kind   string          `json:"kind"`
	Config json.RawMessage `json:"config,omitempty"`
}

// IndexDeclareResponse names the query subject the index will serve once
// caught up, as the caller speaks it.
type IndexDeclareResponse struct {
	Query string `json:"query"`
}

// IndexDeleteRequest retires an index: the META key goes and the
// supervisor stops the workload. The index was derived — nothing of
// record is lost. Changing a declaration is delete + declare.
type IndexDeleteRequest struct {
	Store string `json:"store"`
	Index string `json:"index"`
}

// IndexDeleteResponse acknowledges the retirement.
type IndexDeleteResponse struct {
	Deleted bool `json:"deleted"`
}

// IndexQueryRequest is one search: a match over every string field of
// thing state (empty query matches everything). Any registry role may
// query. The reply is a streamed reply (design 12): Limit caps the hits
// the caller wants — zero or absent streams every match — and there is
// no offset: a lost stream is re-requested.
type IndexQueryRequest struct {
	Query string `json:"query"`
	Limit int    `json:"limit,omitempty"`
}

// IndexHit names a thing and its relevance. The index is never authority:
// the thing's state is the state bucket's, its history the log's.
type IndexHit struct {
	Instance string  `json:"instance"`
	Score    float64 `json:"score"`
}

// QueryTrailer closes a search's streamed reply: the total match count,
// whatever the cap let through.
type QueryTrailer struct {
	Total uint64 `json:"total"`
}

// MemberAddRequest registers a principal as a member of the tenant with
// a role — the registry write of 11-the-two-forms.md § membership,
// without custody: one place writes the registry in both forms. The
// caller (role admin) is the subject's; Member is who joins. The
// credential is not this verb's: in the open form it is the operator's
// NATS's business, and the managed service issues one and records its
// public key here.
type MemberAddRequest struct {
	Member string `json:"member"`
	// Role is one of admin, writer, reader; "writer" when empty.
	Role string `json:"role,omitempty"`
	// Kind is what the principal is to the user: "member" (a person, the
	// default) or "service" (a service account).
	Kind string `json:"kind,omitempty"`
	// PublicKey is the member's NATS user public key where one is known.
	PublicKey string `json:"public_key,omitempty"`
	// GithubID binds the membership to a GitHub identity for the managed
	// service's browser bridge (decision 0026); zero means unbound.
	GithubID int64 `json:"github_id,omitempty"`
}

// MemberAddResponse echoes the membership recorded.
type MemberAddResponse struct {
	Member string `json:"member"`
	Role   string `json:"role"`
}

// MemberRevokeRequest retires a membership: the record leaves the
// registry and the principal can act no more. The principal record stays
// — it is the durable identity past records attribute to, and a re-added
// member is the same principal. Killing the credential on the wire is the
// issuer's job, after this.
type MemberRevokeRequest struct {
	Member string `json:"member"`
}

// MemberRevokeResponse names the retired membership and the public key it
// held, for an issuer that revokes it on the wire.
type MemberRevokeResponse struct {
	Member    string `json:"member"`
	PublicKey string `json:"public_key,omitempty"`
}

// About answers the ping verb.
type About struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ServiceError is a micro endpoint's refusal, code and description intact.
type ServiceError struct {
	Code string
	Desc string
}

func (e *ServiceError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Desc) }

// Request round-trips one micro request on the product surface and
// decodes the reply or the service's error headers — the one building
// block every verb here is, exported so a build that adds verbs (the
// managed service's) speaks the same way.
func Request[Req, Resp any](ctx context.Context, nc *nats.Conn, subject string, req Req) (Resp, error) {
	var zero Resp
	data, err := json.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("marshal request: %w", err)
	}
	msg, err := nc.RequestWithContext(ctx, subject, data)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return zero, fmt.Errorf("no responder for %s: is chronicle running for this account?", subject)
		}
		return zero, fmt.Errorf("%s: %w", subject, err)
	}
	if code := msg.Header.Get(micro.ErrorCodeHeader); code != "" {
		return zero, &ServiceError{Code: code, Desc: msg.Header.Get(micro.ErrorHeader)}
	}
	var resp Resp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return zero, fmt.Errorf("%s: decode response: %w", subject, err)
	}
	return resp, nil
}

// StoreOpt adjusts one log creation.
type StoreOpt func(*StoreCreateRequest)

// WithHistory declares the log's history posture (0019): "preserved"
// makes the trail the product — the node never compacts the log, and its
// stream refuses rollup writes outright. Unset means "compactable",
// today's behavior. Declared at creation, immutable for now.
func WithHistory(history string) StoreOpt {
	return func(r *StoreCreateRequest) { r.History = history }
}

// WithSchedules asks for a store that allows schedules: its stream takes
// NATS message schedules and per-message TTLs, so a schedule registered
// in it has the server write its ticks into the store (decision 0050).
func WithSchedules() StoreOpt {
	return func(r *StoreCreateRequest) { r.Schedules = true }
}

// WithVerifiedAuthors asks for a store that requires verified authors
// (decision 0057): every write to it goes through the node, which vouches
// for the author, and an operation written any other way is kept in the
// history and marked out of the state.
func WithVerifiedAuthors() StoreOpt {
	return func(r *StoreCreateRequest) { r.VerifiedAuthors = true }
}

// CreateStore creates a log through the node's control verb.
func (c *Client) CreateStore(ctx context.Context, log, description string, opts ...StoreOpt) (StoreCreateResponse, error) {
	r := StoreCreateRequest{
		Store:       log,
		Description: description,
	}
	for _, apply := range opts {
		apply(&r)
	}
	return Request[StoreCreateRequest, StoreCreateResponse](ctx, c.nc, StoreCreateSubject(c.author), r)
}

// TypeDefinition is the caller's side of a type record: every facet but
// the revision, which the node computes.
type TypeDefinition struct {
	// Schema is the thing's shape — required: type and schema are born
	// together (0021).
	Schema json.RawMessage
	// History is the type's compaction declaration: "compactable" (the
	// default when unset) or "preserved" — the soft tier (0022 § 4).
	History string
	// Children maps segment names to the types valid under a thing of
	// this type. Targets may be defined later — latest declaration wins.
	Children map[string]string
	// Operations is the op vocabulary: payload schema + effect per op.
	Operations map[string]contract.OpDef
}

// DefineType records a type definition — one act, all facets; a repeat
// bumps the revision (0021).
func (c *Client) DefineType(ctx context.Context, log, name string, def TypeDefinition) (TypeDefineResponse, error) {
	return Request[TypeDefineRequest, TypeDefineResponse](ctx, c.nc, TypeDefineSubject(c.author), TypeDefineRequest{
		Store:      log,
		Type:       name,
		Schema:     def.Schema,
		History:    def.History,
		Children:   def.Children,
		Operations: def.Operations,
	})
}

// DeclareIndex declares an index on a log and returns the query subject
// it will serve once caught up. Config belongs to the kind: nil for
// search, edge rules for graph.
func (c *Client) DeclareIndex(ctx context.Context, log, index, kind string, config json.RawMessage) (IndexDeclareResponse, error) {
	return Request[IndexDeclareRequest, IndexDeclareResponse](ctx, c.nc, IndexDeclareSubject(c.author), IndexDeclareRequest{
		Store:  log,
		Index:  index,
		Kind:   kind,
		Config: config,
	})
}

// DeleteIndex retires an index; its workload stops and its derived index
// is discarded.
func (c *Client) DeleteIndex(ctx context.Context, log, index string) (IndexDeleteResponse, error) {
	return Request[IndexDeleteRequest, IndexDeleteResponse](ctx, c.nc, IndexDeleteSubject(c.author), IndexDeleteRequest{
		Store: log,
		Index: index,
	})
}

// QueryIndex searches one index. No responder means the indexer is not
// running or still replaying — the honest signal of an index that is not
// current yet.
func (c *Client) QueryIndex(ctx context.Context, log, index, query string, limit int) *Streamed[IndexHit, QueryTrailer] {
	return RequestStream[IndexQueryRequest, IndexHit, QueryTrailer](ctx, c.nc, IndexQuerySubject(c.author, log, index), IndexQueryRequest{
		Query: query,
		Limit: limit,
	})
}

// Snapshot asks the node to compact one thing's history now. A
// response with Taken false is the node declining — the effect gate, an
// empty tail, or a lost race — with the reason; only transport and
// refusal failures are errors.
func (c *Client) snapshotTail(ctx context.Context, log, thing string) (InstanceSnapshotResponse, error) {
	return Request[InstanceSnapshotRequest, InstanceSnapshotResponse](ctx, c.nc, InstanceSnapshotSubject(c.author), InstanceSnapshotRequest{
		Store:    log,
		Instance: thing,
	})
}

// MemberOpt adjusts one member add.
type MemberOpt func(*MemberAddRequest)

// WithPublicKey records the member's NATS user public key.
func WithPublicKey(key string) MemberOpt {
	return func(r *MemberAddRequest) { r.PublicKey = key }
}

// WithGithubID binds the membership to a GitHub identity.
func WithGithubID(id int64) MemberOpt {
	return func(r *MemberAddRequest) { r.GithubID = id }
}

// AsServiceAccount records the principal as a service account — a
// machine with a credential — rather than a member.
func AsServiceAccount() MemberOpt {
	return func(r *MemberAddRequest) { r.Kind = contract.PrincipalKindService }
}

// AddMember registers a principal as a member with a role (admin only).
// Role "" means writer.
func (c *Client) AddMember(ctx context.Context, member, role string, opts ...MemberOpt) (MemberAddResponse, error) {
	r := MemberAddRequest{Member: member, Role: role}
	for _, o := range opts {
		o(&r)
	}
	return Request[MemberAddRequest, MemberAddResponse](ctx, c.nc, MemberAddSubject(c.author), r)
}

// RevokeMember retires a membership (admin only).
func (c *Client) RevokeMember(ctx context.Context, member string) (MemberRevokeResponse, error) {
	return Request[MemberRevokeRequest, MemberRevokeResponse](ctx, c.nc, MemberRevokeSubject(c.author), MemberRevokeRequest{
		Member: member,
	})
}

// GraphQueryRequest is the graph kind's payload on the standard query
// subject (05-indexes.md § the graph kind): Op names the verb.
type GraphQueryRequest struct {
	Op       string `json:"op"`
	Instance string `json:"instance"`
	// Direction: out (default), in, or both. In-edges cover this log's
	// things pointing at the target.
	Direction string `json:"direction,omitempty"`
	// Label filters neighbors to one edge type.
	Label string `json:"label,omitempty"`
	// Labels filters a walk's traversable edge types.
	Labels []string `json:"labels,omitempty"`
	// Depth bounds a walk; default 1, capped (the trailer says when).
	Depth int `json:"depth,omitempty"`
	// Limit caps the items the caller wants; zero streams them all.
	Limit int `json:"limit,omitempty"`
}

// GraphTrailer closes a graph query's streamed reply. Total counts what
// matched, whatever the cap let through; on a walk, DepthCapped says the
// requested depth exceeded the cap and Truncated says the limit bit
// before the frontier emptied.
type GraphTrailer struct {
	Total       uint64 `json:"total"`
	DepthCapped bool   `json:"depth_capped,omitempty"`
	Truncated   bool   `json:"truncated,omitempty"`
}

// GraphNeighbors streams the edges at a thing in one graph index, in
// stable order. A target may be dangling — data, not corruption;
// resolution is the caller's state read.
func (c *Client) GraphNeighbors(ctx context.Context, log, index string, q GraphQueryRequest) *Streamed[contract.GraphEdge, GraphTrailer] {
	q.Op = contract.GraphOpNeighbors
	return RequestStream[GraphQueryRequest, contract.GraphEdge, GraphTrailer](ctx, c.nc, IndexQuerySubject(c.author, log, index), q)
}

// GraphWalk streams one graph index's things breadth-first from a thing,
// each with the depth and label it was first reached through.
func (c *Client) GraphWalk(ctx context.Context, log, index string, q GraphQueryRequest) *Streamed[contract.GraphVisit, GraphTrailer] {
	q.Op = contract.GraphOpWalk
	return RequestStream[GraphQueryRequest, contract.GraphVisit, GraphTrailer](ctx, c.nc, IndexQuerySubject(c.author, log, index), q)
}

// SemanticQueryRequest is the semantic kind's payload on the standard
// query subject (05-indexes.md § the semantic kind).
type SemanticQueryRequest struct {
	Text string `json:"text"`
	// Limit caps the hits the caller wants; zero streams them all.
	Limit int `json:"limit,omitempty"`
}

// SemanticHit names a thing, its best-chunk score, and the field the
// meaning matched in. The index is never authority.
type SemanticHit struct {
	Instance string  `json:"instance"`
	Score    float64 `json:"score"`
	Field    string  `json:"field,omitempty"`
}

// SemanticTrailer closes a semantic query's streamed reply: the total
// match count and the honest degradation signal — how many things are
// folded but not yet embedded.
type SemanticTrailer struct {
	Total      uint64 `json:"total"`
	Unembedded int    `json:"unembedded"`
}

// QuerySemantic streams one semantic index's hits by meaning, best first.
func (c *Client) QuerySemantic(ctx context.Context, log, index, text string, limit int) *Streamed[SemanticHit, SemanticTrailer] {
	return RequestStream[SemanticQueryRequest, SemanticHit, SemanticTrailer](ctx, c.nc, IndexQuerySubject(c.author, log, index), SemanticQueryRequest{
		Text:  text,
		Limit: limit,
	})
}
