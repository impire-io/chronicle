package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
)

// The control-plane verb subjects. The designs fix only the CHRON.API.>
// root; the concrete verbs are protocol tokens, uppercase per the case
// rule. They are served in-account by chronicle-node with registry role
// checks. CHRON.CTRL.> is the cross-account control surface — not tenant
// wire contract — served by chronicle-control in its own account.
const (
	LogCreateSubject    = "CHRON.API.LOG.CREATE"
	TypeDefineSubject   = "CHRON.API.TYPE.DEFINE"
	ThingRollupSubject  = "CHRON.API.THING.ROLLUP"
	IndexDeclareSubject = "CHRON.API.INDEX.DECLARE"
	IndexDeleteSubject  = "CHRON.API.INDEX.DELETE"
	PingSubject         = "CHRON.API.PING"
	TenantMintSubject   = "CHRON.CTRL.TENANT.MINT"
	MemberAddSubject    = "CHRON.CTRL.MEMBER.ADD"
	MemberRevokeSubject = "CHRON.CTRL.MEMBER.REVOKE"
	MemberRekeySubject  = "CHRON.CTRL.MEMBER.REKEY"
)

// IndexQuerySubject is the endpoint one index serves, in-account —
// CHRON.API.INDEX.QUERY.<log>.<index> (05-indexes.md § the query surface).
// It is answered by that index's own chronicle-index-* service, not the
// node, and only once the index has caught up with the log: no responder
// means the indexer is not running or still replaying.
func IndexQuerySubject(log, index string) string {
	return "CHRON.API.INDEX.QUERY." + log + "." + index
}

// LogCreateRequest creates a log: a stream and META entries — no key
// operations, no JWT pushes. Principal is the caller's assertion, checked
// against the registry (role admin), the same trust tier as Op-Author.
type LogCreateRequest struct {
	Principal   string `json:"principal"`
	Log         string `json:"log"`
	Description string `json:"description,omitempty"`
	// MaxBytes overrides the stream's default byte budget; zero means the
	// decided default (1 GiB).
	MaxBytes int64 `json:"max_bytes,omitempty"`
	// History is the log's 0019 declaration: "compactable" (the default
	// when unset) or "preserved". Set at creation, immutable for now.
	History string `json:"history,omitempty"`
}

// LogCreateResponse names the created stream.
type LogCreateResponse struct {
	Stream string `json:"stream"`
}

// TypeDefineRequest defines a type — the unit of definition (decision
// 0021): one act sets all facets, and re-defining bumps the revision.
// Evolution is additive: revisions are recorded, never rewritten in
// place. Operations carry each op's payload schema and its effect on
// state (decision 0011, unchanged in substance).
type TypeDefineRequest struct {
	Principal  string                    `json:"principal"`
	Log        string                    `json:"log"`
	Type       string                    `json:"type"`
	Schema     json.RawMessage           `json:"schema"`
	History    string                    `json:"history,omitempty"`
	Aspects    map[string]string         `json:"aspects,omitempty"`
	Operations map[string]contract.OpDef `json:"operations,omitempty"`
}

// TypeDefineResponse carries the recorded revision.
type TypeDefineResponse struct {
	Revision uint64 `json:"revision"`
}

// ThingRollupRequest asks the node to compact one thing's history into a
// fresh snapshot — the on-demand rollup trigger (04-fleet.md § the node's
// duties). The node applies the effect gate (decision 0011): history its
// fold has not fully captured into state is refused with the reason, and
// compaction stays the application's call (SaveVersion).
type ThingRollupRequest struct {
	Principal string `json:"principal"`
	Log       string `json:"log"`
	Thing     string `json:"thing"`
}

// ThingRollupResponse says what happened: Rolled with the new snapshot's
// stream seq, or the reason the node declined — a gate veto, nothing to
// compact, or a lost race. Declining is an answer, not an error.
type ThingRollupResponse struct {
	Rolled bool   `json:"rolled"`
	Seq    uint64 `json:"seq,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// IndexDeclareRequest declares an index on a log: one META key, realized
// by the fleet placing a chronicle-index-* workload. The kind is checked
// against the node's vocabulary — write-side strict, like effects — and
// config belongs to the kind (0015): graph requires edge rules, search
// refuses config.
type IndexDeclareRequest struct {
	Principal string          `json:"principal"`
	Log       string          `json:"log"`
	Index     string          `json:"index"`
	Kind      string          `json:"kind"`
	Config    json.RawMessage `json:"config,omitempty"`
}

// IndexDeclareResponse names the query subject the index will serve once
// caught up.
type IndexDeclareResponse struct {
	Query string `json:"query"`
}

// IndexDeleteRequest retires an index: the META key goes and the
// supervisor stops the workload. The index was derived — nothing of
// record is lost. Changing a declaration is delete + declare.
type IndexDeleteRequest struct {
	Principal string `json:"principal"`
	Log       string `json:"log"`
	Index     string `json:"index"`
}

// IndexDeleteResponse acknowledges the retirement.
type IndexDeleteResponse struct {
	Deleted bool `json:"deleted"`
}

// IndexQueryRequest is one search: a match over every string field of
// thing state (empty query matches everything). Any registry role may
// query. Limit defaults to 10 and is capped at 100.
type IndexQueryRequest struct {
	Principal string `json:"principal"`
	Query     string `json:"query"`
	Limit     int    `json:"limit,omitempty"`
	Offset    int    `json:"offset,omitempty"`
}

// IndexHit names a thing and its relevance. The index is never authority:
// the thing's state is the state bucket's, its history the log's.
type IndexHit struct {
	Thing string  `json:"thing"`
	Score float64 `json:"score"`
}

// IndexQueryResponse carries the hits, best first, and the total match
// count.
type IndexQueryResponse struct {
	Hits  []IndexHit `json:"hits"`
	Total uint64     `json:"total"`
}

// About answers the ping verb.
type About struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// TenantMintRequest asks chronicle-control for a tenant: mint the account,
// provision META, seed the identity registry, mint the first admin
// principal.
type TenantMintRequest struct {
	Name string `json:"name"`
	// Admin is the first principal's ID; "admin" when empty.
	Admin string `json:"admin,omitempty"`
}

// TenantMintResponse hands back the account and the first admin's .creds —
// the only copy; chronicle keeps the registry, not the secret.
type TenantMintResponse struct {
	Account    string `json:"account"`
	Admin      string `json:"admin"`
	AdminCreds []byte `json:"admin_creds"`
}

// MemberAddRequest mints a principal into an existing tenant: a user under
// the tenant's scoped key, a registry entry with the given role. The verb
// chronicle-15 was missing — before it, replacing a burned admin meant
// destroying the install.
type MemberAddRequest struct {
	Tenant    string `json:"tenant"`
	Principal string `json:"principal"`
	// Role is one of admin, writer, reader; "writer" when empty.
	Role string `json:"role,omitempty"`
}

// MemberAddResponse hands back the new member's .creds — the only copy;
// chronicle keeps the registry, not the secret.
type MemberAddResponse struct {
	Principal string `json:"principal"`
	Role      string `json:"role"`
	Creds     []byte `json:"creds"`
}

// MemberRevokeRequest invalidates one principal's credential: the account
// JWT's revocation list kills the wire — new connections refused, live
// ones evicted — and the membership record leaves the registry. The
// principal record stays: it is the durable identity past records
// attribute to, and a re-added member is the same principal.
type MemberRevokeRequest struct {
	Tenant    string `json:"tenant"`
	Principal string `json:"principal"`
}

// MemberRevokeResponse names the revoked user key.
type MemberRevokeResponse struct {
	Principal string `json:"principal"`
	PublicKey string `json:"public_key"`
}

// MemberRekeyRequest is the coarse kill switch: assume every member
// credential is burned. The tenant's member-issuing scoped key is
// replaced — one push evicts every member at once, while the service user
// (issued under the plain signing key) rides through — and every member is
// re-issued under the new key.
type MemberRekeyRequest struct {
	Tenant string `json:"tenant"`
}

// MemberRekeyResponse carries every re-issued member credential — each the
// only copy. The response is one NATS message: with ~1 KiB per creds file
// and the default 1 MiB payload ceiling, a tenant beyond a few hundred
// members needs pagination this verb does not have yet.
type MemberRekeyResponse struct {
	Members []MemberAddResponse `json:"members"`
}

// ServiceError is a micro endpoint's refusal, code and description intact.
type ServiceError struct {
	Code string
	Desc string
}

func (e *ServiceError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Desc) }

// request round-trips one micro request and decodes the reply or the
// service's error headers.
func request[Req, Resp any](ctx context.Context, nc *nats.Conn, subject string, req Req) (Resp, error) {
	var zero Resp
	data, err := json.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("marshal request: %w", err)
	}
	msg, err := nc.RequestWithContext(ctx, subject, data)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return zero, fmt.Errorf("%s: no responder (is the node running for this account?)", subject)
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

// LogOpt adjusts one log creation.
type LogOpt func(*LogCreateRequest)

// WithHistory declares the log's history posture (0019): "preserved"
// makes the trail the product — the node never compacts the log, and its
// stream refuses rollup writes outright. Unset means "compactable",
// today's behavior. Declared at creation, immutable for now.
func WithHistory(history string) LogOpt {
	return func(r *LogCreateRequest) { r.History = history }
}

// CreateLog creates a log through the node's control verb.
func (c *Client) CreateLog(ctx context.Context, log, description string, opts ...LogOpt) (LogCreateResponse, error) {
	r := LogCreateRequest{
		Principal:   c.author,
		Log:         log,
		Description: description,
	}
	for _, apply := range opts {
		apply(&r)
	}
	return request[LogCreateRequest, LogCreateResponse](ctx, c.nc, LogCreateSubject, r)
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
	// Aspects maps segment names to the types valid under a thing of
	// this type. Targets may be defined later — latest declaration wins.
	Aspects map[string]string
	// Operations is the op vocabulary: payload schema + effect per op.
	Operations map[string]contract.OpDef
}

// DefineType records a type definition — one act, all facets; a repeat
// bumps the revision (0021).
func (c *Client) DefineType(ctx context.Context, log, name string, def TypeDefinition) (TypeDefineResponse, error) {
	return request[TypeDefineRequest, TypeDefineResponse](ctx, c.nc, TypeDefineSubject, TypeDefineRequest{
		Principal:  c.author,
		Log:        log,
		Type:       name,
		Schema:     def.Schema,
		History:    def.History,
		Aspects:    def.Aspects,
		Operations: def.Operations,
	})
}

// DeclareIndex declares an index on a log and returns the query subject
// it will serve once caught up. Config belongs to the kind: nil for
// search, edge rules for graph.
func (c *Client) DeclareIndex(ctx context.Context, log, index, kind string, config json.RawMessage) (IndexDeclareResponse, error) {
	return request[IndexDeclareRequest, IndexDeclareResponse](ctx, c.nc, IndexDeclareSubject, IndexDeclareRequest{
		Principal: c.author,
		Log:       log,
		Index:     index,
		Kind:      kind,
		Config:    config,
	})
}

// DeleteIndex retires an index; its workload stops and its derived index
// is discarded.
func (c *Client) DeleteIndex(ctx context.Context, log, index string) (IndexDeleteResponse, error) {
	return request[IndexDeleteRequest, IndexDeleteResponse](ctx, c.nc, IndexDeleteSubject, IndexDeleteRequest{
		Principal: c.author,
		Log:       log,
		Index:     index,
	})
}

// QueryIndex searches one index. No responder means the indexer is not
// running or still replaying — the honest signal of an index that is not
// current yet.
func (c *Client) QueryIndex(ctx context.Context, log, index, query string, limit, offset int) (IndexQueryResponse, error) {
	return request[IndexQueryRequest, IndexQueryResponse](ctx, c.nc, IndexQuerySubject(log, index), IndexQueryRequest{
		Principal: c.author,
		Query:     query,
		Limit:     limit,
		Offset:    offset,
	})
}

// RollupThing asks the node to compact one thing's history now. A
// response with Rolled false is the node declining — the effect gate, an
// empty tail, or a lost race — with the reason; only transport and
// refusal failures are errors.
func (c *Client) RollupThing(ctx context.Context, log, thing string) (ThingRollupResponse, error) {
	return request[ThingRollupRequest, ThingRollupResponse](ctx, c.nc, ThingRollupSubject, ThingRollupRequest{
		Principal: c.author,
		Log:       log,
		Thing:     thing,
	})
}

// Control is a handle on chronicle-control, dialed with control-plane
// credentials — a different account than any tenant.
type Control struct {
	nc *nats.Conn
}

// NewControl adopts a control-plane connection.
func NewControl(nc *nats.Conn) *Control { return &Control{nc: nc} }

// Close closes the underlying connection.
func (c *Control) Close() { c.nc.Close() }

// MintTenant creates a tenant end to end and returns the first admin's
// credentials.
func (c *Control) MintTenant(ctx context.Context, name, admin string) (TenantMintResponse, error) {
	return request[TenantMintRequest, TenantMintResponse](ctx, c.nc, TenantMintSubject, TenantMintRequest{
		Name:  name,
		Admin: admin,
	})
}

// AddMember mints a principal into an existing tenant and returns the only
// copy of their credentials.
func (c *Control) AddMember(ctx context.Context, tenant, principal, role string) (MemberAddResponse, error) {
	return request[MemberAddRequest, MemberAddResponse](ctx, c.nc, MemberAddSubject, MemberAddRequest{
		Tenant:    tenant,
		Principal: principal,
		Role:      role,
	})
}

// RevokeMember invalidates a principal's credential — evicted from the
// wire, retired from the registry.
func (c *Control) RevokeMember(ctx context.Context, tenant, principal string) (MemberRevokeResponse, error) {
	return request[MemberRevokeRequest, MemberRevokeResponse](ctx, c.nc, MemberRevokeSubject, MemberRevokeRequest{
		Tenant:    tenant,
		Principal: principal,
	})
}

// RekeyMembers replaces the tenant's member-issuing key, evicting every
// member credential at once, and returns the re-issued set.
func (c *Control) RekeyMembers(ctx context.Context, tenant string) (MemberRekeyResponse, error) {
	return request[MemberRekeyRequest, MemberRekeyResponse](ctx, c.nc, MemberRekeySubject, MemberRekeyRequest{
		Tenant: tenant,
	})
}

// GraphQueryRequest is the graph kind's payload on the standard query
// subject (05-indexes.md § the graph kind): Op names the verb.
type GraphQueryRequest struct {
	Principal string `json:"principal"`
	Op        string `json:"op"`
	Thing     string `json:"thing"`
	// Direction: out (default), in, or both. In-edges cover this log's
	// things pointing at the target.
	Direction string `json:"direction,omitempty"`
	// Label filters neighbors to one edge type.
	Label string `json:"label,omitempty"`
	// Labels filters a walk's traversable edge types.
	Labels []string `json:"labels,omitempty"`
	// Depth bounds a walk; default 1, capped (the reply says when).
	Depth  int `json:"depth,omitempty"`
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}

// GraphNeighborsResponse answers op neighbors: edges in stable order.
// A target may be dangling — data, not corruption; resolution is the
// caller's state read.
type GraphNeighborsResponse struct {
	Edges []contract.GraphEdge `json:"edges"`
	Total uint64               `json:"total"`
}

// GraphWalkResponse answers op walk: things first reached, breadth
// first, with the depth and label they arrived through. DepthCapped
// says the requested depth exceeded the cap; Truncated says the limit
// bit before the frontier emptied.
type GraphWalkResponse struct {
	Things      []contract.GraphVisit `json:"things"`
	Total       uint64                `json:"total"`
	DepthCapped bool                  `json:"depth_capped,omitempty"`
	Truncated   bool                  `json:"truncated,omitempty"`
}

// GraphNeighbors reads the edges at a thing in one graph index.
func (c *Client) GraphNeighbors(ctx context.Context, log, index string, q GraphQueryRequest) (GraphNeighborsResponse, error) {
	q.Principal = c.author
	q.Op = contract.GraphOpNeighbors
	return request[GraphQueryRequest, GraphNeighborsResponse](ctx, c.nc, IndexQuerySubject(log, index), q)
}

// GraphWalk traverses one graph index breadth-first from a thing.
func (c *Client) GraphWalk(ctx context.Context, log, index string, q GraphQueryRequest) (GraphWalkResponse, error) {
	q.Principal = c.author
	q.Op = contract.GraphOpWalk
	return request[GraphQueryRequest, GraphWalkResponse](ctx, c.nc, IndexQuerySubject(log, index), q)
}

// SemanticQueryRequest is the semantic kind's payload on the standard
// query subject (05-indexes.md § the semantic kind).
type SemanticQueryRequest struct {
	Principal string `json:"principal"`
	Text      string `json:"text"`
	Limit     int    `json:"limit,omitempty"`
	Offset    int    `json:"offset,omitempty"`
}

// SemanticHit names a thing, its best-chunk score, and the field the
// meaning matched in. The index is never authority.
type SemanticHit struct {
	Thing string  `json:"thing"`
	Score float64 `json:"score"`
	Field string  `json:"field,omitempty"`
}

// SemanticQueryResponse carries the hits, best first, and the honest
// degradation signal: how many things are folded but not yet embedded.
type SemanticQueryResponse struct {
	Hits       []SemanticHit `json:"hits"`
	Total      uint64        `json:"total"`
	Unembedded int           `json:"unembedded"`
}

// QuerySemantic searches one semantic index by meaning.
func (c *Client) QuerySemantic(ctx context.Context, log, index, text string, limit, offset int) (SemanticQueryResponse, error) {
	return request[SemanticQueryRequest, SemanticQueryResponse](ctx, c.nc, IndexQuerySubject(log, index), SemanticQueryRequest{
		Principal: c.author,
		Text:      text,
		Limit:     limit,
		Offset:    offset,
	})
}
