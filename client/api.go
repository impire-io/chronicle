package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// The control-plane verb subjects. The designs fix only the CHRON.API.>
// root; the concrete verbs are protocol tokens, uppercase per the case
// rule. They are served in-account by chronicle-node with registry role
// checks. CHRON.CTRL.> is the cross-account control surface — not tenant
// wire contract — served by chronicle-control in its own account.
const (
	LogCreateSubject  = "CHRON.API.LOG.CREATE"
	SchemaSetSubject  = "CHRON.API.SCHEMA.SET"
	PingSubject       = "CHRON.API.PING"
	TenantMintSubject = "CHRON.CTRL.TENANT.MINT"
)

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
}

// LogCreateResponse names the created stream.
type LogCreateResponse struct {
	Stream string `json:"stream"`
}

// SchemaSetRequest records an op-type schema revision. Evolution is
// additive: revisions are recorded, never rewritten in place.
type SchemaSetRequest struct {
	Principal string          `json:"principal"`
	Log       string          `json:"log"`
	OpType    string          `json:"op_type"`
	Schema    json.RawMessage `json:"schema"`
}

// SchemaSetResponse carries the recorded revision.
type SchemaSetResponse struct {
	Revision uint64 `json:"revision"`
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

// CreateLog creates a log through the node's control verb.
func (c *Client) CreateLog(ctx context.Context, log, description string) (LogCreateResponse, error) {
	return request[LogCreateRequest, LogCreateResponse](ctx, c.nc, LogCreateSubject, LogCreateRequest{
		Principal:   c.author,
		Log:         log,
		Description: description,
	})
}

// SetSchema records a new revision of an op-type's payload schema.
func (c *Client) SetSchema(ctx context.Context, log, opType string, schema json.RawMessage) (SchemaSetResponse, error) {
	return request[SchemaSetRequest, SchemaSetResponse](ctx, c.nc, SchemaSetSubject, SchemaSetRequest{
		Principal: c.author,
		Log:       log,
		OpType:    opType,
		Schema:    schema,
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
