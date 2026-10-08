package contract

// The error catalog: every code a control verb or an index query may
// return in the micro error headers, with its meaning (design 12 § the
// artifact — 0005's machine-legible errors, made a list). The services
// speak these constants; an SDK generates its error enum from the
// artifact's copy, and the artifact test proves the two equal.
const (
	CodeInternal            = "500"
	CodeBadRequest          = "bad-request"
	CodeForbidden           = "forbidden"
	CodeNotAMember          = "not-a-member"
	CodeBadStoreName        = "bad-store-name"
	CodeBadIndexName        = "bad-index-name"
	CodeBadTypeName         = "bad-type-name"
	CodeBadPrincipalName    = "bad-principal-name"
	CodeBadInstance         = "bad-instance"
	CodeBadRole             = "bad-role"
	CodeBadHistory          = "bad-history"
	CodeBadKind             = "bad-kind"
	CodeBadConfig           = "bad-config"
	CodeBadSchema           = "bad-schema"
	CodeBadEffect           = "bad-effect"
	CodeBadOpType           = "bad-op-type"
	CodeBadChildName        = "bad-child-name"
	CodeBadChildType        = "bad-child-type"
	CodeBadOp               = "bad-op"
	CodeBadDirection        = "bad-direction"
	CodeStoreExists         = "store-exists"
	CodeIndexExists         = "index-exists"
	CodeMemberExists        = "member-exists"
	CodeNoSuchStore         = "no-such-store"
	CodeNoSuchIndex         = "no-such-index"
	CodeNoSuchInstance      = "no-such-instance"
	CodeReservedStateIndex  = "reserved-state-index"
	CodeProviderUnavailable = "provider-unavailable"
)

// ErrorCode is one catalogued code and what it means.
type ErrorCode struct {
	Code    string `json:"code"`
	Meaning string `json:"meaning"`
}

// ErrorCatalog is every code, in the artifact's order.
var ErrorCatalog = []ErrorCode{
	{CodeInternal, "the service failed; the description says how"},
	{CodeBadRequest, "the request body does not decode, or a required field is missing"},
	{CodeForbidden, "the principal's role does not admit the verb"},
	{CodeNotAMember, "the principal is not a member of the account"},
	{CodeBadStoreName, "the store name is outside [a-z0-9-]+ or reserved"},
	{CodeBadIndexName, "the index name is outside [a-z0-9-]+"},
	{CodeBadTypeName, "the type name is outside [a-z0-9-]+"},
	{CodeBadPrincipalName, "the principal name is outside [a-z0-9-]+ or reserved"},
	{CodeBadInstance, "the instance path has a segment outside [a-zA-Z0-9_-]+"},
	{CodeBadRole, "the role is outside the vocabulary (admin, writer, reader)"},
	{CodeBadHistory, "the history policy is outside the vocabulary (compactable, full)"},
	{CodeBadKind, "the index kind is outside the vocabulary (search, graph, semantic)"},
	{CodeBadConfig, "the index config does not parse under its kind's contract"},
	{CodeBadSchema, "a JSON Schema in the type record does not compile"},
	{CodeBadEffect, "an operation's effect is outside the vocabulary (merge, none)"},
	{CodeBadOpType, "an operation's name is not a valid op type"},
	{CodeBadChildName, "a child name is outside [a-z0-9-]+"},
	{CodeBadChildType, "a child names a type outside [a-z0-9-]+"},
	{CodeBadOp, "the query's op is outside the index kind's vocabulary"},
	{CodeBadDirection, "the graph direction is outside the vocabulary (out, in, both)"},
	{CodeStoreExists, "the store already exists"},
	{CodeIndexExists, "the index is already declared"},
	{CodeMemberExists, "the member is already registered"},
	{CodeNoSuchStore, "the store does not exist"},
	{CodeNoSuchIndex, "the index is not declared"},
	{CodeNoSuchInstance, "the instance has no history in the store"},
	{CodeReservedStateIndex, "state is not an index a caller declares or deletes; it is read as the instance's state"},
	{CodeProviderUnavailable, "the semantic kind's embedding provider did not answer"},
}

// KnownErrorCode says whether a code is catalogued.
func KnownErrorCode(code string) bool {
	for _, c := range ErrorCatalog {
		if c.Code == code {
			return true
		}
	}
	return false
}
