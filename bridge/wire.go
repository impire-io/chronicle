package bridge

import (
	"fmt"
	"regexp"
	"strings"
)

// SelectorIdentity is the selector of the connect token —
// `<selector>:<github-token>` — that asks for the identity plane outright,
// where a login reads the identity's accounts and creates its first. An
// account name as the selector lands in that account; empty leaves the
// placement to the callout — the one account the identity is a member of,
// else the identity plane.
const SelectorIdentity = "+"

// ConnectToken composes the connect token from a selector and a GitHub
// token.
func ConnectToken(selector, githubToken string) string {
	return selector + ":" + githubToken
}

// PlaneAccount is the NATS account an identity-plane placement lands in.
const PlaneAccount = "CONTROL"

// The identity plane's two request subjects (decision 0035): which
// accounts the identity is a member of, and account create. The permission
// is the identity: the numeric GitHub id and the login travel in the
// subject the placed user may publish to, so neither trusts a payload for
// who is asking.
const (
	identityRoot = "CHRON.CTRL.IDENTITY."
	// MembershipsVerb answers the identity's memberships; the subject is
	// <verb>.<github-id>.<login>.
	MembershipsVerb = identityRoot + "MEMBERSHIPS"
	// AccountCreateVerb creates an account for the identity, the identity
	// its admin; the subject is <verb>.<github-id>.<login>.
	AccountCreateVerb = identityRoot + "ACCOUNT.CREATE"
)

// MembershipsSubject is the memberships request for one identity.
func MembershipsSubject(githubID int64, login string) string {
	return fmt.Sprintf("%s.%d.%s", MembershipsVerb, githubID, login)
}

// AccountCreateSubject is the account-create request for one identity.
func AccountCreateSubject(githubID int64, login string) string {
	return fmt.Sprintf("%s.%d.%s", AccountCreateVerb, githubID, login)
}

// The service-account verbs (decision 0052): an admin of an account asks
// the install to issue, rotate or revoke a machine's credential there. The
// identity still rides the subject; the account and the service account's
// name ride the payload, and the install admits the call only when the
// identity is an admin of that account.
const (
	// ServiceAccountIssueVerb mints a service account's credential and
	// records it; the subject is <verb>.<github-id>.<login>.
	ServiceAccountIssueVerb = identityRoot + "SERVICE_ACCOUNT.ISSUE"
	// ServiceAccountRotateVerb revokes the current credential and issues a
	// fresh one under the same principal and role.
	ServiceAccountRotateVerb = identityRoot + "SERVICE_ACCOUNT.ROTATE"
	// ServiceAccountRevokeVerb kills the credential on the wire and retires
	// the record.
	ServiceAccountRevokeVerb = identityRoot + "SERVICE_ACCOUNT.REVOKE"
)

// ServiceAccountIssueSubject is the issue request for one identity.
func ServiceAccountIssueSubject(githubID int64, login string) string {
	return fmt.Sprintf("%s.%d.%s", ServiceAccountIssueVerb, githubID, login)
}

// ServiceAccountRotateSubject is the rotate request for one identity.
func ServiceAccountRotateSubject(githubID int64, login string) string {
	return fmt.Sprintf("%s.%d.%s", ServiceAccountRotateVerb, githubID, login)
}

// ServiceAccountRevokeSubject is the revoke request for one identity.
func ServiceAccountRevokeSubject(githubID int64, login string) string {
	return fmt.Sprintf("%s.%d.%s", ServiceAccountRevokeVerb, githubID, login)
}

// loginRe bounds a login to one subject token.
var loginRe = regexp.MustCompile(`^[a-z0-9-]+$`)

// IdentityFromSubject recovers the identity an identity-plane subject
// carries — its last two tokens; ok is false when the subject is not one.
func IdentityFromSubject(subject string) (githubID int64, login string, ok bool) {
	if !strings.HasPrefix(subject, identityRoot) {
		return 0, "", false
	}
	tokens := strings.Split(subject, ".")
	if len(tokens) < 2 {
		return 0, "", false
	}
	login = tokens[len(tokens)-1]
	if _, err := fmt.Sscanf(tokens[len(tokens)-2], "%d", &githubID); err != nil || githubID <= 0 || !loginRe.MatchString(login) {
		return 0, "", false
	}
	return githubID, login, true
}

// Membership is one account an identity is a member of.
type Membership struct {
	Account   string `json:"account"`
	Principal string `json:"principal"`
	Role      string `json:"role"`
	// Created says the identity created this account itself — what the
	// plan's cap counts.
	Created bool `json:"created,omitempty"`
}

// MembershipsResponse answers which accounts an identity may sign into,
// and where it stands against its plan's cap on created accounts.
type MembershipsResponse struct {
	Memberships     []Membership `json:"memberships"`
	AccountsCreated int          `json:"accounts_created"`
	// AccountsPerIdentity is the plan's cap; zero is unlimited.
	AccountsPerIdentity int    `json:"accounts_per_identity"`
	Plan                string `json:"plan"`
}

// AccountCreateRequest asks the identity plane for an account for the
// identity speaking — the identity its admin, bound by its numeric id. An
// empty name is the login.
type AccountCreateRequest struct {
	Name string `json:"name,omitempty"`
}

// AccountCreateResponse names what was created; there are no credentials
// to hand back, because the admin signs in.
type AccountCreateResponse struct {
	Name    string `json:"name"`
	Account string `json:"account"`
	Admin   string `json:"admin"`
	Plan    string `json:"plan"`
}

// ServiceAccountIssueRequest asks for a service account's credential in
// an account the identity administers: a new name, or one registered
// without a credential.
type ServiceAccountIssueRequest struct {
	Account string `json:"account"`
	Name    string `json:"name"`
	Role    string `json:"role"`
}

// ServiceAccountRequest names a service account for rotate and revoke.
type ServiceAccountRequest struct {
	Account string `json:"account"`
	Name    string `json:"name"`
}

// ServiceAccountCredential is what issue and rotate hand back — once.
// Creds is the whole credential file.
type ServiceAccountCredential struct {
	Account   string `json:"account"`
	Name      string `json:"name"`
	Role      string `json:"role"`
	PublicKey string `json:"public_key"`
	Creds     string `json:"creds"`
}

// ServiceAccountRevokeResponse names what revoke killed.
type ServiceAccountRevokeResponse struct {
	Account   string `json:"account"`
	Name      string `json:"name"`
	PublicKey string `json:"public_key"`
}

// The codes the install answers the service-account verbs with, beside
// the identity plane's own.
const (
	// CodeForbidden: the identity is not an admin of the named account —
	// or there is no such account; the install says no more than that.
	CodeForbidden = "forbidden"
	// CodeNotAServiceAccount: the name is a person's membership.
	CodeNotAServiceAccount = "not-a-service-account"
	// CodeCredentialExists: issue named a service account that already
	// holds a credential; rotate is the sentence.
	CodeCredentialExists = "credential-exists"
	// CodeNotAMember: rotate or revoke named nothing in the registry.
	CodeNotAMember = "not-a-member"
)
