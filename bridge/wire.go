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
