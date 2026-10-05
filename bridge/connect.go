package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/client"
)

// Connect dials through the bridge into an account (decision 0026): the
// sentinel triggers the install's callout, the GitHub token rides the
// connect, and the server places the connection in the account named. The
// principal comes back from the server's own answer to who-am-I — the
// placed user names it, so Op-Author stamps from an identity the bridge
// actually resolved.
func Connect(url string, sentinel []byte, account, githubToken string) (*client.Client, error) {
	if account == "" || githubToken == "" {
		return nil, fmt.Errorf("bridge connect: account and token are required")
	}
	if account == SelectorIdentity {
		return nil, fmt.Errorf("bridge connect: %q is the identity plane's selector, not an account (ConnectIdentity)", account)
	}
	nc, err := dial(url, sentinel, account, githubToken)
	if err != nil {
		return nil, err
	}
	who, err := whoami(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	if who.account != account {
		nc.Close()
		return nil, fmt.Errorf("whoami: placed in account %q, expected %q", who.account, account)
	}
	c, err := client.Wrap(nc, who.user)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return c, nil
}

// Identity is a connection placed in the identity plane: a GitHub identity
// before it has chosen — or has — an account.
type Identity struct {
	// GithubID and Login are what the callout placed, read back from the
	// placement's own permissions.
	GithubID int64
	Login    string
	nc       *nats.Conn
}

// Close closes the placement.
func (i *Identity) Close() { i.nc.Close() }

// Memberships asks which accounts the identity may sign into.
func (i *Identity) Memberships(ctx context.Context) (MembershipsResponse, error) {
	return client.Request[struct{}, MembershipsResponse](ctx, i.nc, MembershipsSubject(i.GithubID, i.Login), struct{}{})
}

// CreateAccount asks for an account of the identity's own — the
// self-service mint (decision 0035): the identity its admin, the plan's
// limits in the account, the plan's cap enforced. The install answers once
// the account serves, so the context bounds a placement, not a round trip.
func (i *Identity) CreateAccount(ctx context.Context, name string) (AccountCreateResponse, error) {
	return client.Request[AccountCreateRequest, AccountCreateResponse](ctx, i.nc, AccountCreateSubject(i.GithubID, i.Login), AccountCreateRequest{Name: name})
}

// ConnectIdentity dials through the bridge into the identity plane
// (decision 0035): where a login learns which accounts it holds and
// creates its first. The identity's id and login come back from the
// placement's own permissions — the only subjects it may speak on name it.
func ConnectIdentity(url string, sentinel []byte, githubToken string) (*Identity, error) {
	if githubToken == "" {
		return nil, fmt.Errorf("bridge connect: token is required")
	}
	nc, err := dial(url, sentinel, SelectorIdentity, githubToken)
	if err != nil {
		return nil, err
	}
	who, err := whoami(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	if who.account != PlaneAccount {
		nc.Close()
		return nil, fmt.Errorf("whoami: placed in account %q, expected the identity plane", who.account)
	}
	var id int64
	var login string
	for _, subject := range who.publish {
		if gid, l, ok := IdentityFromSubject(subject); ok {
			id, login = gid, l
			break
		}
	}
	if id == 0 {
		nc.Close()
		return nil, fmt.Errorf("whoami: the placement names no identity (permissions %v)", who.publish)
	}
	return &Identity{GithubID: id, Login: login, nc: nc}, nil
}

func dial(url string, sentinel []byte, selector, githubToken string) (*nats.Conn, error) {
	token, err := jwt.ParseDecoratedJWT(sentinel)
	if err != nil {
		return nil, fmt.Errorf("parse sentinel jwt: %w", err)
	}
	kp, err := jwt.ParseDecoratedUserNKey(sentinel)
	if err != nil {
		return nil, fmt.Errorf("parse sentinel nkey: %w", err)
	}
	nc, err := nats.Connect(url,
		nats.Name("chronicle-bridge-client"),
		nats.UserJWT(
			func() (string, error) { return token, nil },
			func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) },
		),
		nats.Token(ConnectToken(selector, githubToken)),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("bridge connect: %w", err)
	}
	return nc, nil
}

// placed is what the server says about a callout-placed connection.
type placed struct {
	user    string
	account string
	publish []string
}

// whoami asks the server for the placed identity. For a callout-placed
// client the server reports the placed principal in the info's `user`
// field — `user_name` keeps the sentinel's tag from the original connect —
// the account name, and the permissions the placement carries.
func whoami(nc *nats.Conn) (placed, error) {
	msg, err := nc.Request("$SYS.REQ.USER.INFO", nil, 5*time.Second)
	if err != nil {
		return placed{}, fmt.Errorf("whoami: %w", err)
	}
	var resp struct {
		Data struct {
			User        string `json:"user"`
			AccountName string `json:"account_name"`
			Permissions *struct {
				Publish *struct {
					Allow []string `json:"allow"`
				} `json:"publish"`
			} `json:"permissions"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return placed{}, fmt.Errorf("whoami: decode: %w", err)
	}
	if resp.Data.User == "" || looksLikeNkey(resp.Data.User) {
		return placed{}, fmt.Errorf("whoami: server reported no principal name (got %q) — server behavior changed?", resp.Data.User)
	}
	p := placed{user: resp.Data.User, account: resp.Data.AccountName}
	if resp.Data.Permissions != nil && resp.Data.Permissions.Publish != nil {
		p.publish = resp.Data.Permissions.Publish.Allow
	}
	return p, nil
}

// looksLikeNkey spots a raw user public key where a principal name should
// be — 56 base32 chars starting with U.
func looksLikeNkey(s string) bool {
	if len(s) != 56 || s[0] != 'U' {
		return false
	}
	for _, r := range s {
		if (r < 'A' || r > 'Z') && (r < '2' || r > '7') {
			return false
		}
	}
	return true
}
