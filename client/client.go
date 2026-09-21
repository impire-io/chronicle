// Package client is the Go client of the tenant plane: the wire contract
// of decision 0008, spoken directly. Appends are direct JetStream
// publishes with the correctness machinery the server's; the API verbs
// are NATS micro requests on CHRON.API.>. The package speaks only the
// contract package — never a service's internals — and it works against
// any NATS in any auth mode: a member is whoever the credential says, or
// whoever the caller states when the credential carries no name.
package client

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// Client is one member's (or service user's) handle on a tenant account.
type Client struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	author string

	mu       sync.Mutex
	inFlight map[string]*sync.Mutex // one in-flight publish per subject
	types    *typeCache
}

// Connect dials with decorated .creds content — the managed form's
// credential, and any operator-mode NATS's. The principal ID is read from
// the user JWT's name: the SDK stamps Op-Author from the credentials it
// runs with, an identity it was actually issued. Further options — a
// TLSServerName, say — ride after the credential's own.
func Connect(url string, creds []byte, opts ...nats.Option) (*Client, error) {
	nc, author, err := dialCreds(url, creds, "chronicle-client", opts...)
	if err != nil {
		return nil, err
	}
	c, err := wrap(nc, author)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return c, nil
}

func dialCreds(url string, creds []byte, name string, opts ...nats.Option) (*nats.Conn, string, error) {
	token, err := jwt.ParseDecoratedJWT(creds)
	if err != nil {
		return nil, "", fmt.Errorf("parse creds jwt: %w", err)
	}
	claims, err := jwt.DecodeUserClaims(token)
	if err != nil {
		return nil, "", fmt.Errorf("decode user claims: %w", err)
	}
	kp, err := jwt.ParseDecoratedUserNKey(creds)
	if err != nil {
		return nil, "", fmt.Errorf("parse creds nkey: %w", err)
	}
	nc, err := nats.Connect(url, append([]nats.Option{
		nats.Name(name),
		nats.UserJWT(
			func() (string, error) { return token, nil },
			func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) },
		),
		nats.Timeout(5 * time.Second),
	}, opts...)...)
	if err != nil {
		return nil, "", fmt.Errorf("connect: %w", err)
	}
	return nc, claims.Name, nil
}

// ConnectFile dials with a .creds file path.
func ConnectFile(url, credsPath string, opts ...nats.Option) (*Client, error) {
	creds, err := os.ReadFile(credsPath)
	if err != nil {
		return nil, fmt.Errorf("read creds: %w", err)
	}
	return Connect(url, creds, opts...)
}

// ConnectWith dials however the operator's NATS takes a user — an nkey,
// a user and password, a token — with the principal stated by the caller,
// because a credential that carries no name cannot say who its holder is
// (11-the-two-forms.md § bring your own NATS). The trust tier is the
// registry's: the principal is the caller's assertion, checked against
// membership at the node.
func ConnectWith(url, author string, opts ...nats.Option) (*Client, error) {
	if author == "" {
		return nil, fmt.Errorf("connect: the principal must be stated when the credential carries no name")
	}
	nc, err := nats.Connect(url, append([]nats.Option{nats.Name("chronicle-client"), nats.Timeout(5 * time.Second)}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	c, err := wrap(nc, author)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return c, nil
}

// ConnectNkeyFile dials as an nkey user — a seed file, the way a plain
// server's config or the quick start names a user — stating the
// principal.
func ConnectNkeyFile(url, seedPath, author string, opts ...nats.Option) (*Client, error) {
	opt, err := nats.NkeyOptionFromSeed(seedPath)
	if err != nil {
		return nil, fmt.Errorf("read nkey seed: %w", err)
	}
	return ConnectWith(url, author, append([]nats.Option{opt}, opts...)...)
}

// NkeyFromSeed parses a seed file's key pair — for callers that hold the
// seed bytes rather than a path.
func NkeyFromSeed(seed []byte) (nkeys.KeyPair, error) {
	kp, err := nkeys.FromSeed(seed)
	if err != nil {
		return nil, fmt.Errorf("parse nkey seed: %w", err)
	}
	return kp, nil
}

// Wrap adopts an existing connection — the in-process path the quick
// start and the tests use. The author is stamped explicitly.
func Wrap(nc *nats.Conn, author string) (*Client, error) {
	return wrap(nc, author)
}

func wrap(nc *nats.Conn, author string) (*Client, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	return &Client{
		nc:       nc,
		js:       js,
		author:   author,
		inFlight: map[string]*sync.Mutex{},
		types:    newTypeCache(js),
	}, nil
}

// Author is the principal ID this client stamps as Op-Author.
func (c *Client) Author() string { return c.author }

// Conn exposes the underlying connection — the wire is the product
// surface, and nothing in this package hides it.
func (c *Client) Conn() *nats.Conn { return c.nc }

// Close closes the underlying connection.
func (c *Client) Close() { c.nc.Close() }

// subjectLock serialises this writer's own publishes per subject: one
// in-flight publish per subject when order between your own ops matters
// (pattern § 4.4).
func (c *Client) subjectLock(subject string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.inFlight[subject]
	if !ok {
		l = &sync.Mutex{}
		c.inFlight[subject] = l
	}
	return l
}
