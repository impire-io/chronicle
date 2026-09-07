// Package client is the Go client of the walking skeleton: the wire
// contract of decision 0008, spoken directly. Appends are direct JetStream
// publishes with the correctness machinery the server's; the control verbs
// are NATS micro requests on CHRON.API.>. The package speaks only the
// contract package — never a service's internals.
package client

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Client is one member's (or service user's) handle on a tenant account.
type Client struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	author string

	mu       sync.Mutex
	inFlight map[string]*sync.Mutex // one in-flight publish per subject
	schemas  *schemaCache
}

// Connect dials with decorated .creds content. The principal ID is read
// from the user JWT's name — the SDK stamps Op-Author from the credentials
// it runs with, an identity it was actually issued.
func Connect(url string, creds []byte) (*Client, error) {
	token, err := jwt.ParseDecoratedJWT(creds)
	if err != nil {
		return nil, fmt.Errorf("parse creds jwt: %w", err)
	}
	claims, err := jwt.DecodeUserClaims(token)
	if err != nil {
		return nil, fmt.Errorf("decode user claims: %w", err)
	}
	kp, err := jwt.ParseDecoratedUserNKey(creds)
	if err != nil {
		return nil, fmt.Errorf("parse creds nkey: %w", err)
	}
	nc, err := nats.Connect(url,
		nats.Name("chronicle-client"),
		nats.UserJWT(
			func() (string, error) { return token, nil },
			func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) },
		),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	c, err := wrap(nc, claims.Name)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return c, nil
}

// ConnectFile dials with a .creds file path.
func ConnectFile(url, credsPath string) (*Client, error) {
	creds, err := os.ReadFile(credsPath)
	if err != nil {
		return nil, fmt.Errorf("read creds: %w", err)
	}
	return Connect(url, creds)
}

// Wrap adopts an existing connection — the in-process path `chronicle up`
// and the tests use. The author is stamped explicitly.
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
		schemas:  newSchemaCache(js),
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
