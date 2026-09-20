package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/impire-io/chronicle/internal/identity/github"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/registry"
)

// The browser identity bridge (02-DESIGN/08-browser-identity.md, decision
// 0026): the callout responder, served from control because the tenant
// signing keys already live here — no new custody location. It can only
// place, never provision: unknown tenants and unbound identities are
// refusals, and no registry write ever happens on this path.

// authCalloutSubject is where the server asks (ADR-26); the bridge answers
// on a connection inside CONTROL, the auth account (0030 point 6).
const authCalloutSubject = "$SYS.REQ.USER.AUTH"

// serverXKeyHeader carries the server's public curve key on encrypted
// requests.
const serverXKeyHeader = "Nats-Server-Xkey"

// BridgeConfig wires the responder.
type BridgeConfig struct {
	// Conn is a connection authenticated as a CONTROL user listed in
	// auth_users — the control instance's own; every user chronicle
	// issues is listed, and only the sentinel is not.
	Conn *nats.Conn
	// ResponseSignerSeed is CONTROL's account seed: responses must be
	// signed by the account the callout runs in.
	ResponseSignerSeed []byte
	// XKeySeed is the callout's curve seed (custody's `auth` entry);
	// requests arrive sealed to its public half and responses go back
	// sealed to the server.
	XKeySeed []byte
	// Validator answers who holds a presented token.
	Validator github.TokenValidator
	// TTL bounds every placement — the revocation bound (0026). The
	// effective expiry is min(TTL, the token's own remaining life).
	// Zero means one hour.
	TTL time.Duration
	// Logger; nil means slog.Default. Refusal detail is logged here and
	// never sent to the wire.
	Logger *slog.Logger
}

const defaultBridgeTTL = time.Hour

// bridge holds the running responder's material.
type bridge struct {
	c        *control
	cfg      BridgeConfig
	signer   nkeys.KeyPair
	xkp      nkeys.KeyPair
	signPub  string
	validate github.TokenValidator
	logger   *slog.Logger
}

// startBridge subscribes the responder. Stopping rides the connection's
// close — the composition root owns the conn.
func (c *control) startBridge(cfg BridgeConfig) error {
	if cfg.Conn == nil || cfg.Validator == nil {
		return errors.New("bridge: connection and validator are required")
	}
	signer, err := nkeys.FromSeed(cfg.ResponseSignerSeed)
	if err != nil {
		return fmt.Errorf("bridge: response signer seed: %w", err)
	}
	signPub, err := signer.PublicKey()
	if err != nil {
		return fmt.Errorf("bridge: response signer public: %w", err)
	}
	xkp, err := nkeys.FromCurveSeed(cfg.XKeySeed)
	if err != nil {
		return fmt.Errorf("bridge: xkey seed: %w", err)
	}
	if cfg.TTL == 0 {
		cfg.TTL = defaultBridgeTTL
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	b := &bridge{c: c, cfg: cfg, signer: signer, xkp: xkp, signPub: signPub, validate: cfg.Validator, logger: logger}
	// A queue subscription: n control instances answer as one bridge.
	if _, err := cfg.Conn.QueueSubscribe(authCalloutSubject, "chronicle-bridge", b.handle); err != nil {
		return fmt.Errorf("bridge: subscribe: %w", err)
	}
	if err := cfg.Conn.Flush(); err != nil {
		return fmt.Errorf("bridge: flush subscription: %w", err)
	}
	return nil
}

// handle answers one authorization request. Every refusal is the same
// uniform error on the wire; the reason lands in the log only.
func (b *bridge) handle(msg *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 1900*time.Millisecond)
	defer cancel()

	payload := msg.Data
	serverXPub := msg.Header.Get(serverXKeyHeader)
	if serverXPub != "" {
		opened, err := b.xkp.Open(payload, serverXPub)
		if err != nil {
			b.logger.Warn("bridge: request decrypt failed", "err", err)
			return
		}
		payload = opened
	}
	req, err := jwt.DecodeAuthorizationRequestClaims(string(payload))
	if err != nil {
		b.logger.Warn("bridge: bad authorization request", "err", err)
		return
	}

	userJWT, reason := b.place(ctx, req)
	resp := jwt.NewAuthorizationResponseClaims(req.UserNkey)
	resp.Audience = req.Server.ID
	if reason != nil {
		b.logger.Warn("bridge: refused", "reason", reason, "client", req.ClientInformation.Host)
		resp.Error = "authentication failed"
	} else {
		resp.Jwt = userJWT
	}
	token, err := resp.Encode(b.signer)
	if err != nil {
		b.logger.Error("bridge: encode response", "err", err)
		return
	}
	out := []byte(token)
	if serverXPub != "" {
		sealed, err := b.xkp.Seal(out, serverXPub)
		if err != nil {
			b.logger.Error("bridge: response encrypt", "err", err)
			return
		}
		out = sealed
	}
	if err := msg.Respond(out); err != nil {
		b.logger.Error("bridge: respond", "err", err)
	}
}

// place resolves one request to a signed placement, or the reason it must
// not happen.
func (b *bridge) place(ctx context.Context, req *jwt.AuthorizationRequestClaims) (string, error) {
	tenant, token, ok := strings.Cut(req.ConnectOptions.Token, ":")
	if !ok || tenant == "" || token == "" {
		return "", errors.New("token is not <tenant>:<github-token>")
	}
	ident, err := b.validate.ValidateToken(ctx, token)
	if err != nil {
		return "", fmt.Errorf("validate token: %w", err)
	}
	tm, err := b.c.loadTenant(ctx, tenant)
	if err != nil {
		return "", fmt.Errorf("tenant %q: %w", tenant, err)
	}
	meta, done, err := b.c.meta(ctx, tm)
	if err != nil {
		return "", fmt.Errorf("open registry: %w", err)
	}
	defer done()
	principal, _, err := registry.LookupByGithubID(ctx, meta, ident.ID)
	if err != nil {
		return "", fmt.Errorf("github id %d in %q: %w", ident.ID, tenant, err)
	}

	ttl := b.cfg.TTL
	if !ident.TokenExpiry.IsZero() {
		if remaining := time.Until(ident.TokenExpiry); remaining < ttl {
			ttl = remaining
		}
	}
	if ttl <= 0 {
		return "", errors.New("github token already expired")
	}
	return mint.IssueBridgeUser(tm.accountPub, tm.scopedSeed, principal, req.UserNkey, ttl)
}
