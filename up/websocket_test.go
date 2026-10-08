package up_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/devdir"
	"github.com/impire-io/chronicle/up"
)

// withOrigin makes a websocket handshake carry the browser's Origin header.
func withOrigin(origin string) nats.Option {
	return nats.WebSocketConnectionHeaders(http.Header{"Origin": []string{origin}})
}

// TestUpWebsocketServesTheBrowser is spec 025's could-not-succeed-if-broken
// read: over the recorded websocket url, with the quick start's one nkey,
// a folded state comes back; a stranger and a foreign origin do not get in.
func TestUpWebsocketServesTheBrowser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dir := t.TempDir()
	const console = "http://localhost:3000"

	l, err := up.Up(ctx, up.Config{Dir: dir, Port: -1, WebsocketPort: -1, WebsocketOrigins: []string{console}})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	defer l.Stop()
	if !strings.HasPrefix(l.WebsocketURL, "ws://127.0.0.1:") {
		t.Fatalf("websocket url %q, want ws on loopback", l.WebsocketURL)
	}
	recorded, err := os.ReadFile(devdir.WebsocketURLPath(dir))
	if err != nil || string(recorded) != l.WebsocketURL {
		t.Fatalf("recorded websocket url %q (%v), want %q", recorded, err, l.WebsocketURL)
	}

	admin, err := client.ConnectNkeyFile(l.WebsocketURL, devdir.UserNkeyPath(dir), devdir.LocalPrincipal, withOrigin(console))
	if err != nil {
		t.Fatalf("connect as admin over websocket: %v", err)
	}
	defer admin.Close()
	if _, err := admin.CreateStore(ctx, "orders", "the orders"); err != nil {
		t.Fatalf("create log: %v", err)
	}
	if _, err := admin.CreateFromSnapshot(ctx, "orders", "note-1", json.RawMessage(`{"title":"over the socket"}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	waitState(ctx, t, admin, "orders", "note-1", "over the socket")

	// The one user is still the only one: no key, no connection.
	if _, err := client.ConnectWith(l.WebsocketURL, "stranger", withOrigin(console)); err == nil {
		t.Fatal("an anonymous websocket connection was accepted")
	}

	// The origin check is the server's: a page elsewhere is refused at
	// the handshake, before any credential is looked at.
	_, err = client.ConnectNkeyFile(l.WebsocketURL, devdir.UserNkeyPath(dir), devdir.LocalPrincipal,
		withOrigin("https://evil.example"), nats.MaxReconnects(0))
	if err == nil {
		t.Fatal("a websocket handshake from a foreign origin was accepted")
	}
}

// TestUpWebsocketOffLeavesNoURL: without the flag nothing is recorded —
// and a boot without the listener clears what an earlier boot recorded.
func TestUpWebsocketOffLeavesNoURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dir := t.TempDir()

	on, err := up.Up(ctx, up.Config{Dir: dir, Port: -1, WebsocketPort: -1})
	if err != nil {
		t.Fatalf("up with websocket: %v", err)
	}
	on.Stop()
	if _, err := os.Stat(devdir.WebsocketURLPath(dir)); err != nil {
		t.Fatalf("websocket url not recorded: %v", err)
	}

	off, err := up.Up(ctx, up.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up without websocket: %v", err)
	}
	defer off.Stop()
	if off.WebsocketURL != "" {
		t.Fatalf("websocket url %q without the listener", off.WebsocketURL)
	}
	if _, err := os.Stat(devdir.WebsocketURLPath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale websocket url survived a boot without the listener: %v", err)
	}
}
