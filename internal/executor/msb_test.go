package executor

import (
	"strings"
	"testing"

	"github.com/impire-io/chronicle/internal/index/semantic"
)

// TestMsbRunArgsIsThePinnedSurface: the exact invocation this build makes
// against msb 0.6.8. A diff here is an API-surface change and a
// deliberate revisit of the pin, never an incidental edit.
func TestMsbRunArgsIsThePinnedSurface(t *testing.T) {
	args := msbRunArgs("alpine", "chron-acme-index-orders-text", "/tmp/wl", "/tmp/stage/service.creds", "", "nats://msb-gateway:4222", Spec{
		Tenant: "acme", Workload: "index-orders-text", Slot: "0",
		Kind: "index-search", Log: "orders", Index: "text",
	}, nil)
	want := "run alpine " +
		"--name chron-acme-index-orders-text " +
		"--net host " +
		"--copy-file /tmp/wl:/chronicle-workload " +
		"--copy-file /tmp/stage/service.creds:/service.creds " +
		"-- /chronicle-workload --kind index-search --url nats://msb-gateway:4222 --creds /service.creds " +
		"--log orders --index text"
	if got := strings.Join(args, " "); got != want {
		t.Fatalf("pinned surface drifted:\n got  %s\n want %s", got, want)
	}

	// The semantic kind adds the second secret and the provider flags.
	args = msbRunArgs("alpine", "chron-acme-index-orders-meaning", "/tmp/wl", "/tmp/stage/service.creds", "/tmp/stage/embedding.key", "nats://msb-gateway:4222", Spec{
		Tenant: "acme", Workload: "index-orders-meaning", Slot: "0",
		Kind: "index-semantic", Log: "orders", Index: "meaning",
	}, &semantic.ProviderConfig{BaseURL: "http://host:1234/v1", Model: "embed-model", APIKey: "secret"})
	want = "run alpine " +
		"--name chron-acme-index-orders-meaning " +
		"--net host " +
		"--copy-file /tmp/wl:/chronicle-workload " +
		"--copy-file /tmp/stage/service.creds:/service.creds " +
		"--copy-file /tmp/stage/embedding.key:/embedding.key " +
		"-- /chronicle-workload --kind index-semantic --url nats://msb-gateway:4222 --creds /service.creds " +
		"--log orders --index meaning " +
		"--embedding-url http://host:1234/v1 --embedding-model embed-model --embedding-key-file /embedding.key"
	if got := strings.Join(args, " "); got != want {
		t.Fatalf("semantic pinned surface drifted:\n got  %s\n want %s", got, want)
	}
}

func TestGuestURLKeepsThePort(t *testing.T) {
	got, err := guestURL("nats://127.0.0.1:50532")
	if err != nil || got != "nats://msb-gateway:50532" {
		t.Fatalf("guestURL = %q, %v", got, err)
	}
	got, err = guestURL("nats://example.com")
	if err != nil || got != "nats://msb-gateway:4222" {
		t.Fatalf("portless guestURL = %q, %v", got, err)
	}
}
