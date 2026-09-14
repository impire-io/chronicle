package executor

import (
	"strings"
	"testing"
)

// TestMsbRunArgsIsThePinnedSurface: the exact invocation this build makes
// against msb 0.6.8. A diff here is an API-surface change and a
// deliberate revisit of the pin, never an incidental edit.
func TestMsbRunArgsIsThePinnedSurface(t *testing.T) {
	args := msbRunArgs("alpine", "chron-acme-index-orders-text", "/tmp/wl", "/tmp/stage/service.creds", "nats://msb-gateway:4222", Spec{
		Tenant: "acme", Workload: "index-orders-text", Slot: "0",
		Kind: "index-search", Log: "orders", Index: "text",
	})
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
