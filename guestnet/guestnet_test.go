package guestnet

import (
	"strings"
	"testing"
)

// A route table captured from a live msb sandbox (alpine, --net host):
// default via 172.16.0.137, kernel little-endian hex format.
const capturedRoutes = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT
eth0	00000000	890010AC	0003	0	0	0	00000000	0	0	0
eth0	880010AC	00000000	0001	0	0	0	FCFFFFFF	0	0	0
`

func TestDefaultGatewayFromCapturedTable(t *testing.T) {
	gw, err := defaultGateway(strings.NewReader(capturedRoutes))
	if err != nil {
		t.Fatalf("defaultGateway: %v", err)
	}
	if gw != "172.16.0.137" {
		t.Fatalf("gateway = %q, want 172.16.0.137", gw)
	}
}

func TestDefaultGatewayNoDefaultRoute(t *testing.T) {
	table := `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT
eth0	880010AC	00000000	0001	0	0	0	FCFFFFFF	0	0	0
`
	if _, err := defaultGateway(strings.NewReader(table)); err == nil {
		t.Fatal("a table without a default route resolved anyway")
	}
}

func TestResolveURLPassesOtherHostsThrough(t *testing.T) {
	for _, u := range []string{"nats://127.0.0.1:4222", "nats://example.com:4222"} {
		got, err := ResolveURL(u)
		if err != nil || got != u {
			t.Fatalf("ResolveURL(%q) = %q, %v — want untouched", u, got, err)
		}
	}
}
