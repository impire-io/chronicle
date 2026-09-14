// Package guestnet resolves the one piece of a placement's boot
// configuration that cannot be known before its microVM boots: where the
// host is. Each microsandbox gets its own /30 and gateway, and with the
// host network profile that gateway proxies to the host's loopback — so
// the backend hands the guest a URL whose host is the literal
// GatewayHost, and the workload binary swaps it for the default gateway
// it finds in its own routing table.
package guestnet

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// GatewayHost is the placeholder hostname the backend writes into a
// guest's NATS URL.
const GatewayHost = "msb-gateway"

// routeTable is the Linux kernel's route listing.
const routeTable = "/proc/net/route"

// ResolveURL swaps a GatewayHost host for the guest's default gateway.
// URLs naming any other host pass through untouched, so the same binary
// runs unmodified outside a sandbox.
func ResolveURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse url: %w", err)
	}
	if u.Hostname() != GatewayHost {
		return rawURL, nil
	}
	f, err := os.Open(routeTable)
	if err != nil {
		return "", fmt.Errorf("read the guest routing table (is this a linux guest?): %w", err)
	}
	defer func() { _ = f.Close() }()
	gw, err := defaultGateway(f)
	if err != nil {
		return "", err
	}
	u.Host = net.JoinHostPort(gw, u.Port())
	return u.String(), nil
}

// defaultGateway parses a /proc/net/route listing: the gateway of the
// first entry with an all-zero destination. Fields are little-endian hex
// words, kernel format.
func defaultGateway(r io.Reader) (string, error) {
	scanner := bufio.NewScanner(r)
	first := true
	for scanner.Scan() {
		if first {
			first = false // the header row
			continue
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 {
			continue
		}
		dest, gw := fields[1], fields[2]
		if dest != "00000000" {
			continue
		}
		raw, err := strconv.ParseUint(gw, 16, 32)
		if err != nil {
			continue
		}
		ip := make(net.IP, 4)
		binary.LittleEndian.PutUint32(ip, uint32(raw))
		return ip.String(), nil
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan routing table: %w", err)
	}
	return "", fmt.Errorf("no default route in the guest routing table")
}
