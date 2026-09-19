package mint_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/internal/mint"
)

// testRoot births a root — the material, the manifest, the first
// instance's bundle — in a temp dir.
func testRoot(t *testing.T) *mint.Root {
	t.Helper()
	r, err := mint.InitRoot(t.TempDir())
	if err != nil {
		t.Fatalf("init root: %v", err)
	}
	return r
}

func testBootstrap(t *testing.T) *mint.Bootstrap {
	t.Helper()
	b, err := mint.LoadOrInitBootstrap(t.TempDir())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	return b
}

func TestEmitClusterConfigsRendering(t *testing.T) {
	b := testBootstrap(t)
	cfgs, err := b.EmitClusterConfigs(mint.ClusterConfig{
		Nodes: []mint.ClusterNode{
			{Name: "nats-1", Host: "10.0.0.1"},
			{Name: "nats-2", Host: "10.0.0.2"},
			{Name: "nats-3", Host: "10.0.0.3"},
		},
		TLSCert: "/etc/chronicle/tls/cert.pem",
		TLSKey:  "/etc/chronicle/tls/key.pem",
	})
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if len(cfgs) != 3 {
		t.Fatalf("want 3 configs, got %d", len(cfgs))
	}
	for _, c := range cfgs {
		// Self-contained: operator inline, system account, every
		// bootstrap account preloaded, all three routes, TLS, defaults.
		for _, want := range []string{
			"server_name: " + c.Name,
			"listen: 0.0.0.0:4222",
			"operator: " + strings.TrimSpace(b.OperatorJWT),
			"system_account: " + b.SystemAccountPub,
			b.SystemAccountPub + ": " + b.SystemAccountJWT,
			b.ControlAccountPub + ": " + b.ControlAccountJWT,
			"nats-route://10.0.0.1:6222",
			"nats-route://10.0.0.2:6222",
			"nats-route://10.0.0.3:6222",
			"cert_file: '/etc/chronicle/tls/cert.pem'",
			"key_file: '/etc/chronicle/tls/key.pem'",
			"store_dir: '/var/lib/chronicle/jetstream'",
			"dir: '/var/lib/chronicle/resolver'",
			"type: full",
			"name: CHRONICLE",
		} {
			if !strings.Contains(c.Content, want) {
				t.Errorf("%s: missing %q", c.Name, want)
			}
		}
	}
}

// TestEmitClusterConfigsEncryptAtRest: every emitted node encrypts its
// store (design 10 § at rest), and the key is read from the node's
// environment — it never enters the file.
func TestEmitClusterConfigsEncryptAtRest(t *testing.T) {
	b := testBootstrap(t)
	cfgs, err := b.EmitClusterConfigs(mint.ClusterConfig{Nodes: []mint.ClusterNode{{Name: "n1", Host: "10.0.0.1"}}})
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	c := cfgs[0].Content
	if !strings.Contains(c, "cipher: chacha") || !strings.Contains(c, "key: $"+mint.JetStreamKeyEnv) {
		t.Fatalf("config does not encrypt at rest from the environment:\n%s", c)
	}
	if strings.Contains(c, "key: \"") || strings.Contains(c, "key: '") {
		t.Fatal("config carries a literal key")
	}
}

func TestEmitClusterConfigsRefusals(t *testing.T) {
	b := testBootstrap(t)
	one := []mint.ClusterNode{{Name: "n1", Host: "10.0.0.1"}}
	cases := []struct {
		name string
		cfg  mint.ClusterConfig
		want string
	}{
		{"no nodes", mint.ClusterConfig{}, "at least one"},
		{"bad name", mint.ClusterConfig{Nodes: []mint.ClusterNode{{Name: "a/b", Host: "h"}}}, "must match"},
		{"empty host", mint.ClusterConfig{Nodes: []mint.ClusterNode{{Name: "n1"}}}, "plain host"},
		{"dup name", mint.ClusterConfig{Nodes: []mint.ClusterNode{{Name: "n1", Host: "a"}, {Name: "n1", Host: "b"}}}, "duplicate"},
		{"tls half", mint.ClusterConfig{Nodes: one, TLSCert: "/c.pem"}, "come together"},
	}
	for _, tc := range cases {
		if _, err := b.EmitClusterConfigs(tc.cfg); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: want error containing %q, got %v", tc.name, tc.want, err)
		}
	}
	// No TLS block unless asked for.
	cfgs, err := b.EmitClusterConfigs(mint.ClusterConfig{Nodes: one})
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if strings.Contains(cfgs[0].Content, "tls {") {
		t.Error("tls block emitted without cert/key")
	}
}

// freePorts reserves n distinct loopback ports and releases them for the
// servers to take. The race window is fine at test scale.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, 0, n)
	listeners := make([]net.Listener, 0, n)
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return ports
}

// The spec's could-not-succeed-if-broken read: boot three real servers from
// the emitted files, mint an account through the driver against one node,
// and prove the driver's own verify-by-connecting plus a service user land
// on the *other* nodes. This is design 09's verified mechanism as a
// regression test.
func TestEmittedConfigsFormAClusterThatMintsEverywhere(t *testing.T) {
	r := testRoot(t)
	b := r.B
	bundle, err := mint.ReadBundle(r.BundleDir("instance-1"))
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	tmp := t.TempDir()
	ports := freePorts(t, 6)
	// The emitted configs read their JetStream key from the environment;
	// one key serves the three colocated nodes of this test.
	t.Setenv(mint.JetStreamKeyEnv, strings.Repeat("ab", 32))

	nodes := []mint.ClusterNode{
		{Name: "n1", Host: "127.0.0.1", ClientPort: ports[0], ClusterPort: ports[3]},
		{Name: "n2", Host: "127.0.0.1", ClientPort: ports[1], ClusterPort: ports[4]},
		{Name: "n3", Host: "127.0.0.1", ClientPort: ports[2], ClusterPort: ports[5]},
	}

	// Colocated nodes need distinct dirs, which one host never does: emit
	// the full topology once per node, keeping only that node's file, so
	// every booted config is verbatim emitter output.
	servers := make([]*server.Server, 0, 3)
	for i, n := range nodes {
		cfgs, err := b.EmitClusterConfigs(mint.ClusterConfig{
			Nodes:       nodes,
			ListenHost:  "127.0.0.1",
			StoreDir:    filepath.Join(tmp, fmt.Sprintf("js%d", i)),
			ResolverDir: filepath.Join(tmp, fmt.Sprintf("jwt%d", i)),
		})
		if err != nil {
			t.Fatalf("emit: %v", err)
		}
		path := filepath.Join(tmp, n.Name+".conf")
		if err := os.WriteFile(path, []byte(cfgs[i].Content), 0o644); err != nil {
			t.Fatalf("write config: %v", err)
		}
		opts, err := server.ProcessConfigFile(path)
		if err != nil {
			t.Fatalf("%s: process config: %v", n.Name, err)
		}
		opts.NoLog, opts.NoSigs = true, true
		srv, err := server.NewServer(opts)
		if err != nil {
			t.Fatalf("%s: new server: %v", n.Name, err)
		}
		go srv.Start()
		t.Cleanup(srv.Shutdown)
		servers = append(servers, srv)
	}
	for i, srv := range servers {
		if !srv.ReadyForConnections(10 * time.Second) {
			t.Fatalf("%s not ready", nodes[i].Name)
		}
	}

	// The driver pushes through node 1 and — by URL — verifies by
	// connecting to node 3: the mint ceremony itself proves propagation.
	sysConn, err := mint.ConnectCreds(servers[0].ClientURL(), bundle.SysCreds, "test-sys")
	if err != nil {
		t.Fatalf("connect system user to n1: %v", err)
	}
	defer sysConn.Close()
	// The keys the driver signs with live in the AUTH bucket (design 10):
	// seal the root into the cluster through node 2, R3 across the trio,
	// and the driver on node 1 reads them from there.
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	ctrlConn, err := mint.ConnectCreds(servers[1].ClientURL(), bundle.ControlCreds, "test-control")
	if err != nil {
		t.Fatalf("connect control user to n2: %v", err)
	}
	defer ctrlConn.Close()
	// JetStream's meta group elects a leader a beat after the servers
	// accept connections; the seal's R3 bucket needs it.
	jsc, err := jetstream.New(ctrlConn)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	var lastErr error
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		infoCtx, infoCancel := context.WithTimeout(ctx, 2*time.Second)
		_, lastErr = jsc.AccountInfo(infoCtx)
		infoCancel()
		if lastErr == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("jetstream never became ready on the cluster: %v", lastErr)
	}
	sealCtx, sealCancel := context.WithTimeout(ctx, 15*time.Second)
	_, err = r.Seal(sealCtx, ctrlConn, mint.SealOptions{Replicas: 3})
	sealCancel()
	if err != nil {
		t.Fatalf("seal into the cluster: %v", err)
	}
	c, err := mint.OpenCustody(ctx, ctrlConn)
	if err != nil {
		t.Fatalf("open custody: %v", err)
	}
	d, err := mint.NewJWTDriver(ctx, c, sysConn, servers[2].ClientURL())
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	acct, err := d.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint against the cluster: %v", err)
	}

	// And a freshly issued user lands on the remaining node.
	svc, err := mint.IssueServiceUser(acct, "chronicle-node")
	if err != nil {
		t.Fatalf("issue service user: %v", err)
	}
	nc, err := mint.ConnectCreds(servers[1].ClientURL(), svc.File, "test-service")
	if err != nil {
		t.Fatalf("service user on n2: %v", err)
	}
	nc.Close()
}
