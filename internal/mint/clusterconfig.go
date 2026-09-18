package mint

import (
	"fmt"
	"regexp"
	"strings"
)

// The hosted environment's server half (02-DESIGN/09-hosted-environment.md
// § the substrate): the bootstrap material rendered as self-contained
// nats-server configs for an external cluster. The emit is pure rendering —
// no network, no writes outside the caller's hands, no seeds in the output;
// the operator JWT and account JWTs it inlines are public material.

// ClusterNode names one server of the cluster. Name becomes server_name and
// the config's filename; Host is the address the other nodes' routes and the
// clients dial. The port overrides exist for colocated nodes (tests, the
// single-box beta tier); a production topology of one node per host leaves
// them zero and takes the cluster-wide defaults.
type ClusterNode struct {
	Name        string
	Host        string
	ClientPort  int
	ClusterPort int
}

// ClusterConfig is the rendering input beyond the bootstrap material. Dirs
// and TLS paths are target-host paths, emitted verbatim and never touched
// locally.
type ClusterConfig struct {
	Nodes       []ClusterNode
	ClientPort  int    // default 4222
	ClusterPort int    // default 6222
	ClusterName string // default CHRONICLE
	TLSCert     string // client-listener TLS; both with TLSKey or neither
	TLSKey      string
	StoreDir    string // default /var/lib/chronicle/jetstream
	ResolverDir string // default /var/lib/chronicle/resolver
	ListenHost  string // bind address for both listeners; default 0.0.0.0
}

// NodeConfig is one emitted server config, named after its node.
type NodeConfig struct {
	Name    string
	Content string
}

const (
	defaultClientPort  = 4222
	defaultClusterPort = 6222
	defaultClusterName = "CHRONICLE"
	defaultStoreDir    = "/var/lib/chronicle/jetstream"
	defaultResolverDir = "/var/lib/chronicle/resolver"
)

// nodeNameRe bounds what may become a filename and a server_name.
var nodeNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// preloadEntry is one account the emitted resolver starts with.
type preloadEntry struct {
	pub string
	jwt string
}

// preloadAccounts is every account the bootstrap holds. A bootstrap account
// added later (the AUTH account of design 08 among them) joins the preload
// here and rides into every emitted config without touching the rendering.
func (b *Bootstrap) preloadAccounts() []preloadEntry {
	return []preloadEntry{
		{pub: b.SystemAccountPub, jwt: b.SystemAccountJWT},
		{pub: b.ControlAccountPub, jwt: b.ControlAccountJWT},
		{pub: b.AuthAccountPub, jwt: b.AuthAccountJWT},
	}
}

// EmitClusterConfigs renders one self-contained nats-server config per node:
// trusted operator inline, system account, full dir resolver preloaded with
// every bootstrap account, JetStream, cluster routes to every node, TLS when
// configured.
func (b *Bootstrap) EmitClusterConfigs(cfg ClusterConfig) ([]NodeConfig, error) {
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("emit-cluster-config: at least one --node is required")
	}
	if (cfg.TLSCert == "") != (cfg.TLSKey == "") {
		return nil, fmt.Errorf("emit-cluster-config: --tls-cert and --tls-key come together or not at all")
	}
	clientPort := cfg.ClientPort
	if clientPort == 0 {
		clientPort = defaultClientPort
	}
	clusterPort := cfg.ClusterPort
	if clusterPort == 0 {
		clusterPort = defaultClusterPort
	}
	clusterName := cfg.ClusterName
	if clusterName == "" {
		clusterName = defaultClusterName
	}
	storeDir := cfg.StoreDir
	if storeDir == "" {
		storeDir = defaultStoreDir
	}
	resolverDir := cfg.ResolverDir
	if resolverDir == "" {
		resolverDir = defaultResolverDir
	}
	listenHost := cfg.ListenHost
	if listenHost == "" {
		listenHost = "0.0.0.0"
	}

	seen := make(map[string]bool, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		if !nodeNameRe.MatchString(n.Name) {
			return nil, fmt.Errorf("emit-cluster-config: node name %q must match %s", n.Name, nodeNameRe)
		}
		if n.Host == "" || strings.ContainsAny(n.Host, " \t\n'\"") {
			return nil, fmt.Errorf("emit-cluster-config: node %q needs a plain host address", n.Name)
		}
		if seen[n.Name] {
			return nil, fmt.Errorf("emit-cluster-config: duplicate node name %q", n.Name)
		}
		seen[n.Name] = true
	}

	// Identical route lists on every node keep the configs
	// order-independent; nats-server tolerates the self-route.
	var routes strings.Builder
	for _, n := range cfg.Nodes {
		fmt.Fprintf(&routes, "    nats-route://%s:%d\n", n.Host, portOr(n.ClusterPort, clusterPort))
	}

	var preload strings.Builder
	for _, a := range b.preloadAccounts() {
		fmt.Fprintf(&preload, "  %s: %s\n", a.pub, a.jwt)
	}

	out := make([]NodeConfig, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		var c strings.Builder
		fmt.Fprintf(&c, "# chronicle cluster node %s — emitted by `chronicle operator emit-cluster-config`.\n", n.Name)
		fmt.Fprintf(&c, "# Regenerate from the bootstrap dir instead of editing.\n\n")
		fmt.Fprintf(&c, "server_name: %s\n", n.Name)
		fmt.Fprintf(&c, "listen: %s:%d\n\n", listenHost, portOr(n.ClientPort, clientPort))
		if cfg.TLSCert != "" {
			fmt.Fprintf(&c, "tls {\n  cert_file: '%s'\n  key_file: '%s'\n}\n\n", cfg.TLSCert, cfg.TLSKey)
		}
		fmt.Fprintf(&c, "operator: %s\n", strings.TrimSpace(b.OperatorJWT))
		fmt.Fprintf(&c, "system_account: %s\n\n", b.SystemAccountPub)
		fmt.Fprintf(&c, "jetstream {\n  store_dir: '%s'\n}\n\n", storeDir)
		fmt.Fprintf(&c, "cluster {\n  name: %s\n  listen: %s:%d\n  routes: [\n%s  ]\n}\n\n",
			clusterName, listenHost, portOr(n.ClusterPort, clusterPort), routes.String())
		fmt.Fprintf(&c, "resolver {\n  type: full\n  dir: '%s'\n  interval: \"2m\"\n  timeout: \"1.9s\"\n}\n\n", resolverDir)
		fmt.Fprintf(&c, "resolver_preload {\n%s}\n", preload.String())
		out = append(out, NodeConfig{Name: n.Name, Content: c.String()})
	}
	return out, nil
}

func portOr(p, fallback int) int {
	if p != 0 {
		return p
	}
	return fallback
}
