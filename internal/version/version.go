// Package version holds the build-time version shared by this repo's binaries.
package version

import (
	"runtime/debug"
	"strings"
)

// Version is stamped at build time via
// -ldflags "-X github.com/impire-io/chronicle/internal/version.Version=x.y.z".
// It must stay valid semver: the micro service registration requires it.
var Version = "0.0.0-dev"

// The module path this package belongs to, as a build that depends on it
// records it.
const modulePath = "github.com/impire-io/chronicle"

// A build that imports this module — the managed service's binaries —
// stamps its own version, not this one; the node and the index kinds it
// composes then report the version of the open module they came from,
// read from the build's dependency list. A pseudo-version is valid
// semver; a local replacement reports "(devel)" and stays the dev value.
func init() {
	if Version != "0.0.0-dev" {
		return
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for _, d := range bi.Deps {
		if d.Path != modulePath || d.Version == "" || d.Version == "(devel)" {
			continue
		}
		Version = strings.TrimPrefix(d.Version, "v")
		return
	}
}
