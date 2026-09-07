// Package version holds the build-time version shared by this repo's binaries.
package version

// Version is stamped at build time via
// -ldflags "-X github.com/impire-io/chronicle/internal/version.Version=x.y.z".
// It must stay valid semver: the micro service registration requires it.
var Version = "0.0.0-dev"
