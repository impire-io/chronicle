// Package conformance is the SDK conformance suite (chronicle-hq design
// 12 § the conformance suite): the golden fixtures every SDK's pure rules
// are checked against, and the live scenarios every SDK runs against
// `chronicle up`. The fixtures and scenarios are JSON and language-
// agnostic; the tests in this package are the Go client's runner — the
// first conformant implementation. An SDK declares the contract version
// it implements and runs this suite of that version in its CI; the
// release archive carries the suite so the SDK's CI fetches the suite and
// the binaries by one version.
package conformance
