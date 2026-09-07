// Package contract declares the tenant data-plane wire contract of decision
// 0008 (chronicle-hq/03-DECISIONS/0008-tenant-data-plane-contract.md) as
// code: the subject grammar, the resource names, the operation record, the
// stream and bucket settings, and the META key grammar. It is declared once,
// here, for every service and the client alike; a service carrying its own
// copy is a defect. The package is a leaf: it imports no other chronicle
// package.
package contract

import "strings"

// Root is the one reserved subject root. Every chronicle subject in a tenant
// account lives under it; the member baseline grants CHRON.> once, at account
// creation, and no key is ever touched again (0008 point 2).
const Root = "CHRON"

// The subject grammar's case rule (wire contract § subject grammar):
// protocol tokens UPPERCASE, identifiers lowercase. Resource names — streams,
// KV buckets, object-store buckets — are ALL CAPS with underscores, the log
// name uppercased with "-" mapped to "_".

// MetaBucket is the per-account authoritative-configuration KV bucket
// (decision 0003; design 03-meta-and-state.md).
const MetaBucket = "META"

// OpTypeSnapshot is the one op type chronicle itself understands: the full
// materialised state of a thing plus the frontier (pattern § 5.1). Birth is
// a snapshot; rollup republishes one.
const OpTypeSnapshot = "snapshot"

// UpperLog maps a log name into resource names: uppercased, "-" mapped to
// "_". The mapping is unambiguous because log names admit no underscore.
func UpperLog(log string) string {
	return strings.ToUpper(strings.ReplaceAll(log, "-", "_"))
}

// StreamName is the log's stream: LOG_<LOG>, capturing the log's whole
// namespace.
func StreamName(log string) string {
	return "LOG_" + UpperLog(log)
}

// StateBucket is the log's derived-state KV bucket: STATE_<LOG>.
func StateBucket(log string) string {
	return "STATE_" + UpperLog(log)
}

// LogSubjects is the namespace the log's stream captures: CHRON.<log>.>.
func LogSubjects(log string) string {
	return Root + "." + log + ".>"
}

// OpsFilter is the ops family within the log's stream: CHRON.<log>.OPS.>.
func OpsFilter(log string) string {
	return Root + "." + log + ".OPS.>"
}

// OpsSubject is the exact subject of one thing's ops-log:
// CHRON.<log>.OPS.<thing…>. The thing is one or more caller-chosen tokens,
// already joined with ".". One thing = one exact subject; prefixes are for
// subscribing, never for correctness (pattern § 4.3).
func OpsSubject(log, thing string) string {
	return Root + "." + log + ".OPS." + thing
}

// OpsPrefix is the prefix a thing key is recovered from: subject minus
// prefix = the thing's subject tail, which is also its state-bucket key.
func OpsPrefix(log string) string {
	return Root + "." + log + ".OPS."
}

// ThingFromSubject recovers the thing tail from an exact ops subject, or ""
// when the subject is not under the log's ops family.
func ThingFromSubject(log, subject string) string {
	return strings.TrimPrefix(subject, OpsPrefix(log))
}
