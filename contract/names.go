// Package contract declares the account data-plane wire contract of
// decision 0008 (chronicle-hq/03-DECISIONS/0008-tenant-data-plane-contract.md)
// as code: the subject grammar, the resource names, the operation record,
// the stream and bucket settings, and the META key grammar. It is declared
// once, here, for every service and the client alike; a service carrying
// its own copy is a defect. The package is a leaf: it imports no other
// chronicle package.
//
// The words are the user's (chronicle-hq/00-META/vocabulary.md, decision
// 0044): a store, its types, their instances named by paths, children,
// snapshots. The protocol tokens underneath — CHRON, OPS, API, the verb
// names, LOG_ and STATE_ resource names, the log. META prefix — are the
// node's and the operator's, and do not change with the words.
package contract

import "strings"

// Root is the one reserved subject root. Every chronicle subject in an
// account lives under it; the member baseline grants CHRON.> once, at
// account creation, and no key is ever touched again (0008 point 2).
const Root = "CHRON"

// The subject grammar's case rule (wire contract § subject grammar):
// protocol tokens UPPERCASE, identifiers lowercase. Resource names —
// streams, KV buckets, object-store buckets — are ALL CAPS with
// underscores, the store name uppercased with "-" mapped to "_".

// MetaBucket is the per-account authoritative-configuration KV bucket
// (decision 0003; design 03-meta-and-state.md).
const MetaBucket = "META"

// OpTypeSnapshot is the one op type chronicle itself understands: the full
// materialised state of an instance plus the frontier (pattern § 5.1). An
// untyped instance is created by one; taking a snapshot republishes one.
const OpTypeSnapshot = "snapshot"

// UpperStore maps a store name into resource names: uppercased, "-" mapped
// to "_". The mapping is unambiguous because store names admit no
// underscore.
func UpperStore(store string) string {
	return strings.ToUpper(strings.ReplaceAll(store, "-", "_"))
}

// StreamName is the store's stream: LOG_<STORE>, capturing the store's
// whole namespace.
func StreamName(store string) string {
	return "LOG_" + UpperStore(store)
}

// StateBucket is the store's derived-state KV bucket: STATE_<STORE>.
func StateBucket(store string) string {
	return "STATE_" + UpperStore(store)
}

// StoreSubjects is the namespace the store's stream captures: CHRON.<store>.>.
func StoreSubjects(store string) string {
	return Root + "." + store + ".>"
}

// OpsFilter is the ops family within the store's stream: CHRON.<store>.OPS.>.
func OpsFilter(store string) string {
	return Root + "." + store + ".OPS.>"
}

// OpsSubject is the exact subject of one instance's history:
// CHRON.<store>.OPS.<tail>. The tail is the instance's path in its stored
// form — the tokens joined with ".". One instance = one exact subject;
// prefixes are for subscribing, never for correctness (pattern § 4.3).
func OpsSubject(store, tail string) string {
	return Root + "." + store + ".OPS." + tail
}

// OpsPrefix is the prefix an instance key is recovered from: subject minus
// prefix = the instance's tail, which is also its state-bucket key.
func OpsPrefix(store string) string {
	return Root + "." + store + ".OPS."
}

// InstanceFromSubject recovers the instance's tail from an exact ops
// subject, or "" when the subject is not under the store's ops family.
func InstanceFromSubject(store, subject string) string {
	return strings.TrimPrefix(subject, OpsPrefix(store))
}
