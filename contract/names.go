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

import (
	"fmt"
	"strings"
)

// Root is the one reserved subject root. Every chronicle subject in an
// account lives under it; the member baseline grants a member its share
// of it once, at account creation (MemberBaselinePublish), and creating a
// store touches no key (0008 point 2, 0057).
const Root = "CHRON"

// APIRoot is the control plane's root. Every verb is spoken on
// CHRON.API.<principal>.<VERB…>: the caller's name after API, the
// caller's alone under the member baseline, so a responder reads the
// caller from the subject and never from a payload (decision 0057).
const APIRoot = Root + ".API"

// LiveRoot is the account's ephemeral namespace, CHRON.LIVE.> (decision
// 0055): the members' own, captured by nothing, answered by nothing.
const LiveRoot = Root + ".LIVE"

// APISubject is the subject a caller speaks one verb on:
// CHRON.API.<principal>.<verb>, the verb a dotted protocol token.
func APISubject(principal, verb string) string {
	return APIRoot + "." + principal + "." + verb
}

// APIEndpoint is the subject a responder serves one verb on, whoever
// calls: CHRON.API.*.<verb>.
func APIEndpoint(verb string) string {
	return APIRoot + ".*." + verb
}

// CallerFromSubject reads the caller a request subject carries — the
// third token of CHRON.API.<principal>.…, a principal name or the service
// principal — and refuses anything else. The permission is the identity:
// a member may publish there under its own name only.
func CallerFromSubject(subject string) (string, error) {
	toks := strings.Split(subject, ".")
	if len(toks) < 4 || toks[0] != Root || toks[1] != "API" {
		return "", fmt.Errorf("subject %q is not CHRON.API.<principal>.<verb>", subject)
	}
	caller := toks[2]
	if caller == ServicePrincipal {
		return caller, nil
	}
	if err := ValidatePrincipalName(caller); err != nil {
		return "", fmt.Errorf("caller in subject %q: %w", subject, err)
	}
	return caller, nil
}

// MemberBaselinePublish is what every member may publish (design 01
// § creating an account, decision 0057): every store's operations, the
// ephemeral namespace, the control verbs under the member's own name,
// the whoami request, and the read half of the JetStream API — account
// information included, which is the account's own limits and usage (the
// console's "used of allowed") and what the JavaScript client asks for
// before it manages anything. The principal is the member's name — or a
// server-side template such as "{{name()}}" where one scope serves every
// member. No member writes a bucket, creates or deletes a stream, or
// speaks a verb as another.
func MemberBaselinePublish(principal string) []string {
	return []string{
		Root + ".*.OPS.>",
		LiveRoot + ".>",
		APIRoot + "." + principal + ".>",
		"$SYS.REQ.USER.INFO",
		"$JS.API.INFO",
		"$JS.API.CONSUMER.>",
		"$JS.API.STREAM.INFO.>",
		"$JS.API.STREAM.NAMES",
		"$JS.API.STREAM.MSG.GET.>",
		"$JS.API.DIRECT.GET.>",
	}
}

// MemberBaselineSubscribe is what every member may subscribe to: every
// store's operations, the ephemeral namespace, and reply inboxes — never
// the verbs, so no member answers a request in the node's place.
func MemberBaselineSubscribe() []string {
	return []string{
		Root + ".*.OPS.>",
		LiveRoot + ".>",
		"_INBOX.>",
	}
}

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
