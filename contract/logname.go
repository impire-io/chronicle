package contract

import (
	"fmt"
	"regexp"
	"strings"
)

// logName is the wire contract's rule: a single lowercase token, [a-z0-9-]+.
var logName = regexp.MustCompile(`^[a-z0-9-]+$`)

// reservedLogNames is the short reserved list refused at log creation
// (wire contract § subject grammar).
var reservedLogNames = map[string]bool{"api": true, "sys": true, "meta": true}

// ValidateLogName refuses anything but a single lowercase [a-z0-9-]+ token
// outside the reserved list.
func ValidateLogName(log string) error {
	if !logName.MatchString(log) {
		return fmt.Errorf("log name %q: must match [a-z0-9-]+", log)
	}
	if reservedLogNames[log] {
		return fmt.Errorf("log name %q is reserved", log)
	}
	return nil
}

// ValidateIndexName refuses anything but a single lowercase [a-z0-9-]+
// token — the log-name grammar without the reserved list (05-indexes.md).
// The name is a subject token on the query surface and a META key segment;
// dots would fork both grammars.
func ValidateIndexName(index string) error {
	if !logName.MatchString(index) {
		return fmt.Errorf("index name %q: must match [a-z0-9-]+", index)
	}
	return nil
}

// ValidateTypeName refuses anything but a single lowercase [a-z0-9-]+
// token — the log-name grammar without the reserved list (decision 0021).
// A type name is a subject token in the pair addressing and a META key
// segment; dots would collide with the retired op-type keys. Aspect
// segment names follow the same grammar.
func ValidateTypeName(name string) error {
	if !logName.MatchString(name) {
		return fmt.Errorf("type name %q: must match [a-z0-9-]+", name)
	}
	return nil
}

// ValidatePrincipalName refuses anything but a single lowercase
// [a-z0-9-]+ token — the identifier grammar: the ID becomes a registry key
// segment (identity.member.<id> must stay one KV token) and a default
// .creds filename. The service principal is reserved: it is never a
// member.
func ValidatePrincipalName(name string) error {
	if !logName.MatchString(name) {
		return fmt.Errorf("principal %q: must match [a-z0-9-]+", name)
	}
	if name == ServicePrincipal {
		return fmt.Errorf("principal %q is reserved for the tenant's own service", name)
	}
	return nil
}

// ValidateThing refuses a thing that is not one or more subject-token-safe
// segments joined with ".". Identifiers are the customer's domain: chronicle
// validates subject-token safety and mints nothing (wire contract § subject
// grammar). Tokens must also be KV-key-safe, since the thing tail is the
// state bucket's key (03-meta-and-state.md § state buckets).
func ValidateThing(thing string) error {
	if thing == "" {
		return fmt.Errorf("thing: must be one or more tokens")
	}
	for _, tok := range strings.Split(thing, ".") {
		if tok == "" {
			return fmt.Errorf("thing %q: empty token", thing)
		}
		if !thingToken.MatchString(tok) {
			return fmt.Errorf("thing token %q: must match [a-zA-Z0-9_-]+", tok)
		}
	}
	return nil
}

// thingToken is deliberately narrower than NATS allows: it keeps every thing
// tail a valid KV key, the charset edge case 0008 flags for build-time
// verification.
var thingToken = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
