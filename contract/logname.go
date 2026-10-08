package contract

import (
	"fmt"
	"regexp"
	"strings"
)

// NamePattern is the wire contract's rule for a name: a single lowercase
// token, [a-z0-9-]+ — the grammar of store, index, type, principal and
// child names. The artifact carries it; the SDKs restate it.
const NamePattern = `^[a-z0-9-]+$`

// InstanceTokenPattern is one token of an instance's path — deliberately
// narrower than NATS allows: it keeps every stored tail a valid KV key,
// the charset edge case 0008 flags for build-time verification. A token
// holds no "." and no "/", so the two spellings of a path convert both
// ways without ambiguity.
const InstanceTokenPattern = `^[a-zA-Z0-9_-]+$`

// PathSeparator is how a user writes a path: invoice/inv-1/comments/c-3.
// Stored — as a subject tail and a state-bucket key — the same tokens are
// joined with TailSeparator (decision 0044 § 2: the dotted form is how it
// is stored; a user types slashes).
const (
	PathSeparator = "/"
	TailSeparator = "."
)

// ReservedStoreNames is the short reserved list refused at store creation
// (wire contract § subject grammar), sorted.
var ReservedStoreNames = []string{"api", "meta", "sys"}

var nameToken = regexp.MustCompile(NamePattern)

var reservedStoreNames = func() map[string]bool {
	m := map[string]bool{}
	for _, name := range ReservedStoreNames {
		m[name] = true
	}
	return m
}()

// ValidateStoreName refuses anything but a single lowercase [a-z0-9-]+
// token outside the reserved list.
func ValidateStoreName(store string) error {
	if !nameToken.MatchString(store) {
		return fmt.Errorf("store name %q: must match [a-z0-9-]+", store)
	}
	if reservedStoreNames[store] {
		return fmt.Errorf("store name %q is reserved", store)
	}
	return nil
}

// ValidateIndexName refuses anything but a single lowercase [a-z0-9-]+
// token — the name grammar without the reserved list (05-indexes.md).
// The name is a subject token on the query surface and a META key segment;
// dots would fork both grammars.
func ValidateIndexName(index string) error {
	if !nameToken.MatchString(index) {
		return fmt.Errorf("index name %q: must match [a-z0-9-]+", index)
	}
	return nil
}

// ValidateTypeName refuses anything but a single lowercase [a-z0-9-]+
// token — the name grammar without the reserved list (decision 0021).
// A type name is a token in the path grammar and a META key segment;
// dots would collide with the retired op-type keys. Child names follow
// the same grammar.
func ValidateTypeName(name string) error {
	if !nameToken.MatchString(name) {
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
	if !nameToken.MatchString(name) {
		return fmt.Errorf("principal %q: must match [a-z0-9-]+", name)
	}
	if name == ServicePrincipal {
		return fmt.Errorf("principal %q is reserved for the account's own service", name)
	}
	return nil
}

// ValidateInstance refuses a stored tail that is not one or more
// token-safe segments joined with ".". Identifiers are the customer's
// domain: chronicle validates token safety and mints nothing (wire
// contract § subject grammar). Tokens must also be KV-key-safe, since the
// tail is the state bucket's key (03-meta-and-state.md § state buckets).
// The user-facing spelling is the path; ValidatePath and PathTail are
// its checks.
func ValidateInstance(tail string) error {
	if tail == "" {
		return fmt.Errorf("instance: must be one or more tokens")
	}
	for _, tok := range strings.Split(tail, TailSeparator) {
		if tok == "" {
			return fmt.Errorf("instance %q: empty token", tail)
		}
		if !instanceToken.MatchString(tok) {
			return fmt.Errorf("instance token %q: must match [a-zA-Z0-9_-]+", tok)
		}
	}
	return nil
}

// ValidatePath refuses a path that is not one or more token-safe segments
// joined with "/" — the user's spelling of an instance, type/id and a
// name/id pair per level of nesting. A dotted spelling is refused with the
// grammar named: one spelling, never two (decision 0044 § 6).
func ValidatePath(path string) error {
	if path == "" {
		return fmt.Errorf("path: must be type/id, with /name/id for each child")
	}
	for _, tok := range strings.Split(path, PathSeparator) {
		if tok == "" {
			return fmt.Errorf("path %q: empty segment (a path is type/id, with /name/id for each child)", path)
		}
		if !instanceToken.MatchString(tok) {
			if strings.Contains(tok, TailSeparator) {
				return fmt.Errorf("path %q: segments are separated by \"/\", not \".\" (a path is type/id, with /name/id for each child)", path)
			}
			return fmt.Errorf("path segment %q: must match [a-zA-Z0-9_-]+", tok)
		}
	}
	return nil
}

// PathTail converts a path to the tail it is stored under: the same
// tokens, dotted. It validates the path first.
func PathTail(path string) (string, error) {
	if err := ValidatePath(path); err != nil {
		return "", err
	}
	return strings.ReplaceAll(path, PathSeparator, TailSeparator), nil
}

// TailPath converts a stored tail back to the path a user reads.
func TailPath(tail string) string {
	return strings.ReplaceAll(tail, TailSeparator, PathSeparator)
}

// PathType is the type a path names: its first segment. The grammar's
// first pair is type/id (decision 0021 § 4); whether the type is defined
// is resolution's question, not the grammar's.
func PathType(path string) string {
	if i := strings.Index(path, PathSeparator); i >= 0 {
		return path[:i]
	}
	return path
}

var instanceToken = regexp.MustCompile(InstanceTokenPattern)
