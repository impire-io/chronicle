package contract

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// DefaultMaxBytes is every stream's default byte budget: 1 GiB, DiscardNew
// at the cap, per-log override in META (0008 point 3).
const DefaultMaxBytes = 1 << 30

// DuplicateWindow is the stream dedup window: longer than any SDK retry, so
// a retried publish dedups instead of double-appending.
const DuplicateWindow = 2 * time.Minute

// LogStreamConfig is the wire contract's stream settings table, applied by
// chronicle at log creation — the customer never hand-configures a stream.
// maxBytes zero means the decided default.
func LogStreamConfig(log string, maxBytes int64) jetstream.StreamConfig {
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	return jetstream.StreamConfig{
		Name:        StreamName(log),
		Description: "chronicle ops-log: " + log,
		Subjects:    []string{LogSubjects(log)},
		Retention:   jetstream.LimitsPolicy,
		// MaxAge stays unset: rollup keeps the stream small; age-based
		// expiry would delete independently of the state referencing it.
		AllowRollup: true,
		DenyDelete:  true,
		Duplicates:  DuplicateWindow,
		Storage:     jetstream.FileStorage,
		MaxBytes:    maxBytes,
		Discard:     jetstream.DiscardNew,
	}
}

// MetaBucketConfig is the per-account META bucket. History depth 64 (the KV
// maximum) keeps schema revisions readable: a projection judges an old op by
// the schema it was written under (03-meta-and-state.md § op-type schemas).
func MetaBucketConfig() jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{
		Bucket:      MetaBucket,
		Description: "chronicle authoritative configuration (decision 0003)",
		History:     64,
		Storage:     jetstream.FileStorage,
	}
}

// StateBucketConfig is a log's derived-state bucket. Per-key history on:
// "the last five states of thing N" is a history read, no replay.
func StateBucketConfig(log string) jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{
		Bucket:      StateBucket(log),
		Description: "chronicle derived state: " + log,
		History:     5,
		Storage:     jetstream.FileStorage,
	}
}
