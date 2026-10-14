package contract

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// DefaultMaxBytes is every stream's default byte budget: 1 GiB, DiscardNew
// at the cap, per-log override in META (0008 point 3).
const DefaultMaxBytes = 1 << 30

// StoreBudget is the byte budget a new log's stream gets (decision 0039):
// the creator's override when it names one, else the contract's default
// — or the account's per-stream cap when that is smaller. An account cap
// of zero or less means none.
func StoreBudget(requested, accountCap int64) int64 {
	if requested > 0 {
		return requested
	}
	if accountCap > 0 && accountCap < DefaultMaxBytes {
		return accountCap
	}
	return DefaultMaxBytes
}

// DuplicateWindow is the stream dedup window: longer than any SDK retry, so
// a retried publish dedups instead of double-appending.
const DuplicateWindow = 2 * time.Minute

// StoreStreamConfig is the wire contract's stream settings table, applied by
// chronicle at log creation — the customer never hand-configures a stream.
// maxBytes zero means the decided default; history is the log's 0019
// declaration (empty means compactable) — a preserved log's stream refuses
// rollup writes and purges outright, so nothing can replace or remove a
// subject's history.
// schedules turns on NATS message schedules and per-message TTLs
// (decision 0050): the server appends a registered schedule's ticks
// into the store's own stream, whether any reader is running.
func StoreStreamConfig(log string, maxBytes int64, history string, schedules bool) jetstream.StreamConfig {
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	return jetstream.StreamConfig{
		Name:        StreamName(log),
		Description: "chronicle ops-log: " + log,
		Subjects:    []string{StoreSubjects(log)},
		Retention:   jetstream.LimitsPolicy,
		// MaxAge stays unset: rollup keeps the stream small; age-based
		// expiry would delete independently of the state referencing it.
		AllowRollup: NormalizeHistory(history) != HistoryFull,
		// DenyDelete refuses a single message's delete only; a purge is
		// its own API call, refused by DenyPurge — which the server
		// allows only where rollup is refused (0019 as amended).
		DenyDelete: true,
		DenyPurge:  NormalizeHistory(history) == HistoryFull,
		Duplicates: DuplicateWindow,
		Storage:    jetstream.FileStorage,
		MaxBytes:   maxBytes,
		Discard:    discardFor(schedules),
		// Off by default: a store that allows schedules says so at creation.
		AllowMsgSchedules: schedules,
		AllowMsgTTL:       schedules,
	}
}

// discardFor is the byte budget's edge: a store refuses new appends at its
// cap (0008 point 3) — except a store that allows schedules, which the
// server refuses to create with DiscardNew ("message scheduling cannot
// use discard new"); it discards its oldest operations instead, which is
// what a store of ticks wants and what decision 0050 says it accepts.
func discardFor(schedules bool) jetstream.DiscardPolicy {
	if schedules {
		return jetstream.DiscardOld
	}
	return jetstream.DiscardNew
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
