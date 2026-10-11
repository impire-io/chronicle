package contract

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestUpperLogAndResourceNames(t *testing.T) {
	cases := []struct{ log, stream, state string }{
		{"my-log", "LOG_MY_LOG", "STATE_MY_LOG"},
		{"orders", "LOG_ORDERS", "STATE_ORDERS"},
		{"a1-b2", "LOG_A1_B2", "STATE_A1_B2"},
	}
	for _, c := range cases {
		if got := StreamName(c.log); got != c.stream {
			t.Errorf("StreamName(%q) = %q, want %q", c.log, got, c.stream)
		}
		if got := StateBucket(c.log); got != c.state {
			t.Errorf("StateBucket(%q) = %q, want %q", c.log, got, c.state)
		}
	}
}

func TestSubjectGrammar(t *testing.T) {
	if got := StoreSubjects("orders"); got != "CHRON.orders.>" {
		t.Errorf("StoreSubjects = %q", got)
	}
	if got := OpsFilter("orders"); got != "CHRON.orders.OPS.>" {
		t.Errorf("OpsFilter = %q", got)
	}
	subj := OpsSubject("orders", "invoice-42.line-3")
	if subj != "CHRON.orders.OPS.invoice-42.line-3" {
		t.Errorf("OpsSubject = %q", subj)
	}
	if got := InstanceFromSubject("orders", subj); got != "invoice-42.line-3" {
		t.Errorf("InstanceFromSubject = %q", got)
	}
}

func TestValidateLogName(t *testing.T) {
	for _, ok := range []string{"orders", "my-log", "a1", "x"} {
		if err := ValidateStoreName(ok); err != nil {
			t.Errorf("ValidateStoreName(%q): unexpected error %v", ok, err)
		}
	}
	for _, bad := range []string{"", "Orders", "my_log", "my.log", "api", "sys", "meta", "a b", "CHRON"} {
		if err := ValidateStoreName(bad); err == nil {
			t.Errorf("ValidateStoreName(%q): expected refusal", bad)
		}
	}
}

func TestValidateThing(t *testing.T) {
	for _, ok := range []string{"invoice-42", "invoice-42.line-3", "A.b.c_1"} {
		if err := ValidateInstance(ok); err != nil {
			t.Errorf("ValidateInstance(%q): unexpected error %v", ok, err)
		}
	}
	for _, bad := range []string{"", ".", "a..b", "a b", "a.*", "a.>", "a/b"} {
		if err := ValidateInstance(bad); err == nil {
			t.Errorf("ValidateInstance(%q): expected refusal", bad)
		}
	}
}

func TestOpHeaderRoundTrip(t *testing.T) {
	ts := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	in := Op{
		ID:      "op-1",
		Type:    "comment.add",
		Author:  "alice",
		Parents: []string{"p1", "p2"},
		Ts:      ts,
	}
	h := in.Header()
	out := ParseOp("CHRON.orders.OPS.invoice-42", 7, h, []byte(`{"body":"x"}`))
	if out.ID != "op-1" || out.Type != "comment.add" || out.Author != "alice" {
		t.Errorf("round trip lost identity: %+v", out)
	}
	if len(out.Parents) != 2 || out.Parents[0] != "p1" || out.Parents[1] != "p2" {
		t.Errorf("round trip lost parents: %v", out.Parents)
	}
	if !out.Ts.Equal(ts) {
		t.Errorf("round trip lost ts: %v", out.Ts)
	}
	if out.Version != EnvelopeVersion {
		t.Errorf("version defaulted to %q, want %q", out.Version, EnvelopeVersion)
	}
	if out.Seq != 7 || out.Subject != "CHRON.orders.OPS.invoice-42" {
		t.Errorf("round trip lost placement: %+v", out)
	}
}

func TestLogStreamConfig(t *testing.T) {
	cfg := StoreStreamConfig("my-log", 0, "", false)
	if cfg.Name != "LOG_MY_LOG" {
		t.Errorf("stream name %q", cfg.Name)
	}
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "CHRON.my-log.>" {
		t.Errorf("subjects %v: one stream captures the log's whole namespace", cfg.Subjects)
	}
	if cfg.MaxAge != 0 {
		t.Error("MaxAge must stay unset: rollup keeps the stream small")
	}
	if !cfg.AllowRollup || !cfg.DenyDelete {
		t.Error("AllowRollup and DenyDelete are the 0008 table")
	}
	if cfg.Duplicates != DuplicateWindow {
		t.Errorf("dedup window %v", cfg.Duplicates)
	}
	if cfg.MaxBytes != DefaultMaxBytes {
		t.Errorf("default byte budget %d", cfg.MaxBytes)
	}
	if cfg.MaxBytes != 1<<30 {
		t.Errorf("decided default is 1 GiB, got %d", cfg.MaxBytes)
	}
	if over := StoreStreamConfig("my-log", 42, "", false); over.MaxBytes != 42 {
		t.Errorf("per-log override lost: %d", over.MaxBytes)
	}
	// The 0019 declaration reaches the stream: a preserved log refuses
	// rollup writes outright, and everything else in the table stands.
	preserved := StoreStreamConfig("my-log", 0, HistoryFull, false)
	if preserved.AllowRollup {
		t.Error("a preserved log's stream must refuse rollup writes (0019)")
	}
	if !preserved.DenyDelete {
		t.Error("DenyDelete stands on a preserved log")
	}
	if explicit := StoreStreamConfig("my-log", 0, HistoryCompactable, false); !explicit.AllowRollup {
		t.Error("an explicitly compactable log keeps AllowRollup")
	}
}

func TestMetaKeys(t *testing.T) {
	if got := MetaStoreConfig("orders"); got != "log.orders.config" {
		t.Errorf("MetaStoreConfig = %q", got)
	}
	if got := MetaStoreType("orders", "comment.add"); got != "log.orders.type.comment.add" {
		t.Errorf("MetaStoreType = %q", got)
	}
	if got := MetaMember("alice"); got != "identity.member.alice" {
		t.Errorf("MetaMember = %q", got)
	}
	for _, ok := range []string{"log.orders.config", "identity.principal.alice", "index.orders.search"} {
		if !MetaKeyAllowed(ok) {
			t.Errorf("MetaKeyAllowed(%q) = false", ok)
		}
	}
	for _, bad := range []string{"custom.key", "orders", "logs.x"} {
		if MetaKeyAllowed(bad) {
			t.Errorf("MetaKeyAllowed(%q) = true: no keys outside the reserved prefixes", bad)
		}
	}
}

// TestLogBudget: an override wins; otherwise the default, bounded by the
// account's per-stream cap when there is one (decision 0039).
func TestLogBudget(t *testing.T) {
	for _, c := range []struct{ requested, accountCap, want int64 }{
		{0, 0, DefaultMaxBytes},
		{0, -1, DefaultMaxBytes},
		{0, 64 << 20, 64 << 20},
		{0, 4 << 30, DefaultMaxBytes},
		{8 << 20, 64 << 20, 8 << 20},
		{8 << 20, 0, 8 << 20},
	} {
		if got := StoreBudget(c.requested, c.accountCap); got != c.want {
			t.Errorf("StoreBudget(%d, %d) = %d, want %d", c.requested, c.accountCap, got, c.want)
		}
	}
}

// A store that allows schedules turns on exactly the two server features a
// registered schedule needs — message schedules and per-message TTLs — and
// nothing else changes; a store that does not keeps both off (decision 0050).
func TestStoreStreamConfigSchedules(t *testing.T) {
	off := StoreStreamConfig("ticks", 0, "", false)
	if off.AllowMsgSchedules || off.AllowMsgTTL {
		t.Errorf("schedules off: AllowMsgSchedules=%v AllowMsgTTL=%v", off.AllowMsgSchedules, off.AllowMsgTTL)
	}
	on := StoreStreamConfig("ticks", 0, "", true)
	if !on.AllowMsgSchedules || !on.AllowMsgTTL {
		t.Errorf("schedules on: AllowMsgSchedules=%v AllowMsgTTL=%v", on.AllowMsgSchedules, on.AllowMsgTTL)
	}
	if on.AllowRollup != off.AllowRollup || on.DenyDelete != off.DenyDelete || on.MaxBytes != off.MaxBytes {
		t.Error("schedules changed a setting that is not theirs")
	}
	// The one setting that must move: the server refuses schedules with
	// DiscardNew, so a store of ticks discards its oldest at the cap.
	if off.Discard != jetstream.DiscardNew || on.Discard != jetstream.DiscardOld {
		t.Errorf("discard: off=%v on=%v, want DiscardNew / DiscardOld", off.Discard, on.Discard)
	}
}
