package contract

import (
	"testing"
	"time"
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
	if got := LogSubjects("orders"); got != "CHRON.orders.>" {
		t.Errorf("LogSubjects = %q", got)
	}
	if got := OpsFilter("orders"); got != "CHRON.orders.OPS.>" {
		t.Errorf("OpsFilter = %q", got)
	}
	subj := OpsSubject("orders", "invoice-42.line-3")
	if subj != "CHRON.orders.OPS.invoice-42.line-3" {
		t.Errorf("OpsSubject = %q", subj)
	}
	if got := ThingFromSubject("orders", subj); got != "invoice-42.line-3" {
		t.Errorf("ThingFromSubject = %q", got)
	}
}

func TestValidateLogName(t *testing.T) {
	for _, ok := range []string{"orders", "my-log", "a1", "x"} {
		if err := ValidateLogName(ok); err != nil {
			t.Errorf("ValidateLogName(%q): unexpected error %v", ok, err)
		}
	}
	for _, bad := range []string{"", "Orders", "my_log", "my.log", "api", "sys", "meta", "a b", "CHRON"} {
		if err := ValidateLogName(bad); err == nil {
			t.Errorf("ValidateLogName(%q): expected refusal", bad)
		}
	}
}

func TestValidateThing(t *testing.T) {
	for _, ok := range []string{"invoice-42", "invoice-42.line-3", "A.b.c_1"} {
		if err := ValidateThing(ok); err != nil {
			t.Errorf("ValidateThing(%q): unexpected error %v", ok, err)
		}
	}
	for _, bad := range []string{"", ".", "a..b", "a b", "a.*", "a.>", "a/b"} {
		if err := ValidateThing(bad); err == nil {
			t.Errorf("ValidateThing(%q): expected refusal", bad)
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
	cfg := LogStreamConfig("my-log", 0, "")
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
	if over := LogStreamConfig("my-log", 42, ""); over.MaxBytes != 42 {
		t.Errorf("per-log override lost: %d", over.MaxBytes)
	}
	// The 0019 declaration reaches the stream: a preserved log refuses
	// rollup writes outright, and everything else in the table stands.
	preserved := LogStreamConfig("my-log", 0, HistoryPreserved)
	if preserved.AllowRollup {
		t.Error("a preserved log's stream must refuse rollup writes (0019)")
	}
	if !preserved.DenyDelete {
		t.Error("DenyDelete stands on a preserved log")
	}
	if explicit := LogStreamConfig("my-log", 0, HistoryCompactable); !explicit.AllowRollup {
		t.Error("an explicitly compactable log keeps AllowRollup")
	}
}

func TestMetaKeys(t *testing.T) {
	if got := MetaLogConfig("orders"); got != "log.orders.config" {
		t.Errorf("MetaLogConfig = %q", got)
	}
	if got := MetaLogType("orders", "comment.add"); got != "log.orders.type.comment.add" {
		t.Errorf("MetaLogType = %q", got)
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
