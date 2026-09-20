package contract

// The open half of what the fleet wire once carried whole
// (chronicle-hq/02-DESIGN/11-the-two-forms.md § the line): the kinds a
// placement runs, and the report a node makes about its index slice. A
// subject an open component speaks stays in the open contract even when
// only the managed service listens — in the open form nothing answers the
// report, or the quick start's own supervisor does, and the node tolerates
// either.

// The workload kinds a placement runs — the vocabulary of the
// chronicle-workload binary's --kind, whatever starts it: an operator's
// unit, the quick start, or the managed executor.
const (
	WorkloadKindNode          = "node"
	WorkloadKindIndexSearch   = "index-search"
	WorkloadKindIndexGraph    = "index-graph"
	WorkloadKindIndexSemantic = "index-semantic"
)

// BridgeRoot is the reserved root of the bridge subjects
// (06-scheduler.md § the dispatch surface) — deliberately outside Root,
// because the member baseline grants CHRON.> to every member and
// reporting belongs to service users alone.
const BridgeRoot = "CHRONX"

// FleetBridgeLocalSubject is what a node publishes its index reports to.
// In the managed form the import in the tenant's account JWT maps it to a
// tenant-stamped form in the control account; in the open form whoever
// supervises the tenant's indexers subscribes to it directly, and a
// request nobody answers is a no-responders refusal the node warns about
// and serves through.
const FleetBridgeLocalSubject = BridgeRoot + ".FLEET.REPORT"

// The report actions.
const (
	IndexReportDeclared = "declared"
	IndexReportDeleted  = "deleted"
)

// FleetIndexReport is a node's report: this index was declared or
// deleted. The report is the request; what the listener does with it —
// a placement record in the managed form, an in-process start in the
// quick start — is the listener's.
type FleetIndexReport struct {
	Action string `json:"action"`
	Log    string `json:"log"`
	Index  string `json:"index"`
	Kind   string `json:"kind,omitempty"`
}

// FleetIndexReportAck acknowledges the report.
type FleetIndexReportAck struct {
	Recorded bool `json:"recorded"`
}
