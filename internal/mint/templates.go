package mint

import (
	"fmt"

	"github.com/nats-io/jwt/v2"

	"github.com/impire-io/chronicle/contract"
)

// The fence of design 10 (chronicle-hq/02-DESIGN/10-custody.md § the fence,
// decision 0032): chronicle issues every user of the SYS and CONTROL
// accounts itself, so it stamps one of four permission templates into the
// user JWT, one per role, each an allow-list of what the role reaches.
// That is the whole access boundary — no second account, no sealing layer.

// Template names one of the four roles — the value `operator instance add
// --template` takes.
type Template string

// The four roles.
const (
	TemplateControlInstance Template = "control-instance"
	TemplateExecutor        Template = "executor"
	TemplateWorkloads       Template = "workloads"
	TemplateCLI             Template = "cli"
)

// Templates lists the roles, in the order the CLI names them.
var Templates = []Template{TemplateControlInstance, TemplateExecutor, TemplateWorkloads, TemplateCLI}

// ParseTemplate reads a role name; anything else is refused by name.
func ParseTemplate(s string) (Template, error) {
	for _, t := range Templates {
		if Template(s) == t {
			return t, nil
		}
	}
	return "", fmt.Errorf("template %q is not one of %v", s, Templates)
}

// Limits is the permission template a user under this role carries. The
// executor's is bound to the instance's name — which is the executor's ID.
func (t Template) Limits(name string) jwt.UserPermissionLimits {
	switch t {
	case TemplateExecutor:
		return ExecutorTemplate(name)
	case TemplateWorkloads:
		return WorkloadsTemplate()
	case TemplateCLI:
		return CLITemplate()
	}
	return ControlInstanceTemplate()
}

// inbox is where replies travel: every role publishes replies to the
// requester's inbox and reads its own.
const inbox = "_INBOX.>"

// microSurface is the discovery and management surface every micro
// service subscribes to (how-we-build § every component is a micro
// service).
const microSurface = "$SRV.>"

// ControlInstanceTemplate is unrestricted in the account: only
// chronicle-control's own connections carry it.
func ControlInstanceTemplate() jwt.UserPermissionLimits {
	return jwt.UserPermissionLimits{Limits: unlimited()}
}

// ExecutorTemplate is one executor's credential, bound to its name: it
// registers, reports, and pulls creds on its own subjects, hears the
// auction, serves its own endpoints — and nothing else. No JetStream API,
// no bucket, no verb, no other executor's subjects: a compromised host is
// that host's placements and attributed requests about itself
// (06-scheduler.md § the executors).
func ExecutorTemplate(id string) jwt.UserPermissionLimits {
	return jwt.UserPermissionLimits{
		Permissions: jwt.Permissions{
			Pub: jwt.Permission{Allow: jwt.StringList{
				contract.FleetRegisterSubject(id),
				contract.FleetReportSubject(id),
				contract.FleetCredsSubject(id),
				inbox,
			}},
			Sub: jwt.Permission{Allow: jwt.StringList{
				contract.FleetAuctionSubject,
				contract.FleetDelegateSubject(id),
				contract.FleetStatusSubject(id),
				contract.FleetDestroySubject(id),
				microSurface,
				inbox,
			}},
		},
		Limits: unlimited(),
	}
}

// WorkloadsTemplate is the workload service's credential: the fleet log
// and its buckets through the JetStream API and their subjects, its own
// endpoints — the dispatch surface and the node's index report arriving
// over the bridge export — every executor's endpoints, the auction; never
// the AUTH bucket, never a tenant or member verb, never a creds pull.
func WorkloadsTemplate() jwt.UserPermissionLimits {
	return jwt.UserPermissionLimits{
		Permissions: jwt.Permissions{
			Pub: jwt.Permission{
				Allow: jwt.StringList{
					contract.LogSubjects(contract.FleetLog),
					contract.FleetAuctionSubject,
					contract.FleetDelegateSubject("*"),
					contract.FleetStatusSubject("*"),
					contract.FleetDestroySubject("*"),
					"$JS.API.>",
					"$KV." + contract.MetaBucket + ".>",
					"$KV." + contract.StateBucket(contract.FleetLog) + ".>",
					inbox,
				},
				Deny: append(jwt.StringList{authBucketKeySubjects}, authBucketAPISubjects()...),
			},
			Sub: jwt.Permission{
				Allow: jwt.StringList{
					contract.FleetDispatchSubject,
					contract.FleetStopSubject,
					contract.FleetRegisterSubject("*"),
					contract.FleetReportSubject("*"),
					contract.FleetBridgeExport,
					microSurface,
					inbox,
				},
				Deny: jwt.StringList{authBucketKeySubjects},
			},
		},
		Limits: unlimited(),
	}
}

// CLITemplate is the operator's CLI and the panel's backend: the tenant
// and member verbs, and nothing on JetStream.
func CLITemplate() jwt.UserPermissionLimits {
	return jwt.UserPermissionLimits{
		Permissions: jwt.Permissions{
			Pub: jwt.Permission{Allow: jwt.StringList{"CHRON.CTRL.TENANT.>", "CHRON.CTRL.MEMBER.>", inbox}},
			Sub: jwt.Permission{Allow: jwt.StringList{inbox}},
		},
		Limits: unlimited(),
	}
}

// unlimited is the NATS limits a template carries unless it says
// otherwise. A zero jwt.Limits means zero subscriptions, zero bytes — the
// opposite of no template — so every template states them.
func unlimited() jwt.Limits {
	return jwt.Limits{NatsLimits: jwt.NatsLimits{Subs: jwt.NoLimit, Data: jwt.NoLimit, Payload: jwt.NoLimit}}
}

// authBucketKeySubjects is where the bucket's values travel.
const authBucketKeySubjects = "$KV." + contract.AuthBucket + ".>"

// authBucketStream is the bucket's stream.
const authBucketStream = "KV_" + contract.AuthBucket

// authBucketAPISubjects is every JetStream API subject that names the
// bucket's stream: stream verbs (info, update, delete, purge, snapshot,
// restore, create), message gets and deletes, direct gets, and the
// consumer verbs — create, durable create, info, delete, next, list, names
// — through which a watcher or a history read would otherwise reach the
// values. Spelled out rather than wildcarded because a wildcard wide
// enough to catch CONSUMER.DURABLE.CREATE would catch every stream.
func authBucketAPISubjects() jwt.StringList {
	s := authBucketStream
	return jwt.StringList{
		"$JS.API.STREAM.*." + s,
		"$JS.API.STREAM.*." + s + ".>",
		"$JS.API.STREAM.MSG.*." + s,
		"$JS.API.DIRECT.GET." + s,
		"$JS.API.DIRECT.GET." + s + ".>",
		"$JS.API.CONSUMER.CREATE." + s,
		"$JS.API.CONSUMER.CREATE." + s + ".>",
		"$JS.API.CONSUMER.DURABLE.CREATE." + s + ".>",
		"$JS.API.CONSUMER.*." + s + ".>",
		"$JS.API.CONSUMER.MSG.NEXT." + s + ".>",
		"$JS.API.CONSUMER.LIST." + s,
		"$JS.API.CONSUMER.NAMES." + s,
	}
}
