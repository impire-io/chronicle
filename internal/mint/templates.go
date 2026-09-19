package mint

import (
	"github.com/nats-io/jwt/v2"

	"github.com/impire-io/chronicle/contract"
)

// The fence of design 10 (chronicle-hq/02-DESIGN/10-custody.md § the fence):
// chronicle issues every CONTROL-account user itself, so it stamps one of
// two permission templates into the user JWT. That is the whole access
// boundary around the AUTH bucket — no second account, no sealing layer.

// ControlInstanceTemplate is unrestricted in the account: only
// chronicle-control's own connections carry it.
func ControlInstanceTemplate() jwt.UserPermissionLimits {
	return jwt.UserPermissionLimits{Limits: unlimited()}
}

// unlimited is the NATS limits a template carries unless it says
// otherwise. A zero jwt.Limits means zero subscriptions, zero bytes — the
// opposite of no template — so every template states them.
func unlimited() jwt.Limits {
	return jwt.Limits{NatsLimits: jwt.NatsLimits{Subs: jwt.NoLimit, Data: jwt.NoLimit, Payload: jwt.NoLimit}}
}

// FleetTemplate is everything a workload-service instance, an executor, or
// the operator's CLI needs in the CONTROL account, minus the bucket: its
// key subjects on publish and subscribe, and every JetStream API subject
// that names its stream on publish. Verified against a real server by
// TestFleetTemplateFencesTheBucket: read and write refused, the user's own
// JetStream work untouched.
func FleetTemplate() jwt.UserPermissionLimits {
	deny := authBucketAPISubjects()
	return jwt.UserPermissionLimits{
		Permissions: jwt.Permissions{
			Pub: jwt.Permission{Deny: append(jwt.StringList{authBucketKeySubjects}, deny...)},
			Sub: jwt.Permission{Deny: jwt.StringList{authBucketKeySubjects}},
		},
		Limits: unlimited(),
	}
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
