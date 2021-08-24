package eh

// Constants used in policies to enforce access to streams and snapshots

import (
	"github.com/function61/eventhorizon/pkg/policy"
)

var (
	ActionStreamCreate   = policy.NewAction("eventhorizon:stream:Create")
	ActionStreamRead     = policy.NewAction("eventhorizon:stream:Read")
	ActionStreamAppend   = policy.NewAction("eventhorizon:stream:Append")
	ActionSnapshotRead   = policy.NewAction("eventhorizon:snapshot:Read")
	ActionSnapshotWrite  = policy.NewAction("eventhorizon:snapshot:Write")
	ActionSnapshotDelete = policy.NewAction("eventhorizon:snapshot:Delete")
)

// resource prefixes for which actions will be authorized against
var (
	ResourceNameStream   = policy.F61.Child("eventhorizon").Child("stream")   // f61:eventhorizon:stream
	ResourceNameSnapshot = policy.F61.Child("eventhorizon").Child("snapshot") // f61:eventhorizon:snapshot
	resourceNameDEK      = policy.F61.Child("eventhorizon").Child("dek")      // f61:eventhorizon:dek
)
