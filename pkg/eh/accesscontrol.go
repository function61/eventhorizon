package eh

// Action IDs used in policies to enforce access to streams and snapshots in EventHorizon

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
