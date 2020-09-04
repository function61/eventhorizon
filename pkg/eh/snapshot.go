package eh

import (
	"context"
)

type Snapshot struct {
	Cursor  Cursor `json:"Cursor"`
	Data    []byte `json:"Data"` // opaque byte blob, usually but not necessarily JSON
	Context string `json:"Context"`
}

func NewSnapshot(cursor Cursor, data []byte, context string) *Snapshot {
	return &Snapshot{cursor, data, context}
}

type SnapshotStore interface {
	// NOTE: returns os.ErrNotExist if snapshot is not found (which MUST not be
	//       considered an actual error)
	ReadSnapshot(ctx context.Context, stream StreamName, snapshotContext string) (*Snapshot, error)
	WriteSnapshot(ctx context.Context, snapshot Snapshot) error
	// returns os.ErrNotExist if snapshot-to-delete not found
	DeleteSnapshot(ctx context.Context, stream StreamName, snapshotContext string) error
}
