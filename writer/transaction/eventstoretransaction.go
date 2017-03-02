package transaction

import (
	"github.com/boltdb/bolt"
)

type EventstoreTransaction struct {
	BoltTx    *bolt.Tx
	NewChunks []*ChunkSpec
	// WALs to compact (per file)
	// WriteOps (per file)
}

func NewEventstoreTransaction() *EventstoreTransaction {
	return &EventstoreTransaction{
		NewChunks: []*ChunkSpec{},
	}
}
