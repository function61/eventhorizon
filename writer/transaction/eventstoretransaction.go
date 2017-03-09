package transaction

import (
	"github.com/boltdb/bolt"
	"github.com/function61/pyramid/writer/types"
)

type Write struct {
	Filename string
	Buffer   []byte
	Position int64
}

type EventstoreTransaction struct {
	BoltTx                 *bolt.Tx
	Bolt                   *bolt.DB
	NewChunks              []*types.ChunkSpec
	ShipFiles              []*types.LongTermShippableFile
	FilesToDisengageWalFor []string
	NeedsWALCompaction     []string
	FilesToOpen            []string
	FilesToClose           []string
	WriteOps               []*Write
	AffectedStreams        map[string]string // streamName => cursorSerialized
	NonMetaLinesAdded      int               // only for metrics
}

func NewEventstoreTransaction(bolt *bolt.DB) *EventstoreTransaction {
	return &EventstoreTransaction{
		Bolt:                   bolt,
		NewChunks:              []*types.ChunkSpec{},
		ShipFiles:              []*types.LongTermShippableFile{},
		FilesToDisengageWalFor: []string{},
		NeedsWALCompaction:     []string{},
		FilesToOpen:            []string{},
		FilesToClose:           []string{},
		WriteOps:               []*Write{},
		AffectedStreams:        make(map[string]string),
		NonMetaLinesAdded:      0,
	}
}

func (e *EventstoreTransaction) QueueWrite(filename string, buffer []byte, position int64) {
	write := &Write{
		Filename: filename,
		Buffer:   buffer,
		Position: position,
	}

	e.WriteOps = append(e.WriteOps, write)
}
