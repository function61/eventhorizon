package eh

import (
	"context"
	"fmt"

	"github.com/function61/eventhorizon/pkg/eheventencryption"
)

// application-visible Snapshot. contrast this with PersistedSnapshot that is transparently
// encrypted/marshaled into the SnapshotStore
type Snapshot struct {
	Cursor  Cursor `json:"Cursor"`
	Data    []byte `json:"Data"` // opaque byte blob, usually but not necessarily JSON
	Context string `json:"Context"`
}

func NewSnapshot(cursor Cursor, data []byte, context string) *Snapshot {
	return &Snapshot{cursor, data, context}
}

func (s *Snapshot) Unencrypted() *PersistedSnapshot {
	return &PersistedSnapshot{
		Cursor:  s.Cursor,
		Context: s.Context,
		RawData: append([]byte{byte(PersistedSnapshotKindUnencrypted)}, s.Data...),
	}
}

func (s *Snapshot) Encrypted(dek []byte) (*PersistedSnapshot, error) {
	ciphertext, err := eheventencryption.Encrypt(s.Data, dek)
	if err != nil {
		return nil, err
	}

	return &PersistedSnapshot{
		Cursor:  s.Cursor,
		Context: s.Context,
		RawData: append([]byte{byte(PersistedSnapshotKindEncrypted)}, ciphertext...),
	}, nil
}

type SnapshotStore interface {
	// NOTE: returns os.ErrNotExist if snapshot is not found (which MUST not be
	//       considered an actual error)
	ReadSnapshot(ctx context.Context, stream StreamName, snapshotContext string) (*PersistedSnapshot, error)
	WriteSnapshot(ctx context.Context, snapshot PersistedSnapshot) error
	// returns os.ErrNotExist if snapshot-to-delete not found
	DeleteSnapshot(ctx context.Context, stream StreamName, snapshotContext string) error
}

type PersistedSnapshotKind uint8

const (
	PersistedSnapshotKindUnencrypted PersistedSnapshotKind = 1 // plaintext
	PersistedSnapshotKindEncrypted   PersistedSnapshotKind = 2 // 16 bytes IV || AES256_CTR(plaintext, dek)
)

func (k PersistedSnapshotKind) String() string {
	switch k {
	case PersistedSnapshotKindUnencrypted:
		return "Unencrypted"
	case PersistedSnapshotKindEncrypted:
		return "Encrypted"
	default:
		panic(fmt.Errorf("unknown PersistedSnapshotKind: %d", k))
	}
}

// likely encrypted version of Snapshot. encryption isn't used only in cases where the
// processor consumes only non-encrypted data.
// JSON marshaling required for client-server comms
type PersistedSnapshot struct {
	Cursor  Cursor `json:"Cursor"`
	RawData []byte `json:"RawData"` // first byte is kind, following bytes documented by kind
	Context string `json:"Context"`
}

func (e *PersistedSnapshot) Kind() PersistedSnapshotKind {
	return PersistedSnapshotKind(e.RawData[0])
}

func (e *PersistedSnapshot) DecryptIfRequired(loadDek func() ([]byte, error)) (*Snapshot, error) {
	switch e.Kind() {
	case PersistedSnapshotKindUnencrypted:
		return NewSnapshot(e.Cursor, e.RawData[1:], e.Context), nil
	case PersistedSnapshotKindEncrypted:
		dek, err := loadDek()
		if err != nil {
			return nil, err
		}

		plaintextSnapshot, err := eheventencryption.Decrypt(e.RawData[1:], dek)
		if err != nil {
			return nil, err
		}

		return NewSnapshot(e.Cursor, plaintextSnapshot, e.Context), nil
	default:
		return nil, fmt.Errorf("unknown PersistedSnapshotKind: %d", e.Kind())
	}
}
