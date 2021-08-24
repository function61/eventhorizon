package eh

import (
	"context"
	"fmt"
	"strings"

	"github.com/function61/eventhorizon/pkg/eheventencryption"
)

// application-visible Snapshot. contrast this with PersistedSnapshot that is transparently
// encrypted/marshaled into the SnapshotStore
type Snapshot struct {
	Cursor      Cursor              `json:"Cursor"`
	Data        []byte              `json:"Data"` // opaque byte blob, usually but not necessarily JSON
	Perspective SnapshotPerspective `json:"Perspective"`
}

func NewSnapshot(cursor Cursor, data []byte, perspective SnapshotPerspective) *Snapshot {
	return &Snapshot{cursor, data, perspective}
}

func (s *Snapshot) Unencrypted() *PersistedSnapshot {
	return &PersistedSnapshot{
		Cursor:      s.Cursor,
		Perspective: s.Perspective,
		RawData:     append([]byte{byte(PersistedSnapshotKindUnencrypted)}, s.Data...),
	}
}

func (s *Snapshot) Encrypted(dek []byte) (*PersistedSnapshot, error) {
	ciphertext, err := eheventencryption.Encrypt(s.Data, dek)
	if err != nil {
		return nil, err
	}

	return &PersistedSnapshot{
		Cursor:      s.Cursor,
		Perspective: s.Perspective,
		RawData:     append([]byte{byte(PersistedSnapshotKindEncrypted)}, ciphertext...),
	}, nil
}

type ReadSnapshotInput struct {
	Stream          StreamName
	Perspective     SnapshotPerspective
	PreferEagerRead bool // requests EagerRead to be filled to optimize performance (contains events occurred after the snapshot)
}

type ReadSnapshotOutput struct {
	Snapshot  *PersistedSnapshot
	EagerRead *ReadResult // might be filled if PreferEagerRead=true requested
}

type SnapshotStore interface {
	// NOTE: returns os.ErrNotExist if snapshot is not found (which MUST not be
	//       considered an actual error)
	ReadSnapshot(ctx context.Context, input ReadSnapshotInput) (*ReadSnapshotOutput, error)
	WriteSnapshot(ctx context.Context, snapshot PersistedSnapshot) error
	// returns os.ErrNotExist if snapshot-to-delete not found
	DeleteSnapshot(ctx context.Context, stream StreamName, perspective SnapshotPerspective) error
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
	Cursor      Cursor              `json:"Cursor"`
	RawData     []byte              `json:"RawData"` // first byte is kind, following bytes documented by kind
	Perspective SnapshotPerspective `json:"Perspective"`
}

func (e *PersistedSnapshot) Kind() PersistedSnapshotKind {
	return PersistedSnapshotKind(e.RawData[0])
}

func (e *PersistedSnapshot) DecryptIfRequired(loadDEKv0 func() ([]byte, error)) (*Snapshot, error) {
	switch e.Kind() {
	case PersistedSnapshotKindUnencrypted:
		return NewSnapshot(e.Cursor, e.RawData[1:], e.Perspective), nil
	case PersistedSnapshotKindEncrypted:
		dek, err := loadDEKv0()
		if err != nil {
			return nil, err
		}

		plaintextSnapshot, err := eheventencryption.Decrypt(e.RawData[1:], dek)
		if err != nil {
			return nil, err
		}

		return NewSnapshot(e.Cursor, plaintextSnapshot, e.Perspective), nil
	default:
		return nil, fmt.Errorf("unknown PersistedSnapshotKind: %d", e.Kind())
	}
}

// processor ("projection") reads streams and has a certain perspective in mind when looking at the events.
// since a stream can have multiple processors looking at it, it is likely that they have different
// perspectives and thus have different snapshot shapes. we separate them in snapshot storage by this perspective ID.
//
// tl;dr: when you make changes to your snapshot's "schema" that are incompatible with the old schema,
//        you'll want to increase the version number.
type SnapshotPerspective struct {
	AppID   string
	Version string
}

// parsed format returned by String()
func ParseSnapshotPerspective(serialized string) SnapshotPerspective {
	parts := strings.Split(serialized, ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		panic(fmt.Errorf("ParseSnapshotPerspective: failed to parse: %s", serialized))
	}

	return NewPerspective(parts[0], parts[1])
}

// looks like "myapp:v1"
func (p SnapshotPerspective) String() string {
	return fmt.Sprintf("%s:%s", p.AppID, p.Version)
}

// a way for processor to declare that it doesn't use snapshots
func (p SnapshotPerspective) IsEmpty() bool {
	return p.AppID == "" && p.Version == ""
}

// helper for the most common use case - v1 (or first version) of a processor's perspective.
func NewV1Perspective(appID string) SnapshotPerspective {
	return NewPerspective(appID, "v1")
}

func NewPerspective(appID string, version string) SnapshotPerspective {
	return SnapshotPerspective{appID, version}
}
