package eh

import (
	"context"

	"github.com/function61/eventhorizon/pkg/envelopeenc"
)

type LogDataKind uint8

const (
	LogDataKindMeta          LogDataKind = 1 // one single unencrypted meta event in "ehevent" format
	LogDataKindEncryptedData LogDataKind = 2 // 16 bytes IV || AES256_CTR(plaintext, dek). plaintext is multiple "ehevent" lines split by \n character.
)

// "sub" shortened to save space b/c they're expected to get a lot of writes
var (
	SysCredentials      = sysStreamAddToToCreate("credentials") // /_/credentials
	SysPublishSubscribe = sysStreamAddToToCreate("pubsub")      // /_/pubsub
	SysSubscriptions    = sysStreamAddToToCreate("sub")         // /_/sub

	InternalStreamsToCreate = []StreamName{RootName, RootName.Child("_")} // above streams added here as well
)

type AppendResult struct {
	Cursor Cursor
}

// interface for reading log entries from a stream
type Reader interface {
	Read(ctx context.Context, after Cursor) (*ReadResult, error)
}

// interface for writing to an event log
type Writer interface {
	CreateStream(ctx context.Context, stream StreamName, dekEnvelope envelopeenc.Envelope, data *LogData) (*AppendResult, error)
	Append(ctx context.Context, stream StreamName, data LogData) (*AppendResult, error)
	// used for transactional writes
	// returns *ErrOptimisticLockingFailed if stream had writes after you read it
	AppendAfter(ctx context.Context, after Cursor, data LogData) (*AppendResult, error)
}

type ReaderWriter interface {
	Reader
	Writer
}

type ReadResult struct {
	Entries   []LogEntry
	LastEntry Cursor // Entries[last].Version (use only if you handled all entries) or if no entries, the "after" in Read()
	More      bool   // whether there is more data to fetch
}

// Log entry is log data in a specific position in the event stream
type LogEntry struct {
	Cursor Cursor  `json:"Cursor"`
	Data   LogData `json:"Data"`
}

type LogData struct {
	Kind LogDataKind `json:"Kind"`
	Raw  []byte      `json:"Raw"` // usually encrypted data
}

type ErrOptimisticLockingFailed struct {
	error
}

// needed for testing from outside of this package, also for server client
func NewErrOptimisticLockingFailed(err error) *ErrOptimisticLockingFailed {
	return &ErrOptimisticLockingFailed{err}
}

type MqttActivityNotification struct {
	Activity []CursorCompact `json:"a"` // abbreviated to conserve space
}

func sysStreamAddToToCreate(name string) StreamName {
	stream := RootName.Child("_").Child(name)
	InternalStreamsToCreate = append(InternalStreamsToCreate, stream)
	return stream
}
