package eh

import (
	"context"

	"github.com/function61/eventhorizon/pkg/policy"
)

var (
	ResourceNameStream   = policy.F61rn.Child("eventhorizon").Child("stream")   // f61rn:eventhorizon:stream
	ResourceNameSnapshot = policy.F61rn.Child("eventhorizon").Child("snapshot") // f61rn:eventhorizon:snapshot
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
	CreateStream(ctx context.Context, stream StreamName, initialEvents []string) (*AppendResult, error)
	Append(ctx context.Context, stream StreamName, events []string) (*AppendResult, error)
	// used for transactional writes
	// returns *ErrOptimisticLockingFailed if stream had writes after you read it
	AppendAfter(ctx context.Context, after Cursor, events []string) (*AppendResult, error)
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

type LogEntry struct {
	Version   Cursor   `json:"Version"` // TODO: rename?
	MetaEvent *string  `json:"MetaEvent,omitempty"`
	Events    []string `json:"Events"`
}

type ErrOptimisticLockingFailed struct {
	error
}

// needed for testing from outside of this package
func NewErrOptimisticLockingFailed(err error) *ErrOptimisticLockingFailed {
	return &ErrOptimisticLockingFailed{err}
}

func sysStreamAddToToCreate(name string) StreamName {
	stream := RootName.Child("_").Child(name)
	InternalStreamsToCreate = append(InternalStreamsToCreate, stream)
	return stream
}
