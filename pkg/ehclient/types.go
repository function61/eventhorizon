package ehclient

import (
	"context"
)

type DynamoDbOptions struct {
	AccessKeyId     string
	AccessKeySecret string
	RegionId        string
	TableName       string
}

type AppendResult struct {
	Cursor Cursor
}

// interface for reading log entries from a stream
type Reader interface {
	Read(ctx context.Context, lastKnown Cursor) (*ReadResult, error)
}

// interface for appending log entries to a stream
type Writer interface {
	Append(ctx context.Context, stream string, events []string) (*AppendResult, error)
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

// Raw entry from DynamoDB
// - can contain 0-n events. commit all events in a single transaction.
// - might contain a single meta event
// - why most common attribute names shortened? DynamoDB charges for each byte in item attribute names..
// - we have JSON marshalling defined but please consider it DynamoDB internal implementation
type LogEntry struct {
	Stream    string   `json:"s"` // stream + version form the composite key
	Version   int64    `json:"v"`
	MetaEvent *string  `json:"meta_event"` // StreamStarted | ChildStreamCreated created | ...
	Events    []string `json:"e"`
}

type ErrOptimisticLockingFailed struct {
	error
}

// needed for testing from outside of this package
func NewErrOptimisticLockingFailed(err error) *ErrOptimisticLockingFailed {
	return &ErrOptimisticLockingFailed{err}
}
