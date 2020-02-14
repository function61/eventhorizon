package ehclient

import (
	"context"
)

// interface for reading log entries from a stream
type Reader interface {
	Read(ctx context.Context, lastKnown Cursor) (*ReadResult, error)
}

// interface for appending log entries to a stream
type Writer interface {
	Append(ctx context.Context, stream string, events []string) error
	// TODO: rename -> AppendAfter()
	AppendAt(ctx context.Context, after Cursor, events []string) error
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

type concurrencyError struct {
	error
}
