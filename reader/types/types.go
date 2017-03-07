package types

import (
	"github.com/function61/eventhorizon/cursor"
)

type ReadResultLine struct {
	IsMeta   bool
	PtrAfter string
	Content  string
}

type ReadOptions struct {
	MaxLinesToRead int
	Cursor         *cursor.Cursor
}

func NewReadOptions() *ReadOptions {
	return &ReadOptions{
		MaxLinesToRead: 1000,
	}
}

type ReadResult struct {
	FromOffset string
	Lines      []ReadResultLine
}

func NewReadResult() *ReadResult {
	return &ReadResult{
		Lines: []ReadResultLine{},
	}
}
