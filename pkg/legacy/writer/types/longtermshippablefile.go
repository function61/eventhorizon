package types

import (
	"github.com/function61/eventhorizon/pkg/legacy/cursor"
)

type LongTermShippableFile struct {
	Block    *cursor.Cursor // offset and server irrelevant
	FilePath string
}
