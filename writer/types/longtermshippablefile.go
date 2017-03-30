package types

import (
	"github.com/function61/eventhorizon/cursor"
)

type LongTermShippableFile struct {
	Block    *cursor.Cursor // offset and server irrelevant
	FilePath string
}
