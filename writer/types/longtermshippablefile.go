package types

import (
	"github.com/function61/pyramid/cursor"
)

type LongTermShippableFile struct {
	Block    *cursor.Cursor // offset and server irrelevant
	FilePath string
}
