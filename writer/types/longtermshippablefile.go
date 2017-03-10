package types

import (
	"github.com/function61/pyramid/cursor"
	"os"
)

type LongTermShippableFile struct {
	Block *cursor.Cursor // offset and server irrelevant
	Fd    *os.File
}
