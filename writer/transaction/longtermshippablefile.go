package transaction

import (
	"os"
)

type LongTermShippableFile struct {
	ChunkName string // '/tenants/foo/_/28.log'
	Fd        *os.File
}
