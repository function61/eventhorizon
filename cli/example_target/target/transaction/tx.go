package transaction

// a datatype used to wrap a transaction
// (to avoid circular dependency with main app <=> event handlers)

import (
	"github.com/asdine/storm"
	"github.com/boltdb/bolt"
)

type Tx struct {
	Db *storm.DB
	Tx *bolt.Tx
}
