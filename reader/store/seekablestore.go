package store

import (
	"fmt"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/cursor"
	"log"
	"os"
)

type SeekableStore struct {
}

func NewSeekableStore() *SeekableStore {
	if _, err := os.Stat(config.SeekableStorePath); os.IsNotExist(err) {
		log.Printf("SeekableStore: mkdir %s", config.SeekableStorePath)

		if err = os.MkdirAll(config.SeekableStorePath, 0755); err != nil {
			panic(err)
		}
	}

	return &SeekableStore{}
}

// store a file in SeekableStore by renaming a file here from a temporary location,
// so we can do this in an atomic way (= the second Has() reports true we have 100 % complete file)
func (s *SeekableStore) SaveByRenaming(cursor *cursor.Cursor, fromPath string) {
	if err := os.Rename(fromPath, s.localPath(cursor)); err != nil {
		panic(err)
	}
}

func (s *SeekableStore) Open(cursor *cursor.Cursor) (*os.File, error) {
	localFile, openErr := os.Open(s.localPath(cursor))
	if openErr != nil {
		panic(openErr)
	}

	return localFile, nil
}

func (s *SeekableStore) Has(cursor *cursor.Cursor) bool {
	filePath := s.localPath(cursor)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false
	}

	return true
}

func (s *SeekableStore) localPath(cursor *cursor.Cursor) string {
	return fmt.Sprintf("%s/%s", config.SeekableStorePath, cursor.ToChunkSafePath())
}
