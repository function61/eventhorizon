package store

import (
	"fmt"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/cursor"
	"io"
	"log"
	"os"
)

type SeekableStore struct {
}

func NewSeekableStore() *SeekableStore {
	if _, err := os.Stat(config.SEEKABLE_STORE_PATH); os.IsNotExist(err) {
		log.Printf("SeekableStore: mkdir %s", config.SEEKABLE_STORE_PATH)

		if err = os.MkdirAll(config.SEEKABLE_STORE_PATH, 0755); err != nil {
			panic(err)
		}
	}

	return &SeekableStore{}
}

func (s *SeekableStore) Save(cursor *cursor.Cursor, reader io.Reader) {
	localFile, openErr := os.OpenFile(s.localPath(cursor), os.O_RDWR|os.O_CREATE, 0755)
	if openErr != nil {
		panic(openErr)
	}

	defer localFile.Close()

	_, err := io.Copy(localFile, reader)
	if err != nil {
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
	return fmt.Sprintf("%s/%s", config.SEEKABLE_STORE_PATH, cursor.ToChunkSafePath())
}
