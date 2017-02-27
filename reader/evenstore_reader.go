package reader

import (
	"bufio"
	"errors"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/reader/store"
	"github.com/function61/eventhorizon/scalablestore"
	"io"
	"log"
)

type EventstoreReader struct {
	s3manager                *scalablestore.S3Manager
	seekableStore            *store.SeekableStore
	compressedEncryptedStore *store.CompressedEncryptedStore
}

type ReadResultLine struct {
	IsMeta   bool
	PtrAfter cursor.Cursor
	Content  string
}

type ReadResult struct {
	Lines []ReadResultLine
}

func NewEventstoreReader() *EventstoreReader {
	seekableStore := store.NewSeekableStore()
	compressedEncryptedStore := store.NewCompressedEncryptedStore()
	s3manager := scalablestore.NewS3Manager()

	return &EventstoreReader{
		s3manager:                s3manager,
		seekableStore:            seekableStore,
		compressedEncryptedStore: compressedEncryptedStore,
	}
}

/*
	Download chunk from store:S3 -> store:compressed&encrypted
		(only if required)
	Extract from store:compressed&encrypted -> store:seekable
		(only if required)
	Read from store:seekable
*/
func (e *EventstoreReader) Read(cursor *cursor.Cursor) error {
	/*	Read from S3 as long as we're not encountering EOF.

		If we encounter EOF and chunk is not closed, move to reading from advertised server.
	*/
	// log.Printf("EventstoreReader: starting read from %s", cursor.Serialize())

	if !e.seekableStore.Has(cursor) { // copy from compressed&encrypted store
		log.Printf("EventstoreReader: miss from SeekableStore")

		if !e.compressedEncryptedStore.Has(cursor) { // copy from S3
			log.Printf("EventstoreReader: miss from CompressedEncryptedStore")

			if !e.compressedEncryptedStore.DownloadFromS3(cursor, e.s3manager) {
				log.Printf("EventstoreReader: miss from S3")

				return errors.New("Did not find from S3")
			}
		}

		// the file is now at CompressedEncryptedStore, but not in SeekableStore
		e.compressedEncryptedStore.ExtractToSeekableStore(cursor, e.seekableStore)
	}

	fd, err := e.seekableStore.Open(cursor)
	if err != nil {
		return err
	}

	defer fd.Close()

	_, errSeek := fd.Seek(int64(cursor.Offset), io.SeekStart)
	if errSeek != nil {
		panic(errSeek)
	}

	scanner := bufio.NewScanner(fd)

	maxLinesToRead := 5

	for linesRead := 0; linesRead < maxLinesToRead && scanner.Scan(); linesRead++ {
		line := scanner.Text()

		log.Printf("EventstoreReader: <%s>", line)
	}

	// log.Printf("EventstoreReader: got %s", cursor.Serialize())

	return nil
}
