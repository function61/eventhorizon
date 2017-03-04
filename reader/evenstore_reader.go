package reader

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/metaevents"
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
	IsMeta bool
	// PtrAfter *cursor.Cursor
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
func (e *EventstoreReader) Read(opts *ReadOptions) (*ReadResult, error) {
	/*	Read from S3 as long as we're not encountering EOF.

		If we encounter EOF and chunk is not closed, move to reading from advertised server.
	*/
	// log.Printf("EventstoreReader: starting read from %s", opts.Cursor.Serialize())

	if !e.seekableStore.Has(opts.Cursor) { // copy from compressed&encrypted store
		log.Printf("EventstoreReader: %s miss from SeekableStore", opts.Cursor.Serialize())

		if !e.compressedEncryptedStore.Has(opts.Cursor) { // copy from S3
			log.Printf("EventstoreReader: %s miss from CompressedEncryptedStore", opts.Cursor.Serialize())

			if !e.compressedEncryptedStore.DownloadFromS3(opts.Cursor, e.s3manager) {
				log.Printf("EventstoreReader: %s miss from S3", opts.Cursor.Serialize())

				return nil, errors.New("Did not find from S3")
			}
		}

		// the file is now at CompressedEncryptedStore, but not in SeekableStore
		e.compressedEncryptedStore.ExtractToSeekableStore(opts.Cursor, e.seekableStore)
	}

	// TODO: open fd cache
	fd, err := e.seekableStore.Open(opts.Cursor)
	if err != nil {
		return nil, err
	}

	defer fd.Close()

	fileInfo, errStat := fd.Stat()
	if errStat != nil {
		return nil, errStat
	}

	if int64(opts.Cursor.Offset) > fileInfo.Size() {
		return nil, errors.New(fmt.Sprintf("Attempt to seek past EOF"))
	}

	_, errSeek := fd.Seek(int64(opts.Cursor.Offset), io.SeekStart)
	if errSeek != nil {
		panic(errSeek)
	}

	scanner := bufio.NewScanner(fd)

	readResult := NewReadResult()
	readResult.FromOffset = opts.Cursor.Serialize()

	previousCursor := opts.Cursor

	for linesRead := 0; linesRead < opts.MaxLinesToRead && scanner.Scan(); linesRead++ {
		rawLine := scanner.Text()
		rawLineLen := len(rawLine) + 1 // +1 for newline that we just right-trimmed

		newCursor := cursor.New(
			previousCursor.Stream,
			previousCursor.Chunk,
			previousCursor.Offset+rawLineLen,
			previousCursor.Server)

		isMetaEvent, parsedLine, event := metaevents.Parse(rawLine)

		if isMetaEvent {
			rotated, isRotated := event.(metaevents.Rotated)

			if isRotated {
				newCursor = cursor.CursorFromserializedMust(rotated.Next)
			}
		}

		readResultLine := ReadResultLine{
			IsMeta:   isMetaEvent,
			PtrAfter: newCursor.Serialize(),
			Content:  parsedLine,
		}

		readResult.Lines = append(readResult.Lines, readResultLine)

		previousCursor = newCursor
	}

	return readResult, nil
}
