package reader

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/function61/pyramid/cursor"
	"github.com/function61/pyramid/metaevents"
	"github.com/function61/pyramid/reader/store"
	rtypes "github.com/function61/pyramid/reader/types"
	"github.com/function61/pyramid/scalablestore"
	"github.com/function61/pyramid/writer/writerclient"
	wtypes "github.com/function61/pyramid/writer/writerhttp/types"
	"io"
	"log"
)

type EventstoreReader struct {
	s3manager                *scalablestore.S3Manager
	seekableStore            *store.SeekableStore
	compressedEncryptedStore *store.CompressedEncryptedStore
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
func (e *EventstoreReader) Read(opts *rtypes.ReadOptions) (*rtypes.ReadResult, error) {
	/*	Read from S3 as long as we're not encountering EOF.

		If we encounter EOF and chunk is not closed, move to reading from advertised server.
	*/

	// log.Printf("EventstoreReader: starting read from %s", opts.Cursor.Serialize())

	if !e.seekableStore.Has(opts.Cursor) { // copy from compressed&encrypted store
		log.Printf("EventstoreReader: %s miss from SeekableStore", opts.Cursor.Serialize())

		// FIXME: this being here is a goddamn hack
		if opts.Cursor.Server != "" {
			log.Printf("EventstoreReader: contacting LiveReader for %s", opts.Cursor.Serialize())

			wclient := writerclient.NewClient()

			result, was404, err := wclient.LiveRead(&wtypes.LiveReadInput{
				Cursor: opts.Cursor.Serialize(),
			})

			if err == nil { // got result from LiveReader
				// no need to seek, as the result from LiveReader is already based on offset
				// so is the line read limit but there is no harm in parseFromReader()
				// implementing the limit again
				return parseFromReader(result, opts)
			}

			if !was404 && err != nil { // unexpected error
				panic(err)
			}

			// ok it was 404 => carry on trying from S3
		}

		if !e.compressedEncryptedStore.Has(opts.Cursor) { // copy from S3
			log.Printf("EventstoreReader: %s miss from CompressedEncryptedStore", opts.Cursor.Serialize())

			if !e.compressedEncryptedStore.DownloadFromS3(opts.Cursor, e.s3manager) {
				log.Printf("EventstoreReader: %s miss from S3", opts.Cursor.Serialize())

				// TODO: try this from the server pointed to in the cursor
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

	// happens after parseFromReader() returns
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

	return parseFromReader(fd, opts)
}

func parseFromReader(reader io.Reader, opts *rtypes.ReadOptions) (*rtypes.ReadResult, error) {
	scanner := bufio.NewScanner(reader)

	readResult := rtypes.NewReadResult()
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

		// as a convenience, parse & unpack SubscriptionActivity events' stream
		// activity in the resulting data structure, as not to *require* Targets
		// to have the capability to parse meta events
		activityUnpacked := []string{}

		if isMetaEvent {
			rotated, isRotated := event.(metaevents.Rotated)
			subscriptionActivity, isSubscriptionActivity := event.(metaevents.SubscriptionActivity)

			if isRotated {
				newCursor = cursor.CursorFromserializedMust(rotated.Next)
			} else if isSubscriptionActivity {
				activityUnpacked = append(activityUnpacked, subscriptionActivity.Activity...)
			}
		}

		readResultLine := rtypes.ReadResultLine{
			IsMeta:               isMetaEvent,
			PtrAfter:             newCursor.Serialize(),
			Content:              parsedLine,
			SubscriptionActivity: activityUnpacked,
		}

		readResult.Lines = append(readResult.Lines, readResultLine)

		previousCursor = newCursor
	}

	return readResult, nil
}
