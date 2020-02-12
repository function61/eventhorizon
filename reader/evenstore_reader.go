package reader

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/metaevents"
	"github.com/function61/eventhorizon/reader/store"
	rtypes "github.com/function61/eventhorizon/reader/types"
	"github.com/function61/eventhorizon/scalablestore"
	wtypes "github.com/function61/eventhorizon/writer/types"
	"github.com/function61/eventhorizon/writer/writerclient"
	"io"
	"log"
)

type EventstoreReader struct {
	s3manager                *scalablestore.S3Manager
	seekableStore            *store.SeekableStore
	compressedEncryptedStore *store.CompressedEncryptedStore
	writerClient             *writerclient.Client
	confCtx                  *config.Context
}

func New(confCtx *config.Context, writerClient *writerclient.Client) *EventstoreReader {
	seekableStore := store.NewSeekableStore()
	compressedEncryptedStore := store.NewCompressedEncryptedStore(confCtx)
	s3manager := scalablestore.NewS3Manager(confCtx)

	return &EventstoreReader{
		s3manager:                s3manager,
		seekableStore:            seekableStore,
		compressedEncryptedStore: compressedEncryptedStore,
		writerClient:             writerClient,
		confCtx:                  confCtx,
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
	cur := opts.Cursor

	// FIXME: this assumes we're only running one server
	// (this could be resolved from S3)
	if cur.Server == cursor.UnknownServer {
		// replace cursor with one pointing to writer
		cur = cursor.New(cur.Stream, cur.Chunk, cur.Offset, e.confCtx.GetWriterIp())
	}

	/*	Read from S3 as long as we're not encountering EOF.

		If we encounter EOF and chunk is not closed, move to reading from advertised server.
	*/

	// log.Printf("EventstoreReader: starting read from %s", cur.Serialize())

	if !e.seekableStore.Has(cur) { // copy from compressed&encrypted store
		log.Printf("EventstoreReader: %s miss from SeekableStore", cur.Serialize())

		// FIXME: this being here is a goddamn hack
		if cur.Server != "" {
			log.Printf("EventstoreReader: contacting LiveReader for %s", cur.Serialize())

			result, was404, err := e.writerClient.LiveRead(&wtypes.LiveReadInput{
				Cursor:         cur.Serialize(),
				MaxLinesToRead: opts.MaxLinesToRead,
			})

			if err == nil { // got result from LiveReader
				// no need to seek, as the result from LiveReader is already based on offset
				// so is the line read limit but there is no harm in parseFromReader()
				// implementing the limit again
				return parseFromReader(result, cur, opts)
			}

			if !was404 && err != nil { // unexpected error
				panic(err)
			}

			// ok it was 404 => carry on trying from S3
		}

		if !e.compressedEncryptedStore.Has(cur) { // copy from S3
			log.Printf("EventstoreReader: %s miss from CompressedEncryptedStore", cur.Serialize())

			if !e.compressedEncryptedStore.DownloadFromS3(cur, e.s3manager) {
				log.Printf("EventstoreReader: %s miss from S3", cur.Serialize())

				// TODO: try this from the server pointed to in the cursor
				return nil, errors.New("Did not find from S3")
			}
		}

		// the file is now at CompressedEncryptedStore, but not in SeekableStore
		e.compressedEncryptedStore.ExtractToSeekableStore(cur, e.seekableStore)
	}

	// TODO: open fd cache
	fd, err := e.seekableStore.Open(cur)
	if err != nil {
		return nil, err
	}

	// happens after parseFromReader() returns
	defer fd.Close()

	fileInfo, errStat := fd.Stat()
	if errStat != nil {
		return nil, errStat
	}

	if int64(cur.Offset) > fileInfo.Size() {
		return nil, fmt.Errorf("Attempt to seek past EOF")
	}

	_, errSeek := fd.Seek(int64(cur.Offset), io.SeekStart)
	if errSeek != nil {
		panic(errSeek)
	}

	return parseFromReader(fd, cur, opts)
}

func parseFromReader(reader io.Reader, cur *cursor.Cursor, opts *rtypes.ReadOptions) (*rtypes.ReadResult, error) {
	scanner := bufio.NewScanner(reader)

	readResult := rtypes.NewReadResult()
	readResult.FromOffset = cur.Serialize()

	previousCursor := cur

	for linesRead := 0; linesRead < opts.MaxLinesToRead && scanner.Scan(); linesRead++ {
		rawLine := scanner.Text()
		rawLineLen := len(rawLine) + 1 // +1 for newline that we just right-trimmed

		newCursor := cursor.New(
			previousCursor.Stream,
			previousCursor.Chunk,
			previousCursor.Offset+rawLineLen,
			previousCursor.Server)

		metaType, parsedLine, event := metaevents.Parse(rawLine)

		var metaPayload interface{} = nil

		if metaType != "" {
			if metaType == metaevents.RotatedId {
				rotated := event.(metaevents.Rotated)
				newCursor = cursor.CursorFromserializedMust(rotated.Next)
			}

			if err := json.Unmarshal([]byte(parsedLine), &metaPayload); err != nil {
				panic(err)
			}
		}

		readResultLine := rtypes.ReadResultLine{
			PtrAfter:    newCursor.Serialize(),
			Content:     parsedLine,
			MetaType:    metaType,
			MetaPayload: metaPayload,
		}

		readResult.Lines = append(readResult.Lines, readResultLine)

		previousCursor = newCursor
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return readResult, nil
}
