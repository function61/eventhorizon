package writer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/metaevents"
	"github.com/function61/eventhorizon/writer/wal"
	"log"
	"os"
	"strings"
)

type ChunkSpec struct {
	// TODO: calculate this
	ChunkPath string `json:"chunk_path"`

	StreamName  string `json:"stream_name"`
	ChunkNumber int    `json:"chunk_number"`
}

type EventstoreWriter struct {
	walManager          *wal.WalManager
	ip                  string
	database            *bolt.DB
	streamToChunkName   map[string]*ChunkSpec
	longTermShipperWork chan *LongTermShippableFile
	longTermShipperDone chan bool
}

func NewEventstoreWriter() *EventstoreWriter {
	e := &EventstoreWriter{
		ip:                  "127.0.0.1",
		streamToChunkName:   make(map[string]*ChunkSpec),
		longTermShipperWork: make(chan *LongTermShippableFile),
		longTermShipperDone: make(chan bool),
	}

	e.makeBoltDbDirIfNotExist()

	// DB will be created if not exists

	dbLocation := config.BOLTDB_DIR + "/evenstore-wal.boltdb"

	log.Printf("EventstoreWriter: opening DB %s", dbLocation)

	/*	Bolt buckets:

		_streams:
			stream_name
	*/
	db, err := bolt.Open(dbLocation, 0600, nil)
	if err != nil {
		log.Fatal("EventstoreWriter: bolt open failed: ", err)
	}

	e.walManager = wal.NewWalManager(db)
	e.database = db

	e.scanOpenStreams()

	go LongTermShipperManager(e.longTermShipperWork, e.longTermShipperDone)

	return e
}

func (e *EventstoreWriter) CreateStream(streamName string) {
	log.Printf("EventstoreWriter: CreateStream: %s", streamName)

	// /tenants/foo/_/0.log
	chunkName := cursor.NewWithoutServer(streamName, 0, 0).ToChunkPath()

	e.openChunkLocallyAndUploadToS3(chunkName, 0, streamName)
}

func (e *EventstoreWriter) AppendToStream(streamName string, contentArr []string) error {
	if len(contentArr) == 0 {
		return nil // not an error to call with empty append
	}

	chunkSpec, streamExists := e.streamToChunkName[streamName]
	if !streamExists {
		return errors.New("EventstoreWriter.AppendToStream: stream does not exist")
	}

	for _, c := range contentArr {
		if strings.Contains(c, "\n") {
			return errors.New("EventstoreWriter.AppendToStream: content cannot contain \n")
		}
	}

	contentWithNewline := strings.Join(contentArr, "\n") + "\n"

	nextOffset, err := e.walManager.AppendToFile(chunkSpec.ChunkPath, contentWithNewline)
	if err != nil {
		panic(err)
	}

	if nextOffset > config.CHUNK_ROTATE_THRESHOLD {
		log.Printf("EventstoreWriter: AppendToStream: starting rotate, %d threshold exceeded: %s", config.CHUNK_ROTATE_THRESHOLD, streamName)

		e.rotateStreamChunk(streamName)
	}

	return nil
}

func (e *EventstoreWriter) rotateStreamChunk(streamName string) {
	currentChunkSpec, ok := e.streamToChunkName[streamName]
	if !ok {
		panic(errors.New("Stream to chunk not found")) // should not happen
	}

	nextChunkNumber := e.streamToChunkName[streamName].ChunkNumber + 1

	nextChunkName := cursor.NewWithoutServer(streamName, nextChunkNumber, 0).ToChunkPath()

	log.Printf("EventstoreWriter: rotateStreamChunk: %s -> %s", currentChunkSpec.ChunkPath, nextChunkName)

	// this will never be written to again
	err, fd := e.walManager.SealActiveFile(currentChunkSpec.ChunkPath)
	if err != nil {
		panic(err)
	}

	// longTermShipper has responsibility of closing the file
	e.longTermShipperWork <- &LongTermShippableFile{
		chunkName: currentChunkSpec.ChunkPath,
		fd:        fd,
	}

	e.openChunkLocallyAndUploadToS3(nextChunkName, nextChunkNumber, streamName)
}

// TODO: subscriptions array
func (e *EventstoreWriter) openChunkLocallyAndUploadToS3(chunkName string, chunkNumber int, streamName string) {
	chunkSpec := &ChunkSpec{
		ChunkPath:   chunkName,
		StreamName:  streamName,
		ChunkNumber: chunkNumber,
	}

	err := e.database.Update(func(tx *bolt.Tx) error {
		streamsBucket := tx.Bucket([]byte("_streams"))

		if streamsBucket == nil {
			panic("No _streams bucket")
		}

		specAsJson, err := json.Marshal(chunkSpec)
		if err != nil {
			return err
		}

		return streamsBucket.Put([]byte(streamName), specAsJson)
	})

	if err != nil {
		panic(err)
	}

	e.streamToChunkName[streamName] = chunkSpec

	log.Printf("EventstoreWriter: openChunkLocallyAndUploadToS3: Opening %s", chunkName)

	err = e.walManager.AddActiveChunk(chunkName)
	if err != nil {
		panic(err)
	}

	// assign the chunk to us
	peers := []string{e.ip}

	createdMeta, _ := json.Marshal(metaevents.NewCreated())
	authorityChange, _ := json.Marshal(metaevents.NewAuthorityChanged(peers))

	e.walManager.AppendToFile(chunkName, fmt.Sprintf(".%s\n.%s\n", createdMeta, authorityChange))
}

func (e *EventstoreWriter) Close() {
	log.Printf("EventstoreWriter: Closing. Requesting stop from LongTermShipperManager")

	close(e.longTermShipperWork)

	e.walManager.Close()

	log.Printf("EventstoreWriter: Close: Closing BoltDB")

	e.database.Close()

	<-e.longTermShipperDone
}

func (e *EventstoreWriter) scanOpenStreams() {
	err := e.database.Update(func(tx *bolt.Tx) error {
		streamsBucket, err := tx.CreateBucketIfNotExists([]byte("_streams"))
		if err != nil {
			return err
		}

		streamsBucket.ForEach(func(key, value []byte) error {
			streamName := string(key)

			chunkSpec := &ChunkSpec{}

			if err2 := json.Unmarshal(value, chunkSpec); err2 != nil {
				panic(err2)
			}

			log.Printf("EventstoreWriter: scanOpenStreams: stream=%s chunk=%s", streamName, chunkSpec.ChunkPath)

			e.streamToChunkName[streamName] = chunkSpec

			return nil
		})

		return nil
	})

	if err != nil {
		panic(err)
	}
}

func (e *EventstoreWriter) makeBoltDbDirIfNotExist() {
	if _, err := os.Stat(config.BOLTDB_DIR); os.IsNotExist(err) {
		log.Printf("EventstoreWriter: mkdir %s", config.BOLTDB_DIR)

		if err = os.MkdirAll(config.BOLTDB_DIR, 0755); err != nil {
			panic(err)
		}
	}
}
