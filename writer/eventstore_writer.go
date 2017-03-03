package writer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/metaevents"
	"github.com/function61/eventhorizon/pubsub/client"
	"github.com/function61/eventhorizon/writer/transaction"
	"github.com/function61/eventhorizon/writer/wal"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type EventstoreWriter struct {
	walManager          *wal.WalManager
	ip                  string
	mu                  sync.Mutex
	database            *bolt.DB
	pubSubClient        *client.PubSubClient
	streamToChunkName   map[string]*transaction.ChunkSpec
	longTermShipperWork chan *transaction.LongTermShippableFile
	longTermShipperDone chan bool
}

func NewEventstoreWriter() *EventstoreWriter {
	e := &EventstoreWriter{
		ip:                  "127.0.0.1",
		streamToChunkName:   make(map[string]*transaction.ChunkSpec),
		longTermShipperWork: make(chan *transaction.LongTermShippableFile),
		longTermShipperDone: make(chan bool),
		mu:                  sync.Mutex{},
	}

	e.makeBoltDbDirIfNotExist()

	e.startPubSubClient()

	go LongTermShipperManager(e.longTermShipperWork, e.longTermShipperDone)

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

	e.database = db

	tx := transaction.NewEventstoreTransaction(e.database)

	if err := e.database.Update(func(boltTx *bolt.Tx) error {
		tx.BoltTx = boltTx

		e.walManager = wal.NewWalManager(tx)

		if err := e.recoverOpenStreams(tx); err != nil {
			return err
		}

		return nil
	}); err != nil {
		panic(err)
	}

	if err := e.applySideEffects(tx); err != nil {
		panic(err)
	}

	return e
}

// these happen after COMMIT, i.e. transaction is not in effect.
// in rare cases WAL manager starts a transaction (for compaction)
func (e *EventstoreWriter) applySideEffects(tx *transaction.EventstoreTransaction) error {
	// - forget files from the WAL table.
	//
	// fd is not actually closed by WAL manager (long term shipper does it instead),
	// but that's ok because it promises not to write into the fd anymore.
	if err := e.walManager.ApplySideEffects(tx); err != nil {
		return err
	}

	// New chunks (CreateStream() and rotate produce new chunks)
	for _, spec := range tx.NewChunks {
		// either first chunk for the stream OR continuation chunk (replaces old spec)
		e.streamToChunkName[spec.StreamName] = spec
	}

	// Write all committed WAL entries to the actual files

	// Queued WAL compactions:
	// - Must ensure fsync() for the above queued writes because after we have purged
	//   WAL, failed writes cannot be reconstructed.
	// - These happen in a new transaction. It is not the end of the world if this fails,
	//   That just means that the WAL compaction will be triggered again on next Append.

	for _, ltsf := range tx.ShipFiles {
		e.longTermShipperWork <- ltsf
	}

	return nil
}

func (e *EventstoreWriter) CreateStream(streamName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	log.Printf("EventstoreWriter: CreateStream: %s", streamName)

	// TODO: query scalablestore so that the stream does not already exist,
	//       so we don't accidentally overwrite any data?

	// /tenants/foo/_/0.log
	chunkName := cursor.NewWithoutServer(streamName, 0, 0).ToChunkPath()

	tx := transaction.NewEventstoreTransaction(e.database)

	err := e.database.Update(func(boltTx *bolt.Tx) error {
		tx.BoltTx = boltTx

		return e.openChunkLocallyAndUploadToS3(chunkName, 0, streamName, tx)
	})
	if err != nil {
		return err
	}

	if err := e.applySideEffects(tx); err != nil {
		panic(err)
	}

	return nil
}

func (e *EventstoreWriter) SubscribeToStream(streamName string, subscriptionId string) error {
	// AppendToStream() acquires lock

	log.Printf("EventstoreWriter: SubscribeToStream: %s", streamName)

	subscribed, _ := json.Marshal(metaevents.NewSubscribed(subscriptionId))

	if err := e.AppendToStream(streamName, []string{fmt.Sprintf(".%s", subscribed)}); err != nil {
		return err
	}

	// TODO: return richer response?

	return nil
}

func (e *EventstoreWriter) UnsubscribeFromStream(streamName string, subscriptionId string) error {
	// AppendToStream() acquires lock

	log.Printf("EventstoreWriter: UnsubscribeFromStream: %s", streamName)

	unsubscribed, _ := json.Marshal(metaevents.NewUnsubscribed(subscriptionId))

	if err := e.AppendToStream(streamName, []string{fmt.Sprintf(".%s", unsubscribed)}); err != nil {
		return err
	}

	return nil
}

func (e *EventstoreWriter) AppendToStream(streamName string, contentArr []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	chunkSpec, streamExists := e.streamToChunkName[streamName]
	if !streamExists {
		return errors.New("EventstoreWriter.AppendToStream: stream does not exist")
	}

	if len(contentArr) == 0 {
		return nil // not an error to call with empty append
	}

	rawLines, err := stringArrayToRawLines(contentArr)
	if err != nil {
		return err
	}

	/*
		if err != nil {
			return err // should not happen
		}

		lengthAfterAppend := lengthBeforeAppend + len(rawLines)
		if lengthAfterAppend > config.CHUNK_ROTATE_THRESHOLD {
			log.Printf("EventstoreWriter: AppendToStream: starting rotate, %d threshold exceeded: %s", config.CHUNK_ROTATE_THRESHOLD, streamName)
		}
	*/

	nextOffsetSideEffect := 0

	tx := transaction.NewEventstoreTransaction(e.database)

	err2 := e.database.Update(func(boltTx *bolt.Tx) error {
		tx.BoltTx = boltTx

		lengthBeforeAppend, err := e.walManager.GetCurrentFileLength(chunkSpec.ChunkPath)
		lengthAfterAppend := lengthBeforeAppend + len(rawLines)

		shouldRotate := lengthAfterAppend > config.CHUNK_ROTATE_THRESHOLD

		if shouldRotate {
			rotatedMeta, _ := json.Marshal(metaevents.NewRotated())

			rawLines += string(rotatedMeta) + "\n"
		}

		nextOffset, err := e.walManager.AppendToFile(chunkSpec.ChunkPath, rawLines, tx)
		if err != nil {
			panic(err)
		}

		if shouldRotate {
			log.Printf("EventstoreWriter: AppendToStream: starting rotate, %d threshold exceeded: %s", config.CHUNK_ROTATE_THRESHOLD, streamName)

			e.rotateStreamChunk(streamName, tx)
		}

		// TODO: deliver this via tx
		// FIXME: rotateStreamChunk() should affect this as well
		nextOffsetSideEffect = nextOffset // TODO: need this?

		return nil
	})
	if err2 != nil {
		return err2
	}

	if err := e.applySideEffects(tx); err != nil {
		panic(err)
	}

	// publish "@1235" to topic "stream:/foo"
	e.pubSubClient.Publish("stream:"+streamName, fmt.Sprintf("@%d", nextOffsetSideEffect))

	return nil
}

func (e *EventstoreWriter) rotateStreamChunk(streamName string, tx *transaction.EventstoreTransaction) {
	currentChunkSpec, ok := e.streamToChunkName[streamName]
	if !ok {
		panic(errors.New("Stream to chunk not found")) // should not happen
	}

	nextChunkNumber := e.streamToChunkName[streamName].ChunkNumber + 1

	nextChunkName := cursor.NewWithoutServer(streamName, nextChunkNumber, 0).ToChunkPath()

	log.Printf("EventstoreWriter: rotateStreamChunk: %s -> %s", currentChunkSpec.ChunkPath, nextChunkName)

	// rotatedMeta, _ := json.Marshal(metaevents.NewRotated())

	// e.AppendToStream(streamName, []string{rotatedMeta})

	// this will never be written to again
	err, fd := e.walManager.CloseActiveFile(currentChunkSpec.ChunkPath, tx)
	if err != nil {
		panic(err)
	}

	fileToShip := &transaction.LongTermShippableFile{
		ChunkName: currentChunkSpec.ChunkPath,
		Fd:        fd,
	}

	// longTermShipper has responsibility of closing the file
	tx.ShipFiles = append(tx.ShipFiles, fileToShip)

	if err := e.openChunkLocallyAndUploadToS3(nextChunkName, nextChunkNumber, streamName, tx); err != nil {
		panic(err)
	}
}

// TODO: subscriptions array
func (e *EventstoreWriter) openChunkLocallyAndUploadToS3(chunkName string, chunkNumber int, streamName string, tx *transaction.EventstoreTransaction) error {
	chunkSpec := &transaction.ChunkSpec{
		ChunkPath:   chunkName,
		StreamName:  streamName,
		ChunkNumber: chunkNumber,
	}

	log.Printf("EventstoreWriter: openChunkLocallyAndUploadToS3: Opening %s", chunkName)

	streamsBucket := tx.BoltTx.Bucket([]byte("_streams"))

	if streamsBucket == nil {
		panic("No _streams bucket")
	}

	specAsJson, err := json.Marshal(chunkSpec)
	if err != nil {
		return err
	}

	if err := streamsBucket.Put([]byte(streamName), specAsJson); err != nil {
		return err
	}

	if err := e.walManager.OpenNewFile(chunkName, tx); err != nil {
		return err
	}

	// assign the chunk to us
	peers := []string{e.ip}

	createdMeta, _ := json.Marshal(metaevents.NewCreated())
	authorityChange, _ := json.Marshal(metaevents.NewAuthorityChanged(peers))

	if _, err := e.walManager.AppendToFile(chunkName, fmt.Sprintf(".%s\n.%s\n", createdMeta, authorityChange), tx); err != nil {
		return err
	}

	tx.NewChunks = append(tx.NewChunks, chunkSpec)

	return nil
}

func (e *EventstoreWriter) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()

	log.Printf("EventstoreWriter: Closing. Requesting stop from LongTermShipperManager")

	close(e.longTermShipperWork)

	e.pubSubClient.Close()

	// FIXME: basically we could just crash as well,
	//        because that's what we are designed for
	tx := transaction.NewEventstoreTransaction(e.database)

	// Close doesn't need an active transaction, but only the database reference
	e.walManager.Close(tx)

	if err := e.applySideEffects(tx); err != nil {
		panic(err)
	}

	log.Printf("EventstoreWriter: Close: Closing BoltDB")

	e.database.Close()

	<-e.longTermShipperDone

	log.Printf("EventstoreWriter: Closed")
}

func (e *EventstoreWriter) recoverOpenStreams(tx *transaction.EventstoreTransaction) error {
	streamsBucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte("_streams"))
	if err != nil {
		return err
	}

	streamsBucket.ForEach(func(key, value []byte) error {
		// streamName := string(key)

		chunkSpec := &transaction.ChunkSpec{}

		if err := json.Unmarshal(value, chunkSpec); err != nil {
			panic(err)
		}

		log.Printf("EventstoreWriter: recoverOpenStreams: stream=%s chunk=%s", chunkSpec.StreamName, chunkSpec.ChunkPath)

		tx.NewChunks = append(tx.NewChunks, chunkSpec)

		return nil
	})

	return nil
}

func (e *EventstoreWriter) startPubSubClient() {
	serverAddr := "127.0.0.1:" + strconv.Itoa(config.PUBSUB_PORT) // TODO: this is us. Will not always be

	log.Printf("EventstoreWriter: connecting to pub/sub server at %s", serverAddr)

	e.pubSubClient = client.NewPubSubClient(serverAddr)
}

func (e *EventstoreWriter) makeBoltDbDirIfNotExist() {
	if _, err := os.Stat(config.BOLTDB_DIR); os.IsNotExist(err) {
		log.Printf("EventstoreWriter: mkdir %s", config.BOLTDB_DIR)

		if err = os.MkdirAll(config.BOLTDB_DIR, 0755); err != nil {
			panic(err)
		}
	}
}

// do not call with empty contentArr
func stringArrayToRawLines(contentArr []string) (string, error) {
	for _, c := range contentArr {
		if strings.Contains(c, "\n") {
			return "", errors.New("EventstoreWriter.AppendToStream: content cannot contain \n")
		}
	}

	return strings.Join(contentArr, "\n") + "\n", nil
}
