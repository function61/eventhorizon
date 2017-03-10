package writer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/cursor"
	"github.com/function61/pyramid/metaevents"
	"github.com/function61/pyramid/pubsub/client"
	"github.com/function61/pyramid/writer/longtermshipper"
	"github.com/function61/pyramid/writer/transaction"
	"github.com/function61/pyramid/writer/types"
	"github.com/function61/pyramid/writer/wal"
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
	streamToChunkName   map[string]*types.ChunkSpec
	longTermShipperWork chan *types.LongTermShippableFile
	longTermShipperDone chan bool
	subAct              *SubscriptionActivityTask
	LiveReader          *LiveReader
	metrics             *Metrics
}

func NewEventstoreWriter() *EventstoreWriter {
	e := &EventstoreWriter{
		ip:                  "127.0.0.1",
		streamToChunkName:   make(map[string]*types.ChunkSpec),
		longTermShipperWork: make(chan *types.LongTermShippableFile),
		longTermShipperDone: make(chan bool),
		mu:                  sync.Mutex{},
		metrics:             NewMetrics(),
	}

	e.makeBoltDbDirIfNotExist()

	e.startPubSubClient()

	go longtermshipper.RunManager(e.longTermShipperWork, e.longTermShipperDone)

	// DB will be created if not exists

	dbLocation := config.BOLTDB_DIR + "/evenstore-wal.boltdb"

	log.Printf("EventstoreWriter: opening DB %s", dbLocation)

	/*	Bolt buckets:

		_dirtystreams:
			/tenants/foo => /tenants/foo:2:450

		_streamsubscriptions:
			/tenants/foo => subId1,subId2
			/tenants/bar => subId2

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

		// since we just started with empty data structures, read from database
		// which streams we have open (and their metadata)
		// TODO: have one WAL instance per file, so this call will be the only
		//       source of truth. currently both of them keep track of open files.
		if err := e.discoverOpenStreamsMetadata(tx); err != nil {
			return err
		}

		return nil
	}); err != nil {
		panic(err)
	}

	// Recovered writes from WAL, WAL compactions etc.
	if err := e.applySideEffects(tx); err != nil {
		panic(err)
	}

	e.subAct = NewSubscriptionActivityTask(e)

	e.LiveReader = NewLiveReader(e)

	return e
}

func (e *EventstoreWriter) CreateStream(streamName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	log.Printf("EventstoreWriter: CreateStream: %s", streamName)

	// TODO: query scalablestore so that the stream does not already exist,
	//       so we don't accidentally overwrite any data?

	// /tenants/foo/_/0.log
	streamFirstChunkCursor := cursor.BeginningOfStream(streamName, e.ip)

	tx := transaction.NewEventstoreTransaction(e.database)

	err := e.database.Update(func(boltTx *bolt.Tx) error {
		tx.BoltTx = boltTx

		// "/tenants/foo" => "/tenants"
		parentStream := parentStreamName(streamName)

		if parentStream != streamName { // only equal when "/" (root stream)
			childStreamCreated := metaevents.NewChildStreamCreated(streamFirstChunkCursor.Serialize())

			// errors also if parent stream does not exist
			if err := e.appendToStreamInternal(parentStream, nil, childStreamCreated.Serialize(), tx); err != nil {
				return err
			}
		}

		return e.openChunkLocally(streamFirstChunkCursor, tx)
	})
	if err != nil {
		return err
	}

	if err := e.applySideEffects(tx); err != nil {
		return err
	}

	e.metrics.CreateStreamOps.Inc()

	return nil
}

func (e *EventstoreWriter) SubscribeToStream(streamName string, subscriptionId string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	log.Printf("EventstoreWriter: SubscribeToStream: %s", streamName)

	subscribedEvent := metaevents.NewSubscribed(subscriptionId)

	if strings.HasPrefix(streamName, subscriptionStreamPath("")) {
		// this would cause an endless SubscriptionActivity notification loop
		return errors.New("SubscribeToStream: cannot subscribe to a subscription stream")
	}

	tx := transaction.NewEventstoreTransaction(e.database)

	err := e.database.Update(func(boltTx *bolt.Tx) error {
		tx.BoltTx = boltTx

		if !e.streamExists(subscriptionStreamPath(subscriptionId), tx) {
			return errors.New(fmt.Sprintf("SubscribeToStream: subscription %s does not exist", subscriptionId))
		}

		existingSubscriptions := getSubscriptionsForStream(streamName, tx.BoltTx)

		if stringSliceItemIndex(subscriptionId, existingSubscriptions) != -1 {
			return errors.New(fmt.Sprintf("SubscribeToStream: subscription %s already subscribed", subscriptionId))
		}

		newSubscriptions := append(existingSubscriptions, subscriptionId)

		if err := saveSubscriptionsForStream(streamName, newSubscriptions, tx.BoltTx); err != nil {
			return err
		}

		// since the subscribed event is saved into the *very same stream* we are
		// subscribing to, an initial SubscriptionActivity event will be raised
		// for the stream even if the stream doesn't have any other "real" activity.
		// => subscriber will notice it. everything went better than expected :)

		return e.appendToStreamInternal(streamName, nil, subscribedEvent.Serialize(), tx)
	})
	if err != nil {
		return err
	}

	if err := e.applySideEffects(tx); err != nil {
		return err
	}

	// TODO: return richer response?

	e.metrics.SubscribeToStreamOps.Inc()

	return nil
}

func (e *EventstoreWriter) UnsubscribeFromStream(streamName string, subscriptionId string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	log.Printf("EventstoreWriter: UnsubscribeFromStream: %s", streamName)

	unsubscribedEvent := metaevents.NewUnsubscribed(subscriptionId)

	tx := transaction.NewEventstoreTransaction(e.database)

	err := e.database.Update(func(boltTx *bolt.Tx) error {
		tx.BoltTx = boltTx

		// intentionally not checking if subscription stream exists here, because it was
		// already checked on subscription

		existingSubscriptions := getSubscriptionsForStream(streamName, tx.BoltTx)

		idxInSlice := stringSliceItemIndex(subscriptionId, existingSubscriptions)

		if idxInSlice == -1 {
			return errors.New(fmt.Sprintf("SubscribeToStream: subscription %s is not subscribed", subscriptionId))
		}

		newSubscriptions := append(existingSubscriptions[:idxInSlice], existingSubscriptions[idxInSlice+1:]...)

		if err := saveSubscriptionsForStream(streamName, newSubscriptions, tx.BoltTx); err != nil {
			return err
		}

		return e.appendToStreamInternal(streamName, nil, unsubscribedEvent.Serialize(), tx)
	})
	if err != nil {
		return err
	}

	if err := e.applySideEffects(tx); err != nil {
		return err
	}

	e.metrics.UnsubscribeFromStreamOps.Inc()

	return nil
}

func (e *EventstoreWriter) AppendToStream(streamName string, contentArr []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	tx := transaction.NewEventstoreTransaction(e.database)

	err := e.database.Update(func(boltTx *bolt.Tx) error {
		tx.BoltTx = boltTx

		return e.appendToStreamInternal(streamName, contentArr, "", tx)
	})
	if err != nil {
		return err
	}

	if err := e.applySideEffects(tx); err != nil {
		return err
	}

	e.metrics.AppendToStreamOps.Inc()

	return nil
}

func (e *EventstoreWriter) appendToStreamInternal(streamName string, contentArr []string, metaEventsRaw string, tx *transaction.EventstoreTransaction) error {
	chunkSpec, streamExists := e.streamToChunkName[streamName]
	if !streamExists {
		return errors.New(fmt.Sprintf("EventstoreWriter.AppendToStream: stream %s does not exist", streamName))
	}

	if len(contentArr) == 0 && metaEventsRaw == "" {
		return nil // not an error to call with empty append
	}

	tx.NonMetaLinesAdded += len(contentArr)

	rawLines, err := stringArrayToRawLines(contentArr)
	if err != nil {
		return err
	}

	lengthBeforeAppend, err := e.walManager.GetCurrentFileLength(chunkSpec.ChunkPath)
	if err != nil {
		return err
	}
	lengthAfterAppend := lengthBeforeAppend + len(rawLines)

	var rotatedCursor *cursor.Cursor

	if lengthAfterAppend > config.CHUNK_ROTATE_THRESHOLD {
		rotatedCursor = e.nextChunkCursorFromCurrentChunkSpec(chunkSpec)
	}

	if rotatedCursor != nil {
		// contains pointer to next chunk
		rotatedEvent := metaevents.NewRotated(rotatedCursor.Serialize())

		metaEventsRaw += rotatedEvent.Serialize()
	}

	nextOffset, err := e.walManager.AppendToFile(chunkSpec.ChunkPath, rawLines+metaEventsRaw, tx)
	if err != nil {
		return err
	}

	cursorAfter := cursor.New(streamName, chunkSpec.ChunkNumber, nextOffset, e.ip)

	if rotatedCursor != nil {
		log.Printf("EventstoreWriter: AppendToStream: starting rotate, %d threshold exceeded: %s", config.CHUNK_ROTATE_THRESHOLD, streamName)

		// FIXME: this affects cursor as well
		if err := e.rotateStreamChunk(rotatedCursor, tx); err != nil {
			return err
		}
	}

	e.subAct.MarkOneDirty(cursorAfter, tx)

	tx.AffectedStreams[cursorAfter.Stream] = cursorAfter.Serialize()

	return nil
}

func (e *EventstoreWriter) rotateStreamChunk(nextChunkCursor *cursor.Cursor, tx *transaction.EventstoreTransaction) error {
	currentChunkSpec, ok := e.streamToChunkName[nextChunkCursor.Stream]
	if !ok {
		return errors.New("Stream to chunk not found") // should not happen
	}

	log.Printf("EventstoreWriter: rotateStreamChunk: %s -> %s", currentChunkSpec.ChunkPath, nextChunkCursor.ToChunkPath())

	// this will never be written to again
	err, fd := e.walManager.CloseActiveFile(currentChunkSpec.ChunkPath, tx)
	if err != nil {
		return err
	}

	fileToShip := &types.LongTermShippableFile{
		ChunkName: currentChunkSpec.ChunkPath,
		Fd:        fd,
	}

	// longTermShipper has responsibility of closing the file
	tx.ShipFiles = append(tx.ShipFiles, fileToShip)

	if err := e.openChunkLocally(nextChunkCursor, tx); err != nil {
		return err
	}

	return nil
}

func (e *EventstoreWriter) openChunkLocally(chunkCursor *cursor.Cursor, tx *transaction.EventstoreTransaction) error {
	chunkSpec := &types.ChunkSpec{
		ChunkPath:   chunkCursor.ToChunkPath(),
		StreamName:  chunkCursor.Stream,
		ChunkNumber: chunkCursor.Chunk,
	}

	log.Printf("EventstoreWriter: openChunkLocally: Opening %s", chunkCursor.ToChunkPath())

	streamsBucket := tx.BoltTx.Bucket([]byte("_streams"))

	if streamsBucket == nil {
		panic("No _streams bucket")
	}

	specAsJson, err := json.Marshal(chunkSpec)
	if err != nil {
		return err
	}

	if err := streamsBucket.Put([]byte(chunkCursor.Stream), specAsJson); err != nil {
		return err
	}

	if err := e.walManager.OpenNewFile(chunkCursor.ToChunkPath(), tx); err != nil {
		return err
	}

	streamsActiveSubscriptions := getSubscriptionsForStream(chunkCursor.Stream, tx.BoltTx)

	created := metaevents.NewCreated(streamsActiveSubscriptions)

	metaEventsRaw := created.Serialize()

	if _, err := e.walManager.AppendToFile(chunkCursor.ToChunkPath(), metaEventsRaw, tx); err != nil {
		return err
	}

	tx.NewChunks = append(tx.NewChunks, chunkSpec)

	return nil
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
		e.metrics.ChunkShippedToLongTermStorage.Inc()
		e.longTermShipperWork <- ltsf
	}

	for streamName, latestCursorSerialized := range tx.AffectedStreams {
		e.pubSubClient.Publish("stream:"+streamName, latestCursorSerialized)
	}

	e.metrics.AppendedLinesExclMeta.Add(float64(tx.NonMetaLinesAdded))

	return nil
}

func (e *EventstoreWriter) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()

	log.Printf("EventstoreWriter: Closing. Requesting stop from LongTermShipperManager")

	close(e.longTermShipperWork)

	e.subAct.Close()

	e.pubSubClient.Close()

	// FIXME: basically we could just crash as well,
	//        because that's what we are designed for
	tx := transaction.NewEventstoreTransaction(e.database)

	// Close doesn't need an active transaction, but only the database reference
	e.walManager.Close(tx)

	// WAL's applySideEffects() might start a transaction in the case
	// if we've got WAL logs to compact
	if err := e.applySideEffects(tx); err != nil {
		panic(err)
	}

	log.Printf("EventstoreWriter: Close: Closing BoltDB")

	e.database.Close()

	<-e.longTermShipperDone

	log.Printf("EventstoreWriter: Closed")
}

func (e *EventstoreWriter) discoverOpenStreamsMetadata(tx *transaction.EventstoreTransaction) error {
	streamsBucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte("_streams"))
	if err != nil {
		return err
	}

	streamsBucket.ForEach(func(key, value []byte) error {
		// streamName := string(key)

		chunkSpec := &types.ChunkSpec{}

		if err := json.Unmarshal(value, chunkSpec); err != nil {
			panic(err)
		}

		// not actually new, but "re-opened"
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

func (e *EventstoreWriter) streamExists(streamName string, tx *transaction.EventstoreTransaction) bool {
	_, streamExists := e.streamToChunkName[streamName]

	return streamExists
}

func (e *EventstoreWriter) nextChunkCursorFromCurrentChunkSpec(chunkSpec *types.ChunkSpec) *cursor.Cursor {
	return cursor.New(chunkSpec.StreamName, chunkSpec.ChunkNumber+1, 0, e.ip)
}
