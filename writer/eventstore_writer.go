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
	"github.com/function61/pyramid/util/stringslice"
	"github.com/function61/pyramid/writer/longtermshipper"
	"github.com/function61/pyramid/writer/transaction"
	"github.com/function61/pyramid/writer/types"
	"github.com/function61/pyramid/writer/wal"
	"log"
	"os"
	"strings"
	"sync"
)

type EventstoreWriter struct {
	walManager        *wal.WalManager
	mu                sync.Mutex
	database          *bolt.DB
	shipper           *longtermshipper.Shipper
	pubSubClient      *client.PubSubClient
	streamToChunkName map[string]*types.ChunkSpec
	subAct            *SubscriptionActivityTask
	LiveReader        *LiveReader
	metrics           *Metrics
	confCtx           *config.Context
}

func New(confCtx *config.Context) *EventstoreWriter {
	e := &EventstoreWriter{
		streamToChunkName: make(map[string]*types.ChunkSpec),
		mu:                sync.Mutex{},
		shipper:           longtermshipper.New(confCtx),
		metrics:           NewMetrics(),
		confCtx:           confCtx,
	}

	e.makeBoltDbDirIfNotExist()

	e.startPubSubClient()

	// DB will be created if not exists

	dbLocation := config.BoltDbDir + "/evenstore-wal.boltdb"

	log.Printf("EventstoreWriter: opening DB %s", dbLocation)

	/*	Bolt buckets:

		_dirtystreams:
			/tenants/foo => /tenants/foo:2:450

		_streamsubscriptions:
			/tenants/foo => subId1,subId2
			/tenants/bar => subId2

		_streams:
			stream_name => latest block spec
	*/
	db, err := bolt.Open(dbLocation, 0600, nil)
	if err != nil {
		log.Fatal("EventstoreWriter: bolt open failed: ", err)
	}

	e.database = db

	tx := transaction.NewEventstoreTransaction(e.database)

	if err := e.database.Update(func(boltTx *bolt.Tx) error {
		tx.BoltTx = boltTx

		// TODO: have one WAL instance per file instead of a WAL manager.
		e.walManager = wal.NewWalManager(tx)

		// since we just started with empty data structures, read from database
		// which blocks we have open (and their metadata), and re-open
		// those with WAL manager + recover corrupted writes if required.
		if err := e.discoverOpenStreamsMetadataAndRecoverWal(tx); err != nil {
			return err
		}

		if err := e.shipper.RecoverUnfinishedShipments(tx); err != nil {
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

func (e *EventstoreWriter) GetConfigurationContext() *config.Context {
	return e.confCtx
}

func (e *EventstoreWriter) CreateStream(streamName string) (*types.CreateStreamOutput, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	log.Printf("EventstoreWriter: CreateStream: %s", streamName)

	// TODO: query scalablestore so that the stream does not already exist,
	//       so we don't accidentally overwrite any data?

	// /tenants/foo/_/0.log
	streamFirstChunkCursor := cursor.BeginningOfStream(streamName, e.confCtx.GetWriterIp())

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
		return nil, err
	}

	if err := e.applySideEffects(tx); err != nil {
		// TODO: transaction is committed - no much point in returning error?
		return nil, err
	}

	e.metrics.CreateStreamOps.Inc()

	output := &types.CreateStreamOutput{
		Name: streamName,
	}

	return output, nil
}

func (e *EventstoreWriter) SubscribeToStream(streamName string, subscriptionId string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	log.Printf("EventstoreWriter: SubscribeToStream: %s", streamName)

	subscribedEvent := metaevents.NewSubscribed(subscriptionId)

	if !strings.HasPrefix(subscriptionId, subscriptionStreamPath("")) {
		return errors.New("SubscribeToStream: subscription is not a subscription stream")
	}

	if strings.HasPrefix(streamName, subscriptionStreamPath("")) {
		// this would cause an endless SubscriptionActivity notification loop
		return errors.New("SubscribeToStream: cannot subscribe to a subscription stream")
	}

	tx := transaction.NewEventstoreTransaction(e.database)

	err := e.database.Update(func(boltTx *bolt.Tx) error {
		tx.BoltTx = boltTx

		if !e.streamExists(subscriptionId, tx) {
			return errors.New(fmt.Sprintf("SubscribeToStream: subscription %s does not exist", subscriptionId))
		}

		existingSubscriptions := getSubscriptionsForStream(streamName, tx.BoltTx)

		if stringslice.ItemIndex(subscriptionId, existingSubscriptions) != -1 {
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

		idxInSlice := stringslice.ItemIndex(subscriptionId, existingSubscriptions)

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

func (e *EventstoreWriter) AppendToStream(streamName string, contentArr []string) (*types.AppendToStreamOutput, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tx := transaction.NewEventstoreTransaction(e.database)

	err := e.database.Update(func(boltTx *bolt.Tx) error {
		tx.BoltTx = boltTx

		return e.appendToStreamInternal(streamName, contentArr, "", tx)
	})
	if err != nil {
		return nil, err
	}

	if err := e.applySideEffects(tx); err != nil {
		return nil, err
	}

	e.metrics.AppendToStreamOps.Inc()

	output := &types.AppendToStreamOutput{
		Offset: tx.AffectedStreams[streamName],
	}

	return output, nil
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

	if lengthAfterAppend > config.ChunkRotateThreshold {
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

	cursorAfter := cursor.New(
		streamName,
		chunkSpec.ChunkNumber,
		nextOffset,
		e.confCtx.GetWriterIp())

	if rotatedCursor != nil {
		log.Printf("EventstoreWriter: AppendToStream: starting rotate, %d threshold exceeded: %s", config.ChunkRotateThreshold, streamName)

		// FIXME: this affects cursor as well
		if err := e.rotateStreamChunk(rotatedCursor, tx); err != nil {
			return err
		}
	}

	e.subAct.MarkOneDirty(cursorAfter, tx)

	cursorAfterSerialized := cursorAfter.Serialize()

	tx.AffectedStreams[cursorAfter.Stream] = cursorAfterSerialized

	subscribers := getSubscriptionsForStream(cursorAfter.Stream, tx.BoltTx)
	for _, subscriber := range subscribers {
		tx.SubscriberNotifications = append(tx.SubscriberNotifications, &types.SubscriberNotification{
			SubscriptionId:         subscriber,
			LatestCursorSerialized: cursorAfterSerialized,
		})
	}

	return nil
}

func (e *EventstoreWriter) rotateStreamChunk(nextChunkCursor *cursor.Cursor, tx *transaction.EventstoreTransaction) error {
	currentChunkSpec, ok := e.streamToChunkName[nextChunkCursor.Stream]
	if !ok {
		return errors.New("Stream to chunk not found") // should not happen
	}

	log.Printf("EventstoreWriter: rotateStreamChunk: %s -> %s", currentChunkSpec.ChunkPath, nextChunkCursor.ToChunkPath())

	// this will never be written to again
	filePath, err := e.walManager.CloseActiveFile(currentChunkSpec.ChunkPath, tx)
	if err != nil {
		return err
	}

	fileToShip := &types.LongTermShippableFile{
		Block:    cursor.New(currentChunkSpec.StreamName, currentChunkSpec.ChunkNumber, 0, cursor.NoServer),
		FilePath: filePath,
	}

	// durably mark sealed block to be shipped to long term storage
	if err := e.shipper.MarkFileToBeShipped(fileToShip, tx); err != nil {
		return err
	}

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

	streamsBucket := tx.BoltTx.Bucket([]byte("_streams"))

	if streamsBucket == nil {
		return errors.New("No _streams bucket") // should not happen
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
	// do all kinds of complicated stuff related to the file writing
	if err := e.walManager.ApplySideEffects(tx); err != nil {
		return err
	}

	// New chunks (CreateStream() and rotate produce new chunks)
	for _, spec := range tx.NewChunks {
		// either first chunk for the stream OR continuation chunk (replaces old spec)
		e.streamToChunkName[spec.StreamName] = spec
	}

	// files to ship to long term storage. this is done transactionally
	// so shipping is guaranteed to be done at least once
	for _, file := range tx.ShipFiles {
		e.metrics.ChunkShippedToLongTermStorage.Inc()
		e.shipper.Ship(file, tx.Bolt)
	}

	// pub/sub publishes are guaranteed to never block and to never grow buffers
	// unbounded even on connectivity issues. publishes are partitioned per topic
	// and if pub/sub server reads our publishes slowly, we only deliver the latest msg.

	for _, notification := range tx.SubscriberNotifications {
		e.pubSubClient.Publish("sub:"+notification.SubscriptionId, notification.LatestCursorSerialized)
	}

	e.metrics.AppendedLinesExclMeta.Add(float64(tx.NonMetaLinesAdded))

	return nil
}

func (e *EventstoreWriter) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.shipper.Close()

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

	log.Printf("EventstoreWriter: Closed")
}

func (e *EventstoreWriter) discoverOpenStreamsMetadataAndRecoverWal(tx *transaction.EventstoreTransaction) error {
	streamsBucket, createBucketErr := tx.BoltTx.CreateBucketIfNotExists([]byte("_streams"))
	if createBucketErr != nil {
		return createBucketErr
	}

	// key is stream name, but it is also found from block spec.
	// key is mainly used as a unique id.
	return streamsBucket.ForEach(func(key, value []byte) error {
		chunkSpec := &types.ChunkSpec{}

		if err := json.Unmarshal(value, chunkSpec); err != nil {
			return err
		}

		if err := e.walManager.RecoverAndOpenFile(chunkSpec.ChunkPath, tx); err != nil {
			return err
		}

		// not actually new, but "re-opened"
		tx.NewChunks = append(tx.NewChunks, chunkSpec)

		return nil
	})
}

func (e *EventstoreWriter) startPubSubClient() {
	e.pubSubClient = client.New(e.confCtx)
}

func (e *EventstoreWriter) makeBoltDbDirIfNotExist() {
	if _, err := os.Stat(config.BoltDbDir); os.IsNotExist(err) {
		log.Printf("EventstoreWriter: mkdir %s", config.BoltDbDir)

		if err = os.MkdirAll(config.BoltDbDir, 0755); err != nil {
			panic(err)
		}
	}
}

func (e *EventstoreWriter) streamExists(streamName string, tx *transaction.EventstoreTransaction) bool {
	_, streamExists := e.streamToChunkName[streamName]

	return streamExists
}

func (e *EventstoreWriter) nextChunkCursorFromCurrentChunkSpec(chunkSpec *types.ChunkSpec) *cursor.Cursor {
	return cursor.New(chunkSpec.StreamName, chunkSpec.ChunkNumber+1, 0, e.confCtx.GetWriterIp())
}
