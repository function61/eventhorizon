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
	"github.com/function61/eventhorizon/pubsub/server"
	"github.com/function61/eventhorizon/writer/wal"
	"log"
	"os"
	"strconv"
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
	pubSubServer        *server.ESPubSubServer
	pubSubClient        *client.PubSubClient
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

	e.startPubSubServer()
	e.startPubSubClient()

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

func (e *EventstoreWriter) CreateStream(streamName string) error {
	log.Printf("EventstoreWriter: CreateStream: %s", streamName)

	// TODO: query scalablestore so that the stream does not already exist,
	//       so we don't accidentally overwrite any data?

	// /tenants/foo/_/0.log
	chunkName := cursor.NewWithoutServer(streamName, 0, 0).ToChunkPath()

	if err := e.openChunkLocallyAndUploadToS3(chunkName, 0, streamName); err != nil {
		return err
	}

	return nil
}

func (e *EventstoreWriter) SubscribeToStream(streamName string, subscriptionId string) error {
	log.Printf("EventstoreWriter: SubscribeToStream: %s", streamName)

	subscribed, _ := json.Marshal(metaevents.NewSubscribed(subscriptionId))

	if err := e.AppendToStream(streamName, []string{fmt.Sprintf(".%s", subscribed)}); err != nil {
		return err
	}

	// TODO: return richer response?

	return nil
}

func (e *EventstoreWriter) UnsubscribeFromStream(streamName string, subscriptionId string) error {
	log.Printf("EventstoreWriter: UnsubscribeFromStream: %s", streamName)

	unsubscribed, _ := json.Marshal(metaevents.NewUnsubscribed(subscriptionId))

	if err := e.AppendToStream(streamName, []string{fmt.Sprintf(".%s", unsubscribed)}); err != nil {
		return err
	}

	return nil
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

	// publish "@1235" to topic "stream:/foo"
	e.pubSubClient.Publish("stream:"+streamName, fmt.Sprintf("@%d", nextOffset))

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

	if err := e.openChunkLocallyAndUploadToS3(nextChunkName, nextChunkNumber, streamName); err != nil {
		panic(err)
	}
}

// TODO: subscriptions array
func (e *EventstoreWriter) openChunkLocallyAndUploadToS3(chunkName string, chunkNumber int, streamName string) error {
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
		return err
	}

	e.streamToChunkName[streamName] = chunkSpec

	log.Printf("EventstoreWriter: openChunkLocallyAndUploadToS3: Opening %s", chunkName)

	err = e.walManager.AddActiveChunk(chunkName)
	if err != nil {
		return err
	}

	// assign the chunk to us
	peers := []string{e.ip}

	createdMeta, _ := json.Marshal(metaevents.NewCreated())
	authorityChange, _ := json.Marshal(metaevents.NewAuthorityChanged(peers))

	e.walManager.AppendToFile(chunkName, fmt.Sprintf(".%s\n.%s\n", createdMeta, authorityChange))

	return nil
}

func (e *EventstoreWriter) Close() {
	log.Printf("EventstoreWriter: Closing. Requesting stop from LongTermShipperManager")

	close(e.longTermShipperWork)

	e.pubSubClient.Close()

	e.pubSubServer.Close()

	e.walManager.Close()

	log.Printf("EventstoreWriter: Close: Closing BoltDB")

	e.database.Close()

	<-e.longTermShipperDone

	log.Printf("EventstoreWriter: Closed")
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

func (e *EventstoreWriter) startPubSubServer() {
	serverPort := config.PUBSUB_PORT

	log.Printf("EventstoreWriter: starting pub/sub server on port %d", serverPort)

	e.pubSubServer = server.NewESPubSubServer("0.0.0.0:" + strconv.Itoa(serverPort))
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
