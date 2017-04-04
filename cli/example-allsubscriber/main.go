package main

import (
	"github.com/boltdb/bolt"
	"github.com/function61/eventhorizon/metaevents"
	"github.com/function61/eventhorizon/pusher/pushlib"
	"github.com/function61/eventhorizon/pusher/pushlib/writerproxyclient"
	rtypes "github.com/function61/eventhorizon/reader/types"
	"github.com/function61/eventhorizon/util/clicommon"
	"github.com/function61/eventhorizon/util/cryptorandombytes"
	wtypes "github.com/function61/eventhorizon/writer/types"
	"log"
	"net/http"
)

/*	Preparations:

	Create subscription "all":

	$ horizon stream-create /_sub/all

	Subscribe "all" to root stream

	$ horizon stream-subscribe / /_sub/all

	Now when you run this program, it will start observing events from root and
	when it sees any child streams, it will subscribe to them, and the whole
	process goes recursively, resulting in this program seeing *everything*
	there ever will be.
*/
var (
	cursorsBucket = []byte("cursors")
)

type AllSubscriberApp struct {
	pushLibrary    *pushlib.Library
	db             *bolt.DB
	srv            *http.Server
	wproxy         *writerproxyclient.Client
	subscriptionId string
}

func NewAllSubscriberApp() *AllSubscriberApp {
	db, err := bolt.Open("/tmp/allsubscriber.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	// defer db.Close()

	a := &AllSubscriberApp{
		db:             db,
		srv:            &http.Server{Addr: ":8080"},
		wproxy:         writerproxyclient.New(),
		subscriptionId: "/_sub/all",
	}

	if err := a.initBoltBuckets(); err != nil {
		panic(err)
	}

	a.pushLibrary = pushlib.New(
		a.subscriptionId,
		a)

	return a
}

func (a *AllSubscriberApp) Run() {
	pusherAuthToken := cryptorandombytes.Hex(8)

	go pushlib.StartChildProcess("http://localhost:8080/_eventhorizon_push?auth=" + pusherAuthToken)

	a.pushLibrary.AttachPushHandler("/_eventhorizon_push", pusherAuthToken)

	log.Printf("AllSubscriberApp: listening at %s", a.srv.Addr)

	go func() {
		if err := a.srv.ListenAndServe(); err != nil {
			// cannot panic, because this probably is an intentional close
			log.Printf("AllSubscriberApp: ListenAndServe() error: %s", err)
		}
	}()
}

func (a *AllSubscriberApp) PushHandleEvent(stream string, line *rtypes.ReadResultLine, tx_ interface{}) error {
	// we are only interested about child stream creations
	if line.MetaType != metaevents.ChildStreamCreatedId {
		return nil
	}

	payload := line.MetaPayload.(map[string]interface{})
	childStream := payload["name"].(string)

	// do not subscribe to any subscription streams, that is not allowed as
	// that would cause recursive loop
	if stream == "/_sub" {
		return nil
	}

	log.Printf("newstream: %s", childStream)

	// subscribe to the new stream. if this fails, Pusher will re-push this event.
	// this operation is idempotent, so lost ACKs from Writer are ok.
	err := a.wproxy.SubscribeToStream(&wtypes.SubscribeToStreamRequest{
		Stream:         childStream,
		SubscriptionId: a.subscriptionId,
	})

	return err
}

func (a *AllSubscriberApp) PushGetOffset(stream string, tx_ interface{}) (string, error) {
	tx := tx_.(*bolt.Tx)

	offset := string(tx.Bucket(cursorsBucket).Get([]byte(stream)))

	return offset, nil
}

func (a *AllSubscriberApp) PushSetOffset(stream string, offset string, tx_ interface{}) error {
	tx := tx_.(*bolt.Tx)

	return tx.Bucket(cursorsBucket).Put([]byte(stream), []byte(offset))
}

func (a *AllSubscriberApp) PushWrapTransaction(run func(interface{}) error) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		return run(tx)
	})
}

func (a *AllSubscriberApp) Close() {
	log.Printf("AllSubscriberApp: shutting down")

	if err := a.srv.Shutdown(nil); err != nil {
		panic(err)
	}
}

func (a *AllSubscriberApp) initBoltBuckets() error {
	return a.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("cursors")); err != nil {
			return err
		}

		return nil
	})
}

func main() {
	allSubscriber := NewAllSubscriberApp()
	allSubscriber.Run()

	clicommon.WaitForInterrupt()

	allSubscriber.Close()

	log.Printf("main: done")
}
