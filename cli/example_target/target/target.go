package target

import (
	"github.com/asdine/storm"
	"github.com/boltdb/bolt"
	"github.com/function61/pyramid/cli/example_target/target/events"
	"github.com/function61/pyramid/cli/example_target/target/transaction"
	"github.com/function61/pyramid/pusher/pushlib"
	"github.com/function61/pyramid/util/lineformatsimple"
	"log"
	"net/http"
)

// implements PushAdapter interface
type Target struct {
	pushListener *pushlib.Listener
	db           *storm.DB
}

func NewTarget() *Target {
	subscriptionId := "foo"

	db, err := storm.Open("/tmp/listener.db")
	if err != nil {
		log.Fatal(err)
	}
	// defer db.Close()

	pa := &Target{
		db: db,
	}

	pa.pushListener = pushlib.New(
		subscriptionId,
		pa)

	return pa
}

func (pa *Target) Run() {
	pa.setupJsonRestApi()

	go pushlib.StartChildProcess("http://127.0.0.1:8080/_pyramid_push")

	// sets up HTTP endpoint for receiving pushes
	pa.pushListener.AttachPushHandler()

	srv := &http.Server{Addr: ":8080"}

	log.Printf("Target: listening at :8080")

	if err := srv.ListenAndServe(); err != nil {
		// cannot panic, because this probably is an intentional close
		log.Printf("Target: ListenAndServe() error: %s", err)
	}
}

// this is where all the magic happens. pushlib calls this function for every
// incoming event from Pyramid.
func (pa *Target) PushHandleEvent(eventSerialized string, tx interface{}) error {
	txReal := tx.(*transaction.Tx)

	// 'FooEvent {"bar": "input here"}'
	//     => eventType='FooEvent'
	//     => payload='{"bar": "input here"}'
	eventType, payload, err := lineformatsimple.Parse(eventSerialized)

	if err != nil {
		return err
	}

	if fn, fnExists := events.EventNameToApplyFn[eventType]; fnExists {
		return fn(txReal, payload)
	}

	log.Printf("Target: unknown event: %s", eventSerialized)

	return nil
}

func (pa *Target) PushGetOffset(stream string, tx interface{}) (string, bool) {
	txReal := tx.(*transaction.Tx)

	offset := ""
	if err := txReal.Db.WithTransaction(txReal.Tx).Get("cursors", stream, &offset); err != nil {
		if err == storm.ErrNotFound {
			return "", false
		}

		// more serious error
		panic(err)
	}

	return offset, true
}

func (pa *Target) PushSetOffset(stream string, offset string, tx interface{}) error {
	txReal := tx.(*transaction.Tx)

	if err := txReal.Db.WithTransaction(txReal.Tx).Set("cursors", stream, offset); err != nil {
		return err
	}

	return nil
}

func (pa *Target) PushWrapTransaction(run func(interface{}) error) error {
	err := pa.db.Bolt.Update(func(tx *bolt.Tx) error {
		return run(&transaction.Tx{pa.db, tx})
	})

	return err
}
