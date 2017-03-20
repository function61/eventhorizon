package target

import (
	"github.com/asdine/storm"
	"github.com/boltdb/bolt"
	"github.com/function61/pyramid/pusher/pushlib"
	"log"
)

// implements PushAdapter interface
type Target struct {
	pushListener *pushlib.Listener
	db           *storm.DB
	tx           *bolt.Tx
}

func NewTarget() *Target {
	subscriptionId := "foo"

	db, err := storm.Open("/tmp/listener.db")
	// db, err := bolt.Open("/tmp/listener.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	// defer db.Close()

	pa := &Target{
		db:    db,
	}

	pa.pushListener = pushlib.New(
		subscriptionId,
		pa)

	return pa
}

func (pa *Target) Run() {
	pa.setupRoutes()

	pa.pushListener.Serve()
}

func (pa *Target) PushGetOffset(stream string) (string, bool) {
	offset := ""
	if err := pa.db.WithTransaction(pa.tx).Get("cursors", stream, &offset); err != nil {
		if err == storm.ErrNotFound {
			return "", false
		}

		// more serious error
		panic(err)
	}

	return offset, true
}

func (pa *Target) PushSetOffset(stream string, offset string) {
	if err := pa.db.WithTransaction(pa.tx).Set("cursors", stream, offset); err != nil {
		panic(err)
	}
}

func (pa *Target) PushHandleEvent(eventSerialized string) error {
	obj := parse(eventSerialized)

	if e, ok := obj.(*CompanyCreated); ok {
		company := &Company{
			ID:      e.Id,
			Name:    e.Name,
		}

		if err := pa.db.WithTransaction(pa.tx).From("companies").Save(company); err != nil {
			return err
		}
	} else if e, ok := obj.(*UserCreated); ok {
		user := &User{
			ID:      e.Id,
			Name:    e.Name,
			Company: e.Company,
		}

		if err := pa.db.WithTransaction(pa.tx).From("users").Save(user); err != nil {
			return err
		}
	} else {
		log.Printf("Target: unknown event: %s", eventSerialized)
	}

	return nil
}

func (pa *Target) PushTransaction(run func() error) error {
	err := pa.db.Bolt.Update(func(tx *bolt.Tx) error {
		pa.tx = tx

		return run()
	})

	return err
}
