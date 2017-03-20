package target

import (
	"github.com/asdine/storm"
	"github.com/boltdb/bolt"
	"github.com/function61/pyramid/pusher/pushlib"
	"log"
)

type TargetState struct {
	// stream => offset mappings
	offset map[string]string
}

// implements PushAdapter interface
type Target struct {
	pushListener *pushlib.Listener
	state        *TargetState
	db           *storm.DB
	tx           *bolt.Tx
}

func NewTarget() *Target {
	state := &TargetState{
		offset: make(map[string]string),
	}

	subscriptionId := "foo"

	db, err := storm.Open("/tmp/listener.db")
	// db, err := bolt.Open("/tmp/listener.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	// defer db.Close()

	pa := &Target{
		state: state,
		db:    db,
	}
	pa.pushListener = pushlib.New(
		subscriptionId,
		pa)

	return pa
}

func (pa *Target) Run() {
	pa.pushListener.Serve()
}

func (pa *Target) PushGetOffset(stream string) (string, bool) {
	offset, exists := pa.state.offset[stream]
	return offset, exists
}

func (pa *Target) PushSetOffset(stream string, offset string) {
	log.Printf("Target: saving %s -> %s", stream, offset)

	pa.state.offset[stream] = offset
}

func (pa *Target) PushHandleEvent(eventSerialized string) error {
	obj := parse(eventSerialized)

	if _, ok := obj.(*CompanyCreated); ok {
		log.Printf("Target: CompanyCreated: %s", eventSerialized)
	} else if userCreated, ok := obj.(*UserCreated); ok {
		user := &User{
			ID:      userCreated.Id,
			Name:    userCreated.Name,
			Company: userCreated.Company,
		}

		dbx := pa.db.WithTransaction(pa.tx)

		userRepo := dbx.From("users")

		if err := userRepo.Save(user); err != nil {
			return err
		}
		log.Printf("Target: UserCreated: %s", eventSerialized)
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
