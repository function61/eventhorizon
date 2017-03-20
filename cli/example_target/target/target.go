package target

import (
	"github.com/function61/pyramid/pusher/pushlib"
	"log"
)

type TargetState struct {
	// stream => offset mappings
	offset map[string]string
}

// implements PushAdapter interface
type Target struct {
	eventsRead   int
	pushListener *pushlib.Listener
	state        *TargetState
}

func NewTarget() *Target {
	state := &TargetState{
		offset: make(map[string]string),
	}

	subscriptionId := "foo"

	pa := &Target{
		state:      state,
		eventsRead: 0,
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

func (pa *Target) PushHandleEvent(eventSerialized string) {
	if (pa.eventsRead % 10000) == 0 {
		log.Printf("Target: %d events read", pa.eventsRead)
	}

	pa.eventsRead++

	// log.Printf("Target: handle %s", eventSerialized)
}

func (pa *Target) PushTransactionBegin() {

}

func (pa *Target) PushTransactionCommit() {

}

func (pa *Target) PushTransactionRollback() {

}
