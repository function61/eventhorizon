package target

import (
	"github.com/function61/pyramid/pusher/pushlib"
	"log"
)

type MsgHandlerFunc func(msg string)

type ReceiverState struct {
	// stream => offset mappings
	offset map[string]string
}

type Receiver struct {
	state        *ReceiverState
	eventsRead   int
	fn           MsgHandlerFunc
	pushListener *pushlib.Listener
}

func NewReceiver() *Receiver {
	state := &ReceiverState{
		offset: make(map[string]string),
	}

	subscriptionId := "foo"

	r := &Receiver{
		state:      state,
		eventsRead: 0,
	}

	offsetGetter := func(stream string) (string, bool) {
		// cannot just return, because then Golang uses single-value version
		offset, exists := r.state.offset[stream]
		return offset, exists
	}

	offsetSaver := func(stream string, offset string) {
		log.Printf("Receiver: saving %s -> %s", stream, offset)

		r.state.offset[stream] = offset
	}

	eventHandler := func(eventSerialized string) {
		if (r.eventsRead % 10000) == 0 {
			log.Printf("Receiver: %d events read", r.eventsRead)
		}

		r.eventsRead++

		log.Printf("Receiver: handle %s", eventSerialized)
	}

	r.pushListener = pushlib.New(
		subscriptionId,
		offsetGetter,
		offsetSaver,
		eventHandler)

	return r
}

func (r *Receiver) Run() {
	r.pushListener.Serve()
}
