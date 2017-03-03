package pusher

import (
	"errors"
	"fmt"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/pusher/endpoint"
	"github.com/function61/eventhorizon/reader"
	"log"
)

type ReceiverState struct {
	offset map[string]string
}

type Receiver struct {
	state *ReceiverState
	eventsRead int
}

func NewReceiver() *Receiver {
	state := &ReceiverState{
		offset: make(map[string]string),
	}

	state.offset["/tenants/foo"] = "/tenants/foo:0:0"

	return &Receiver{
		state: state,
		eventsRead: 0,
	}
}

func (r *Receiver) PushReadResult(result *reader.ReadResult) (*endpoint.PushResult, error) {
	streamName := cursor.CursorFromserializedMust(result.FromOffset).Stream

	ourOffset, err := r.QueryOffset(streamName)
	if err != nil {
		panic(err)
	}

	if ourOffset != result.FromOffset {
		panic(errors.New("Invalid offset"))
	}

	acceptedOffset := ourOffset

	for _, line := range result.Lines {
		if (r.eventsRead % 10000) == 0 {
			log.Printf("Receiver: %d events read", r.eventsRead)
		}

		r.eventsRead++
		// log.Printf("Receiver: accepted %s", line.Content)

		acceptedOffset = line.PtrAfter
	}

	// log.Printf("Receiver: saving ACKed offset %s", acceptedOffset)

	r.state.offset[streamName] = acceptedOffset

	return endpoint.NewPushResult(acceptedOffset), nil
}

func (r *Receiver) QueryOffset(stream string) (string, error) {
	pos, exists := r.state.offset[stream]
	if !exists {
		return "", errors.New(fmt.Sprintf("We do not track stream %s", stream))
	}

	return pos, nil
}
