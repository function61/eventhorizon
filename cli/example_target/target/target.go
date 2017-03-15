package target

import (
	"encoding/json"
	"github.com/function61/pyramid/cursor"
	ptypes "github.com/function61/pyramid/pusher/types"
	"log"
	"net/http"
	"sync"
)

const (
	subscriptionId = "foo"
)

type MsgHandlerFunc func(msg string)

type ReceiverState struct {
	// stream => offset mappings
	offset map[string]string
}

type Receiver struct {
	state      *ReceiverState
	eventsRead int
	mu         *sync.Mutex
	fn         MsgHandlerFunc
}

func NewReceiver() *Receiver {
	state := &ReceiverState{
		offset: make(map[string]string),
	}

	r := &Receiver{
		state:      state,
		eventsRead: 0,
		mu:         &sync.Mutex{},
	}

	r.fn = func(eventSerialized string) {
		log.Printf("%s", eventSerialized)
	}

	subscriptionPath := "/_subscriptions/" + subscriptionId

	state.offset[subscriptionPath] = cursor.BeginningOfStream(
		subscriptionPath,
		cursor.UnknownServer).Serialize()

	return r
}

func (r *Receiver) isRemoteAhead(remote *cursor.Cursor) *cursor.Cursor {
	ourCursorSerialized, offsetExists := r.state.offset[remote.Stream]

	// we've no record for the stream => we are definitely behind
	if !offsetExists {
		return cursor.BeginningOfStream(remote.Stream, cursor.UnknownServer)
	}

	ourCursor := cursor.CursorFromserializedMust(ourCursorSerialized)

	if remote.IsAheadComparedTo(ourCursor) {
		return ourCursor
	} else {
		return nil
	}
}

func (r *Receiver) Push(input *ptypes.PushInput) (*ptypes.PushOutput, error) {
	// TODO: lock this at database level (per stream), so no two receivers can ever
	//       race within the same stream
	r.mu.Lock()
	defer r.mu.Unlock()

	if input.SubscriptionId != subscriptionId {
		return ptypes.NewPushOutputIncorrectSubscriptionId(subscriptionId), nil
	}

	fromOffset := cursor.CursorFromserializedMust(input.Read.FromOffset)
	ourOffset := r.queryOffset(fromOffset.Stream)

	if !fromOffset.PositionEquals(ourOffset) {
		return ptypes.NewPushOutputIncorrectBaseOffset(ourOffset.Serialize()), nil
	}

	// start with the offset stored in database. if we don't ACK a single
	// event, this is what we'll return and pusher will know that we didn't move
	// forward and throttle the pushes accordingly
	acceptedOffset := ourOffset.Serialize()

	behindCursors := make(map[string]string)

	for _, line := range input.Read.Lines {
		if line.IsMeta {
			// everything we encounter in SubscriptionActivity is something we ourselves
			// have subscribed to, so we can just check:
			// => if we're behind
			// => if we're never heard of the stream => start following it
			for _, remoteCursorSerialized := range line.SubscriptionActivity {
				remoteCursor := cursor.CursorFromserializedMust(remoteCursorSerialized)

				// see if this stream's behind-ness is already confirmed as behind?
				// in that case we don't need newer data because we already know our
				// position for this stream, and re-checking it will never change it.
				_, weAlreadyKnowThisStreamIsehind := behindCursors[remoteCursor.Stream]

				if !weAlreadyKnowThisStreamIsehind {
					shouldStartFrom := r.isRemoteAhead(remoteCursor)

					if shouldStartFrom != nil {
						log.Printf("Receiver: remote ahead of us: %s", remoteCursorSerialized)

						behindCursors[remoteCursor.Stream] = shouldStartFrom.Serialize()
					}
				}
			}
		} else {
			r.fn(line.Content)
		}

		if (r.eventsRead % 10000) == 0 {
			log.Printf("Receiver: %d events read", r.eventsRead)
		}

		r.eventsRead++
		// log.Printf("Receiver: accepted %s", line.Content)

		// only ACK offsets if no behind streams encountered
		// (this happens only for subscription streams anyway)
		if len(behindCursors) == 0 {
			acceptedOffset = line.PtrAfter
		}
	}

	// log.Printf("Receiver: saving ACKed offset %s", acceptedOffset)

	r.state.offset[fromOffset.Stream] = acceptedOffset

	return ptypes.NewPushOutputSuccess(acceptedOffset, stringMapToSlice(behindCursors)), nil
}

func (r *Receiver) queryOffset(stream string) *cursor.Cursor {
	cursorSerialized, exists := r.state.offset[stream]

	// we can trust that it is a valid stream because all pushes are based on
	// the subscription ID that is exclusive to us. so if stream does not exist
	// => allow it to be created
	if !exists {
		return cursor.BeginningOfStream(stream, cursor.NoServer)
	}

	return cursor.CursorFromserializedMust(cursorSerialized)
}

func stringMapToSlice(mapp map[string]string) []string {
	slice := []string{}

	for _, value := range mapp {
		slice = append(slice, value)
	}

	return slice
}

func Serve() {
	srv := &http.Server{Addr: ":8080"}

	rcvr := NewReceiver()

	log.Printf("Receiver: listening at :8080")

	http.Handle("/_pyramid_push", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var push ptypes.PushInput
		if err := json.NewDecoder(r.Body).Decode(&push); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		output, err := rcvr.Push(&push)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		enc := json.NewEncoder(w)
		enc.Encode(output)
	}))

	if err := srv.ListenAndServe(); err != nil {
		// cannot panic, because this probably is an intentional close
		log.Printf("WriterHttp: ListenAndServe() error: %s", err)
	}
}
