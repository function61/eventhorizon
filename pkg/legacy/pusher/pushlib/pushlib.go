package pushlib

// This small library receives pushes over HTTP + JSON and feeds them to your
// application via PushAdapter (you have to implement this), which essentially
// is a bridge between handling events and persisting the results to a database.

import (
	"encoding/json"
	"github.com/function61/eventhorizon/pkg/legacy/cursor"
	"github.com/function61/eventhorizon/pkg/legacy/metaevents"
	ptypes "github.com/function61/eventhorizon/pkg/legacy/pusher/types"
	"log"
	"net/http"
)

type Library struct {
	subscriptionId string
	adapter        PushAdapter
}

func New(subscriptionId string, adapter PushAdapter) *Library {
	return &Library{
		subscriptionId: subscriptionId,
		adapter:        adapter,
	}
}

// called by the HTTP endpoint for pushing.
// returns PushOutput for sending status back to Pusher
func (l *Library) Push(input *ptypes.PushInput) (*ptypes.PushOutput, error) {
	var output *ptypes.PushOutput

	// ask adapter to provide us with a transaction
	err := l.adapter.PushWrapTransaction(func(tx interface{}) error {
		var err error
		output, err = l.pushInternal(input, tx)

		return err
	})

	return output, err
}

// TODO: return nice PushOutput-level errors?
func (l *Library) pushInternal(input *ptypes.PushInput, tx interface{}) (*ptypes.PushOutput, error) {
	// ensure that subscription ID is correct
	if input.SubscriptionId != l.subscriptionId {
		return ptypes.NewPushOutputIncorrectSubscriptionId(l.subscriptionId), nil
	}

	fromOffset := cursor.CursorFromserializedMust(input.Push.FromOffset)

	// ensure that Pusher is continuing Push of the stream from the stream
	// offset that we last saved
	ourOffset, err := l.queryOffset(fromOffset.Stream, tx)
	if err != nil {
		return nil, err
	}

	if !fromOffset.PositionEquals(ourOffset) {
		return ptypes.NewPushOutputIncorrectBaseOffset(ourOffset.Serialize()), nil
	}

	// start with the offset stored in database. if we don't ACK a single
	// event, this is what we'll return and pusher will know that we didn't move
	// forward and throttle the pushes accordingly
	acceptedOffset := ourOffset.Serialize()

	behindCursors := make(map[string]string)

	for _, line := range input.Push.Lines {
		if line.MetaType == metaevents.SubscriptionActivityId {
			payload := line.MetaPayload.(map[string]interface{})
			activity := payload["activity"].([]interface{})

			// everything we encounter in SubscriptionActivity is something we ourselves
			// have subscribed to, so we can just check:
			// => if we're behind
			// => if we're never heard of the stream => start following it
			for _, remoteCursorSerialized_ := range activity {
				remoteCursorSerialized := remoteCursorSerialized_.(string)
				remoteCursor := cursor.CursorFromserializedMust(remoteCursorSerialized)

				// see if this stream's behind-ness is already confirmed as behind?
				// in that case we don't need newer data because we already know our
				// position for this stream, and re-checking it will never change it.
				_, weAlreadyKnowThisStreamIsBehind := behindCursors[remoteCursor.Stream]

				if !weAlreadyKnowThisStreamIsBehind {
					shouldStartFrom, err := l.isRemoteAhead(remoteCursor, tx)
					if err != nil {
						return nil, err
					}

					if shouldStartFrom != nil {
						log.Printf("Library: remote ahead of us: %s", remoteCursorSerialized)

						behindCursors[remoteCursor.Stream] = shouldStartFrom.Serialize()
					}
				}
			}
		}

		line := line // pin
		if err := l.adapter.PushHandleEvent(fromOffset.Stream, &line, tx); err != nil {
			return nil, err
		}

		// only ACK offsets if no behind streams encountered
		// (this happens only for subscription streams anyway)
		if len(behindCursors) == 0 {
			acceptedOffset = line.PtrAfter
		}
	}

	if err := l.adapter.PushSetOffset(fromOffset.Stream, acceptedOffset, tx); err != nil {
		return nil, err
	}

	return ptypes.NewPushOutputSuccess(acceptedOffset, stringMapToSlice(behindCursors)), nil
}

func (l *Library) queryOffset(stream string, tx interface{}) (*cursor.Cursor, error) {
	cursorSerialized, err := l.adapter.PushGetOffset(stream, tx)
	if err != nil {
		return nil, err
	}

	// we can trust that it is a valid stream because all pushes are based on
	// the subscription ID that is exclusive to us. so if stream does not exist
	// => allow it to be created
	if cursorSerialized == "" {
		return cursor.BeginningOfStream(stream, cursor.UnknownServer), nil
	}

	return cursor.CursorFromserializedMust(cursorSerialized), nil
}

func (l *Library) isRemoteAhead(remote *cursor.Cursor, tx interface{}) (*cursor.Cursor, error) {
	ourCursor, err := l.queryOffset(remote.Stream, tx)
	if err != nil {
		return nil, err
	}

	if remote.IsAheadComparedTo(ourCursor) {
		return ourCursor, nil
	} else {
		return nil, nil
	}
}

// attach to receive Pusher's pushes at a defined path, example: "/_eventhorizon_push"
func (l *Library) AttachPushHandler(path string, authToken string) {
	http.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("auth") != authToken {
			http.Error(w, "invalid auth token", http.StatusUnauthorized)
			return
		}

		var push ptypes.PushInput
		if err := json.NewDecoder(r.Body).Decode(&push); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		output, err := l.Push(&push)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		enc := json.NewEncoder(w)
		// FIXME
		_ = enc.Encode(output)
	}))
}

func stringMapToSlice(map_ map[string]string) []string {
	slice := []string{}

	for _, value := range map_ {
		slice = append(slice, value)
	}

	return slice
}
