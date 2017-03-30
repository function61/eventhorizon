package pushlib

// this is the interface your application has to fulfill in order to receive
// pushes from Pusher over HTTP + JSON

import (
	rtypes "github.com/function61/eventhorizon/reader/types"
)

/*	Sequence of events, success:
	----------------------------

	PushWrapTransaction
		PushGetOffset
		PushHandleEvent
		PushHandleEvent
		PushHandleEvent
		PushSetOffset

	Sequence of events, failure:
	----------------------------

	PushWrapTransaction
		PushGetOffset
		PushHandleEvent => error => stop
*/
type PushAdapter interface {
	// PushWrapTransaction() is an API that pushlib calls to wrap all
	// following operations in a single transaction. we:
	//
	//     1) start transaction
	//     2) call back to pushlib with "run" with the transaction, after which pusher calls:
	//        - PushGetOffset()
	//        - PushHandleEvent() multiple times
	//        - PushSetOffset()
	//     3) app gets back error state from "run" callback indicating if anything went
	//        wrong. if we get error back we must rollback the transaction, otherwise commit.
	PushWrapTransaction(run func(tx interface{}) error) error
	PushGetOffset(stream string, tx interface{}) (string, error)
	PushSetOffset(stream string, offset string, tx interface{}) error
	PushHandleEvent(line *rtypes.ReadResultLine, tx interface{}) error
}
