package pusher

import (
	"github.com/function61/eventhorizon/cursor"
	ptypes "github.com/function61/eventhorizon/pusher/types"
	rtypes "github.com/function61/eventhorizon/reader/types"
	"log"
	"sync"
	"time"
)

/*
	Start thread: known authority cursor, known receiver cursor

		Read that from remote
		Push into receiver
		If BehindCursors, launch new threads for them
		If error or none ACKed, try again in 5s
*/
// subscribe to stream updates only after we have reached realtime for the receiver

type PusherThread struct {
	largestAuthorityCursor string // not doing anything WRT this right now
	receiverCursorToPush   *cursor.Cursor
	stream                 string
	// on pub/sub notification inactivity, is polled every 5sec
	// to guarantee delivery of messages if pub/sub subsystem down
	isSubscriptionStream bool
	pusher               *Pusher
	stopCh               chan bool
	waitGroup            *sync.WaitGroup
}

func NewPusherThread(pusher *Pusher, stream string, isSubscriptionStream bool, largestAuthorityCursor string, receiverCursorToPush *cursor.Cursor, waitGroup *sync.WaitGroup) *PusherThread {
	t := &PusherThread{
		stream:               stream,
		pusher:               pusher,
		receiverCursorToPush: receiverCursorToPush,
		stopCh:               make(chan bool, 1),
		waitGroup:            waitGroup,
	}

	waitGroup.Add(1)

	go t.run()

	return t
}

func (t *PusherThread) run() {
	log.Printf("PusherThread: starting for %s", t.stream)

	defer t.waitGroup.Done()

	if t.receiverCursorToPush == nil {
		t.resolveReceiverCursor()
	}

	for {
		select { // just peek if stop requested
		case <-t.stopCh:
			return // will trigger waitGroup done
			break
		default:
		}

		readReq := rtypes.NewReadOptions()
		readReq.Cursor = t.receiverCursorToPush

		readResult, err := t.pusher.reader.Read(readReq)
		if err != nil {
			panic(err)
		}

		if len(readResult.Lines) == 0 {
			panic("at the top?")
		}

		// this is where Receiver does her magic
		pushResult := t.pusher.receiver.PushReadResult(readResult)

		if pushResult.Code != ptypes.CodeSuccess {
			// above push was not an offset query, so our push offset
			// being incorrect was truly a surprise
			if pushResult.Code == ptypes.CodeIncorrectBaseOffset {
				log.Printf(
					"PusherThread: receiver unexpected %s, correcting to %s",
					ptypes.CodeIncorrectBaseOffset,
					pushResult.CorrectBaseOffset)

				t.receiverCursorToPush = cursor.CursorFromserializedMust(pushResult.CorrectBaseOffset)
				continue // start over from the top
			} else {
				// or something truly unexpected?
				panic("Unexpected pushResult: " + pushResult.Code)
			}
		}

		t.pumpBehindCursorsToManager(pushResult)

		ackedCursor := cursor.CursorFromserializedMust(pushResult.AcceptedOffset)

		// if push and ack cursors were equal, receiver didn't ack anything
		// (most likely a subscription stream) => no use in pushing too soon
		// before
		if ackedCursor.PositionEquals(t.receiverCursorToPush) {
			dur := 5 * time.Second
			log.Printf("PusherThread: Receiver did not ack anything. waiting for %s", dur)
			select { // just peek if stop requested
			case <-t.stopCh:
				return // will trigger waitGroup done
				break
			case <-time.After(dur):
				break
			}
		}

		// update receiver cursor
		t.receiverCursorToPush = ackedCursor
	}

	log.Printf("PusherThread: %s. Stopping.", t.stream)
}

func (t *PusherThread) pumpBehindCursorsToManager(result *ptypes.PushResult) {
	for _, missed := range result.BehindCursors {
		log.Printf("PusherThread: %s behind cursor %s", t.stream, missed)

		// notify pusher manager that receiver told us about streams
		// whose cursors were behind. pusher manager will spawn (or notify)
		// threads for these streams to catch up. most likely this was a subscription
		// stream (other type streams don't respond with BehindCursors) and new
		// pushes won't be accepted until these streams are brought up-to-date
		t.pusher.streamActivity <- StreamActivityMsg{
			CursorSerialized: missed,
		}
	}
}

func (t *PusherThread) resolveReceiverCursor() {
	log.Printf("PusherThread: don't know Receiver's position on %s; querying", t.stream)

	offsetQueryReadResult := rtypes.NewReadResult()
	offsetQueryReadResult.FromOffset = cursor.ForOffsetQuery(t.stream).Serialize()

	correctOffsetQueryResponse := t.pusher.receiver.PushReadResult(offsetQueryReadResult)

	if correctOffsetQueryResponse.Code != ptypes.CodeIncorrectBaseOffset {
		panic("expecting CodeIncorrectBaseOffset")
	}

	log.Printf("PusherThread: Receiver position is %s", correctOffsetQueryResponse.CorrectBaseOffset)

	t.receiverCursorToPush = cursor.CursorFromserializedMust(correctOffsetQueryResponse.CorrectBaseOffset)
}
