package pusher

import (
	"github.com/function61/pyramid/cursor"
	ptypes "github.com/function61/pyramid/pusher/types"
	"github.com/function61/pyramid/reader"
	"log"
	"sync"
)

type Pusher struct {
	receiver       ptypes.Receiver
	reader         *reader.EventstoreReader
	threads        map[string]*PusherThread
	done           chan bool
	stop           chan bool
	streamActivity chan StreamActivityMsg
}

func NewPusher(receiver ptypes.Receiver) *Pusher {
	return &Pusher{
		receiver:       receiver,
		reader:         reader.NewEventstoreReader(),
		threads:        make(map[string]*PusherThread),
		done:           make(chan bool),
		stop:           make(chan bool),
		streamActivity: make(chan StreamActivityMsg, 1),
	}
}

// run this in a goroutine
func (p *Pusher) Run() {
	subscriptionId := p.receiver.GetSubscriptionId()

	subscriptionStreamPath := "/_subscriptions/" + subscriptionId

	wg := &sync.WaitGroup{}

	// start with single thread for the subscription stream.
	// that thread will yield us messages on p.streamActivity if that subscription
	// has new activity any subscribed streams
	p.threads[subscriptionStreamPath] = NewPusherThread(
		p,
		subscriptionStreamPath, // stream
		true, // is subscription stream - check docs for significance
		"",
		nil, // since we don't know Receiver's cursor for this stream, it will be queried
		wg)

	for {
		select {
		case streamActivityMsg := <-p.streamActivity:
			poopCursor := cursor.CursorFromserializedMust(streamActivityMsg.CursorSerialized)

			_, threadExists := p.threads[poopCursor.Stream]

			if threadExists {
				log.Printf("Pusher: thread exists for %s; notifying", poopCursor.Stream)

				log.Printf("Pusher: NOT IMPLEMENTED YET")
			} else {
				log.Printf("Pusher: spawning new thread for %s", poopCursor.Stream)

				p.threads[poopCursor.Stream] = NewPusherThread(
					p,
					poopCursor.Stream, // stream
					false,             // is subscription stream - check docs for significance
					"",
					poopCursor,
					wg)
			}
			break
		case <-p.stop:
			// request stop for all threads
			for _, thread := range p.threads {
				thread.stopCh <- true
			}

			wg.Wait()

			// we are done when all threads have been stopped
			p.done <- true

			return
		}
	}
}

func (p *Pusher) Close() {
	log.Printf("Pusher: requesting stop")

	p.stop <- true
	<-p.done

	log.Printf("Pusher: stopped")
}
