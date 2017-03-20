package pusher

import (
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/cursor"
	"github.com/function61/pyramid/pubsub/client"
	ptypes "github.com/function61/pyramid/pusher/types"
	"github.com/function61/pyramid/reader"
	"log"
	"sync"
	"time"
)

const (
	maxWorkerCount = 5
)

type Pusher struct {
	target       ptypes.Transport
	reader       *reader.EventstoreReader
	pubSubClient *client.PubSubClient
	stopping     bool
	done         *sync.WaitGroup
	streams      map[string]*StreamStatus
}

func New(confCtx *config.Context, target ptypes.Transport) *Pusher {
	return &Pusher{
		target:       target,
		pubSubClient: client.New(confCtx),
		reader:       reader.New(confCtx),
		done:         &sync.WaitGroup{},
		streams:      make(map[string]*StreamStatus),
	}
}

func (p *Pusher) Close() {
	p.stopping = true

	log.Printf("Pusher: stopping")

	p.done.Wait()

	p.pubSubClient.Close()

	log.Printf("Pusher: stopped")
}

func (p *Pusher) Run() {
	responseCh := make(chan *WorkResponse, 1)

	// this design is a bit awkward because we have to send work items
	// to Worker now from two places (here and the below loop) because we can't
	// go to the below loop before we know the subscription ID
	subscriptionId, stop := p.resolveSubscriptionIdForever(responseCh)
	if stop {
		return
	}

	subscriptionStreamPath := "/_subscriptions/" + subscriptionId

	p.pubSubClient.Subscribe("sub:" + subscriptionId)

	p.streams[subscriptionStreamPath] = &StreamStatus{
		Stream:    subscriptionStreamPath,
		shouldRun: true,
	}

	inFlight := 0

	for {
		for _, sint := range p.streams {
			// cannot take anymore workers
			if inFlight >= maxWorkerCount || p.stopping {
				break
			}

			if sint.shouldRun && !sint.isRunning {
				sint.isRunning = true

				request := &WorkRequest{
					SubscriptionId: subscriptionId,
					Status:         &*sint,
				}

				inFlight++
				p.done.Add(1)
				go Worker(p, request, responseCh)
			}
		}

		if inFlight == 0 {
			if p.stopping {
				log.Printf("Pusher: runner stopping")
				return
			} else {
				log.Printf("Pusher: nothing to do")
			}
		}

		select {
		case response := <-responseCh:
			inFlight--
			p.done.Done()

			concerningStream := response.Request.Status.Stream

			p.streams[concerningStream].isRunning = false
			p.streams[concerningStream].shouldRun = response.ShouldContinueRunning

			// if worker had an error, have a small period of sleep before doing
			// any more work for the same stream
			if response.Error != nil {
				log.Printf(
					"Pusher: ERROR (will re-try) pushing %s: %s",
					concerningStream,
					response.Error.Error())
			}

			p.streams[concerningStream].Sleep = response.Sleep

			for _, inte := range response.ActivityIntelligence {
				p.processIntelligence(inte)
			}
		case notificationMsg := <-p.pubSubClient.Notifications:
			log.Printf("Pusher: notification from pubsub: %v", notificationMsg)

			if notificationMsg[0] != "NOTIFY" {
				break
			}

			cur := cursor.CursorFromserializedMust(notificationMsg[2])

			inte := &StreamStatus{
				Stream:              cur.Stream,
				writerLargestCursor: cur,
				shouldRun:           true,
			}

			p.processIntelligence(inte)
		}
	}
}

func (p *Pusher) processIntelligence(inte *StreamStatus) {
	if _, exists := p.streams[inte.Stream]; !exists {
		p.streams[inte.Stream] = &StreamStatus{
			Stream:    inte.Stream,
			shouldRun: true,
		}
	}

	stored := p.streams[inte.Stream]

	if inte.writerLargestCursor != nil {
		// we didn't have previous information => copy as is
		if stored.writerLargestCursor == nil {
			stored.writerLargestCursor = inte.writerLargestCursor

			log.Printf(
				"Pusher: %s Writer known largest initialized @ %s",
				inte.writerLargestCursor.Stream,
				inte.writerLargestCursor.OffsetString())
		} else {
			// have information => compare if provided information is ahead
			if inte.writerLargestCursor.IsAheadComparedTo(stored.writerLargestCursor) {
				stored.writerLargestCursor = inte.writerLargestCursor
				stored.shouldRun = true

				log.Printf(
					"Pusher: %s Writer known largest forward @ %s",
					inte.writerLargestCursor.Stream,
					inte.writerLargestCursor.OffsetString())
			} else {
				log.Printf(
					"Pusher: %s Writer known largest outdated @ %s",
					inte.writerLargestCursor.Stream,
					inte.writerLargestCursor.OffsetString())
			}
		}
	}

	// have intelligence on target status?
	if inte.targetAckedCursor != nil {
		// we didn't have previous information => copy as is
		if stored.targetAckedCursor == nil {
			stored.targetAckedCursor = inte.targetAckedCursor

			log.Printf(
				"Pusher: %s Target initialized @ %s",
				inte.targetAckedCursor.Stream,
				inte.targetAckedCursor.OffsetString())
		} else {
			// have information => compare if provided information is ahead
			if inte.targetAckedCursor.IsAheadComparedTo(stored.targetAckedCursor) {
				stored.targetAckedCursor = inte.targetAckedCursor

				log.Printf(
					"Pusher: %s Target forward @ %s",
					inte.targetAckedCursor.Stream,
					inte.targetAckedCursor.OffsetString())
			} else {
				log.Printf(
					"Pusher: %s Target backpedal/stay still @ %s",
					inte.targetAckedCursor.Stream,
					inte.targetAckedCursor.OffsetString())
			}
		}
	}
}

func (p *Pusher) resolveSubscriptionIdForever(responseCh chan *WorkResponse) (string, bool) {
	subscriptionIdRequest := &WorkRequest{
		SubscriptionId: "",
		// dummy stream. Target will not use this because our subscription id is missing
		Status: &StreamStatus{
			Stream: "/",
		},
	}

	for {
		if p.stopping {
			return "", true
		}

		go Worker(p, subscriptionIdRequest, responseCh)

		subscriptionResponse := <-responseCh

		if subscriptionResponse.Error == nil {
			return subscriptionResponse.SubscriptionId, false
		}

		log.Printf(
			"Pusher: subscriptionResponse error: %s",
			subscriptionResponse.Error.Error())

		subscriptionIdRequest.Status.Sleep = 1 * time.Second
	}
}
