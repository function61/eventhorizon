package pusher

import (
	"errors"
	"fmt"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/cursor"
	"github.com/function61/pyramid/pubsub/client"
	ptypes "github.com/function61/pyramid/pusher/types"
	"github.com/function61/pyramid/reader"
	rtypes "github.com/function61/pyramid/reader/types"
	"log"
	"sync"
	"time"
)

const (
	maxWorkerCount = 5
)

type WorkOutput struct {
	OldInput *WorkInput
	/*
		StreamActivity
		Re-try
	*/
	ShouldContinueRunning bool

	// if this is subscription stream, intelligence about
	// where the Target stands on subscribed streams that
	// had activity
	ActivityIntelligence []*StreamStatus
}

type StreamStatus struct {
	writerLargestCursor *cursor.Cursor
	targetAckedCursor   *cursor.Cursor
	shouldRun           bool
	isRunning           bool
	Stream              string
}

type WorkInput struct {
	Sleep  time.Duration
	Status *StreamStatus
}

type Pusher struct {
	receiver     ptypes.Receiver
	reader       *reader.EventstoreReader
	pubSubClient *client.PubSubClient
	stopping     bool
	done         *sync.WaitGroup
	streams      map[string]*StreamStatus
}

func New(receiver ptypes.Receiver) *Pusher {
	return &Pusher{
		receiver:     receiver,
		pubSubClient: client.New(fmt.Sprintf("%s:%d", "127.0.0.1", config.PUBSUB_PORT)),
		reader:       reader.New(),
		done:         &sync.WaitGroup{},
		streams:      make(map[string]*StreamStatus),
	}
}

// this is thread safe as long as it doesn't mutate anything from input structs
func Worker(p *Pusher, input *WorkInput, response chan *WorkOutput) {
	if input.Status.targetAckedCursor == nil {
		resolvedCursor, err := resolveReceiverCursor(p.receiver, input.Status.Stream)

		if err != nil {
			panic(err)
		}

		inte := &StreamStatus{
			Stream:            resolvedCursor.Stream,
			targetAckedCursor: resolvedCursor,
		}

		response <- &WorkOutput{
			OldInput:              input,
			ShouldContinueRunning: true,
			ActivityIntelligence:  []*StreamStatus{inte},
		}
		return
	}

	// now input.Status.targetAckedCursor is guaranteed to be defined

	// time.Sleep(1 * time.Second)
	/*
		if input.Sleep != 0 {
			log.Printf("%s: sleeping for %s", input.Status.Stream, input.Sleep)
			time.Sleep(input.Sleep)
		}
	*/

	readReq := rtypes.NewReadOptions()
	readReq.Cursor = input.Status.targetAckedCursor

	readResult, err := p.reader.Read(readReq)
	if err != nil {
		panic(err)
	}

	// succesfull read result is empty only when we are at the top
	if len(readResult.Lines) == 0 {
		log.Printf("Pusher: reached the top for %s", input.Status.Stream)

		// TODO: normally should stop, but if this is a subscription stream, ask livereader
		// again in 5 seconds if we don't have any new information from pub/sub

		response <- &WorkOutput{
			OldInput:              input,
			ShouldContinueRunning: false,
			ActivityIntelligence:  []*StreamStatus{},
		}
		return
	}

	// this is where Receiver does her magic
	pushResult := p.receiver.PushReadResult(readResult)

	if pushResult.Code != ptypes.CodeSuccess && pushResult.Code != ptypes.CodeIncorrectBaseOffset {
		// or something truly unexpected?
		panic("Unexpected pushResult: " + pushResult.Code)
	}

	output := &WorkOutput{
		OldInput:              input,
		ShouldContinueRunning: true,
		ActivityIntelligence:  []*StreamStatus{},
	}

	mainAckedCursor := cursor.CursorFromserializedMust(pushResult.AcceptedOffset)

	// didn't move?
	if mainAckedCursor.PositionEquals(input.Status.targetAckedCursor) {
		log.Printf("Pusher: no movement. should sleep for 5s")
		// time.Sleep(5 * time.Second)
	}

	mainIntelligence := &StreamStatus{
		targetAckedCursor: mainAckedCursor,
		Stream:            mainAckedCursor.Stream,
	}

	if len(readResult.Lines) > 0 {
		mainIntelligence.writerLargestCursor = cursor.CursorFromserializedMust(readResult.Lines[len(readResult.Lines)-1].PtrAfter)
	}

	output.ActivityIntelligence = append(output.ActivityIntelligence, mainIntelligence)

	for _, supplementaryIntelligenceCurSerialized := range pushResult.BehindCursors {
		supplementaryIntelligenceCur := cursor.CursorFromserializedMust(supplementaryIntelligenceCurSerialized)

		supplementaryIntelligence := &StreamStatus{
			targetAckedCursor: supplementaryIntelligenceCur,
			Stream:            supplementaryIntelligenceCur.Stream,
		}

		output.ActivityIntelligence = append(output.ActivityIntelligence, supplementaryIntelligence)
	}

	response <- output
}

/*	Possible outcomes:

	could not reach writer -> retry in 5s
	reached writer, read events -> target acked none
	reached writer, read events -> target acked some
	reached writer, read events -> target said wrong offset
	reached writer, no events -> start sleeping

	Results:

	- last operation failed - retry in 5 sec
	- target's pointer now at
	- i have intelligence for these cursors
*/

func (p *Pusher) Run() {
	subscriptionId := p.receiver.GetSubscriptionId()

	subscriptionStreamPath := "/_subscriptions/" + subscriptionId

	p.pubSubClient.Subscribe("sub:" + subscriptionId)

	p.streams[subscriptionStreamPath] = &StreamStatus{
		Stream:    subscriptionStreamPath,
		shouldRun: true,
	}

	responseCh := make(chan *WorkOutput, 1)

	inFlight := 0

	for {
		for _, sint := range p.streams {
			// cannot take anymore workers
			if inFlight >= maxWorkerCount || p.stopping {
				break
			}

			if sint.shouldRun && !sint.isRunning {
				sint.isRunning = true

				streamWorkItem := &WorkInput{
					Sleep:  5 * time.Second,
					Status: &*sint,
				}

				inFlight++
				p.done.Add(1)
				go Worker(p, streamWorkItem, responseCh)
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
		case output := <-responseCh:
			inFlight--
			p.done.Done()

			inputWas := output.OldInput

			concerningStream := inputWas.Status.Stream

			p.streams[concerningStream].isRunning = false
			p.streams[concerningStream].shouldRun = output.ShouldContinueRunning

			for _, inte := range output.ActivityIntelligence {
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

func (p *Pusher) Close() {
	p.stopping = true

	log.Printf("Pusher: stopping")

	p.done.Wait()

	p.pubSubClient.Close()

	log.Printf("Pusher: stopped")
}

func resolveReceiverCursor(receiver ptypes.Receiver, streamName string) (*cursor.Cursor, error) {
	log.Printf("Pusher: don't know Receiver's position on %s; querying", streamName)

	offsetQueryReadResult := rtypes.NewReadResult()
	offsetQueryReadResult.FromOffset = cursor.ForOffsetQuery(streamName).Serialize()

	correctOffsetQueryResponse := receiver.PushReadResult(offsetQueryReadResult)

	if correctOffsetQueryResponse.Code != ptypes.CodeIncorrectBaseOffset {
		return nil, errors.New("resolveReceiverCursor: expecting CodeIncorrectBaseOffset")
	}

	return cursor.CursorFromserializedMust(correctOffsetQueryResponse.AcceptedOffset), nil
}
