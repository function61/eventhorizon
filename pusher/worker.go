package pusher

import (
	"errors"
	"github.com/function61/pyramid/cursor"
	ptypes "github.com/function61/pyramid/pusher/types"
	rtypes "github.com/function61/pyramid/reader/types"
	"log"
	"time"
)

// this is thread safe as long as it doesn't mutate anything from input structs
func Worker(p *Pusher, input *WorkRequest, responseCh chan *WorkResponse) {
	// we probably had an error so backoff for a while
	// as not to cause DOS on the targer
	if input.Status.Sleep != 0 {
		log.Printf("PusherWorker: %s: sleep %s", input.Status.Stream, input.Status.Sleep)
		time.Sleep(input.Status.Sleep)
	}

	if input.SubscriptionId == "" {
		responseCh <- querySubscriptionId(p.receiver, input)
		return
	}

	if input.Status.targetAckedCursor == nil {
		resolvedCursor, err := queryReceiverCursor(
			p.receiver,
			input.Status.Stream,
			input.SubscriptionId)

		if err != nil {
			responseCh <- &WorkResponse{
				Request:               input,
				ShouldContinueRunning: true,
				Error: err,
			}
			return
		}

		inte := &StreamStatus{
			Stream:            resolvedCursor.Stream,
			targetAckedCursor: resolvedCursor,
		}

		responseCh <- &WorkResponse{
			Request:               input,
			ShouldContinueRunning: true,
			ActivityIntelligence:  []*StreamStatus{inte},
		}
		return
	}

	// now input.Status.targetAckedCursor is guaranteed to be defined

	readReq := rtypes.NewReadOptions()
	readReq.Cursor = input.Status.targetAckedCursor

	readResult, readerErr := p.reader.Read(readReq)
	if readerErr != nil {
		responseCh <- &WorkResponse{
			Request:               input,
			ShouldContinueRunning: true,
			Error: readerErr,
		}
		return
	}

	// succesfull read result is empty only when we are at the top
	if len(readResult.Lines) == 0 {
		log.Printf("Pusher: reached the top for %s", input.Status.Stream)

		// TODO: normally should stop, but if this is a subscription stream, ask livereader
		// again in 5 seconds if we don't have any new information from pub/sub

		responseCh <- &WorkResponse{
			Request:               input,
			ShouldContinueRunning: false,
			ActivityIntelligence:  []*StreamStatus{},
		}
		return
	}

	// this is where Receiver does her magic
	pushOutput, pushNetworkErr := p.receiver.Push(ptypes.NewPushInput(input.SubscriptionId, readResult))

	if pushNetworkErr != nil {
		responseCh <- &WorkResponse{
			Request:               input,
			ShouldContinueRunning: true,
			Error: pushNetworkErr,
		}
		return
	}

	if pushOutput.Code != ptypes.CodeSuccess && pushOutput.Code != ptypes.CodeIncorrectBaseOffset {
		// or something truly unexpected?
		panic("Unexpected pushOutput: " + pushOutput.Code)
	}

	response := &WorkResponse{
		Request:               input,
		ShouldContinueRunning: true,
		ActivityIntelligence:  []*StreamStatus{},
	}

	mainAckedCursor := cursor.CursorFromserializedMust(pushOutput.AcceptedOffset)

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

	response.ActivityIntelligence = append(response.ActivityIntelligence, mainIntelligence)

	for _, supplementaryIntelligenceCurSerialized := range pushOutput.BehindCursors {
		supplementaryIntelligenceCur := cursor.CursorFromserializedMust(supplementaryIntelligenceCurSerialized)

		supplementaryIntelligence := &StreamStatus{
			targetAckedCursor: supplementaryIntelligenceCur,
			Stream:            supplementaryIntelligenceCur.Stream,
		}

		response.ActivityIntelligence = append(response.ActivityIntelligence, supplementaryIntelligence)
	}

	responseCh <- response
}

func queryReceiverCursor(receiver ptypes.Receiver, streamName string, subscriptionId string) (*cursor.Cursor, error) {
	log.Printf("PusherWorker: don't know Receiver's position on %s; querying", streamName)

	offsetQueryReadResult := rtypes.NewReadResult()
	offsetQueryReadResult.FromOffset = cursor.ForOffsetQuery(streamName).Serialize()

	correctOffsetQueryResponse, pushNetworkErr := receiver.Push(ptypes.NewPushInput(subscriptionId, offsetQueryReadResult))

	if pushNetworkErr != nil {
		return nil, pushNetworkErr
	}

	if correctOffsetQueryResponse.Code != ptypes.CodeIncorrectBaseOffset {
		return nil, errors.New("PusherWorker: expecting CodeIncorrectBaseOffset")
	}

	return cursor.CursorFromserializedMust(correctOffsetQueryResponse.AcceptedOffset), nil
}

func querySubscriptionId(receiver ptypes.Receiver, input *WorkRequest) *WorkResponse {
	subscriptionId, err := querySubscriptionIdInternal(receiver, input)
	if err != nil {
		return &WorkResponse{
			Request: input,
			Error:   err,
		}
	}

	return &WorkResponse{
		Request:        input,
		SubscriptionId: subscriptionId,
	}
}

func querySubscriptionIdInternal(receiver ptypes.Receiver, input *WorkRequest) (string, error) {
	// just dummy values - receiver must notice the incorrect subscription ID
	// and respond with it first
	subscriptionQueryReadResult := rtypes.NewReadResult()
	subscriptionQueryReadResult.FromOffset = cursor.ForOffsetQuery(input.Status.Stream).Serialize()

	correctSubscriptionQueryResponse, pushNetworkErr := receiver.Push(ptypes.NewPushInput("_query_", subscriptionQueryReadResult))

	if pushNetworkErr != nil {
		return "", pushNetworkErr
	}

	if correctSubscriptionQueryResponse.Code != ptypes.CodeIncorrectSubscriptionId {
		return "", errors.New("PusherWorker: expecting CodeIncorrectSubscriptionId")
	}

	return correctSubscriptionQueryResponse.CorrectSubscriptionId, nil
}
