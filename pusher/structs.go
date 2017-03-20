package pusher

import (
	"github.com/function61/pyramid/cursor"
	"time"
)

type StreamStatus struct {
	writerLargestCursor *cursor.Cursor
	targetAckedCursor   *cursor.Cursor
	shouldRun           bool
	isRunning           bool
	Stream              string
	Sleep               time.Duration
}

type WorkResponse struct {
	Request               *WorkRequest
	Error                 error
	Sleep                 time.Duration
	ShouldContinueRunning bool

	// if this is subscription stream, intelligence about
	// where the Target stands on subscribed streams that
	// had activity
	ActivityIntelligence []*StreamStatus

	SubscriptionId string
}

type WorkRequest struct {
	SubscriptionId string
	Status         *StreamStatus
}
