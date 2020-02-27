package ehevent

import (
	"time"
)

func Meta(timestamp time.Time, userId string) EventMeta {
	return EventMeta{
		Timestamp: timestamp.UTC(),
		UserId:    userId,
	}
}

func MetaSystemUser(timestamp time.Time) EventMeta {
	return EventMeta{
		Timestamp: timestamp.UTC(),
	}
}

func MetaWithImpersonator(timestamp time.Time, userId string, impersonatingUserId string) EventMeta {
	if impersonatingUserId == userId {
		// save some space, since this can be deduced
		impersonatingUserId = ""
	}

	return EventMeta{
		Timestamp:           timestamp.UTC(),
		UserId:              userId,
		ImpersonatingUserId: impersonatingUserId,
	}
}

type Allocators map[string]func() Event

// event has type (event's name) and metadata (common attributes shared by all events)
type Event interface {
	MetaType() string
	Meta() *EventMeta
}

type EventMeta struct {
	Timestamp            time.Time
	TimestampOfRecording time.Time
	// whose resource was acted upon?
	// - user changes her password => target=<self> and actor=<self> (though actor omitted then)
	// - when admin changes another users password => target=<someone> and actor=<admin>
	UserId string
	// "who done this"? most times user acts on her own resources, but sometimes
	// e.g. a super user can do stuff to another user's (the target) resources
	ImpersonatingUserId string
}

// returns "who acted on someones resource" (most times same as user id, but could be impersonator)
func (m *EventMeta) ActingUserOrDefaultToTarget() string {
	if m.ImpersonatingUserId != "" {
		return m.ImpersonatingUserId
	} else {
		// no special "actor" recorded => user actioned her own resource
		return m.UserId
	}
}
