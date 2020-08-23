// Optional, standardized event structure + serialization format
package ehevent

import (
	"math"
	"time"
)

// creates event metadata with timestamp and user. timestamp is by default normalized to UTC and
// stored in only millisecond precision to conserve space when storing timestamps as a string.
func Meta(logical time.Time, userId string) EventMeta {
	return EventMeta{
		DontUseTimeLogical: toUtcWithMillisecondPrecision(logical),
		DontUseUserId:      userId,
	}
}

// for when you need to backdate a user's event. i.e. something logical happened before it
// was finally recorded (usually *now*)
func MetaBackdate(logical time.Time, recorded time.Time, userId string) EventMeta {
	return EventMeta{
		DontUseTimeLogical:     toUtcWithMillisecondPrecision(logical),
		DontUseTimeOfRecording: timePtr(toUtcWithMillisecondPrecision(recorded)),
		DontUseUserId:          userId,
	}
}

// for when the event doesn't have a known user, i.e. the user will be "system"
func MetaSystemUser(logical time.Time) EventMeta {
	return EventMeta{
		DontUseTimeLogical: toUtcWithMillisecondPrecision(logical),
	}
}

// same as MetaBackdate(), but with MetaSystemUser() semantics
func MetaSystemUserBackdate(logical time.Time, recorded time.Time) EventMeta {
	return EventMeta{
		DontUseTimeLogical:     toUtcWithMillisecondPrecision(logical),
		DontUseTimeOfRecording: timePtr(toUtcWithMillisecondPrecision(recorded)),
	}
}

// type string => an allocator that returns pointers to concrete structs
type Types map[string]func() Event

// event has type (event's name) and metadata (common attributes shared by all events)
type Event interface {
	MetaType() string
	Meta() *EventMeta
}

// common attributes for all events. timestamps are forced to UTC and rounded to
// millisecond precision. JSON fields are aggressively shortened for compactness
type EventMeta struct {
	// All fields prefixed with "DontUse" to discourage from using directly, but they have
	// to be exported for JSON marshaling to work. There are ways around this by using
	// separate struct for marshaling step, but not with zero-copy so that would consume
	// more memory bandwidth and increase GC pressure

	DontUseTimeLogical     time.Time  `json:"t"`
	DontUseTimeOfRecording *time.Time `json:"tr,omitempty"`
	DontUseUserId          string     `json:"u,omitempty"`
}

// returns systemUserId if event was raised by "system", i.e. there's no explicit user
func (e *EventMeta) UserId(systemUserId string) string {
	if e.DontUseUserId != "" {
		return e.DontUseUserId
	} else {
		return systemUserId
	}
}

func (e *EventMeta) UserIdOrEmptyIfSystem() string {
	return e.UserId("")
}

// "When did this happen" - logical timestamp. most times you'll use this (as opposed to time of recording)
func (e *EventMeta) Time() time.Time {
	return e.DontUseTimeLogical
}

// "When was this event recorded". sometimes logical timestamp precedes time of recording:
// - sometimes you need to backdate events, e.g. importing old data where logical events
//   happened years ago, but the events we're migrating to will be recorded *now*. only in
//   these cases we have separate logical and recording timestamps. sensor data e.g. GPS
//   time of fix vs. time the server received it can be even minutes later if end device
//   has connectivity problems (or end device vs. server clock not in sync).
// - most times you're interested in TimestampLogical()
// - you'll almost always set time of recording to time.Now() when creating events
func (e *EventMeta) TimeOfRecording() time.Time {
	// most times logical timestamp = recording timestamp, so to reduce duplication it'll be nil
	if e.DontUseTimeOfRecording != nil {
		return *e.DontUseTimeOfRecording
	} else {
		return e.DontUseTimeLogical
	}
}

func timePtr(a time.Time) *time.Time {
	return &a
}

func toUtcWithMillisecondPrecision(d time.Time) time.Time {
	millisecondInNanoseconds := float64(time.Millisecond / time.Nanosecond)

	//    123456789
	// => 123000000
	nanosecondsInMillisecondPrecision := math.Round(
		float64(d.Nanosecond())/millisecondInNanoseconds,
	) * millisecondInNanoseconds

	return time.Date(
		d.Year(),
		d.Month(),
		d.Day(),
		d.Hour(),
		d.Minute(),
		d.Second(),
		int(nanosecondsInMillisecondPrecision),
		d.Location(),
	).UTC() // force/translate to UTC
}
