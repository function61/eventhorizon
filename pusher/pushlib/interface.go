package pushlib

/*	Sequence of events, success:
	----------------------------

	PushTransaction
		PushGetOffset
			PushHandleEvent
			PushHandleEvent
			PushHandleEvent
		PushSetOffset

	Sequence of events, failure:
	----------------------------

	PushTransaction
		PushGetOffset
			PushHandleEvent => error => stop
*/
type PushAdapter interface {
	PushGetOffset(stream string) (string, bool)
	PushSetOffset(stream string, offset string)
	PushHandleEvent(eventSerialized string) error
	PushTransaction(func() error) error
}
