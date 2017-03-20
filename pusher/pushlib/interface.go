package pushlib

/*	Sequence of events, success:
	----------------------------

	PushTransactionBegin
		PushGetOffset
			PushHandleEvent
			PushHandleEvent
			PushHandleEvent
		PushSetOffset
	PushTransactionCommit

	Sequence of events, failure:
	----------------------------

	PushTransactionBegin
		PushGetOffset
			PushHandleEvent => error
	PushTransactionRollback
*/
type PushAdapter interface {
	PushGetOffset(stream string) (string, bool)
	PushSetOffset(stream string, offset string)
	PushHandleEvent(eventSerialized string) error
	PushTransaction(func() error) error
}
