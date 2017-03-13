package types

type CreateStreamRequest struct {
	Name string
}

type AppendToStreamRequest struct {
	Stream string
	Lines  []string
}

type LiveReadInput struct {
	Cursor         string
	MaxLinesToRead int
}

type SubscriberNotification struct {
	SubscriptionId         string
	LatestCursorSerialized string
}
