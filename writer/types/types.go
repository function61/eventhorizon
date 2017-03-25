package types

type CreateStreamRequest struct {
	Name string
}

type CreateStreamOutput struct {
	Name string
}

type AppendToStreamRequest struct {
	Stream string
	Lines  []string
}

type AppendToStreamOutput struct {
	Offset string
}

type LiveReadInput struct {
	Cursor         string
	MaxLinesToRead int
}

type SubscriberNotification struct {
	SubscriptionId         string
	LatestCursorSerialized string
}

type SubscribeToStreamRequest struct {
	Stream         string
	SubscriptionId string
}

type UnsubscribeFromStreamRequest struct {
	Stream         string
	SubscriptionId string
}
