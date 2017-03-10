package types

type SubscribeToStreamRequest struct {
	Stream         string
	SubscriptionId string
}

type UnsubscribeFromStreamRequest struct {
	Stream         string
	SubscriptionId string
}
