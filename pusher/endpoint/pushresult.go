package endpoint

type PushResult struct {
	AcceptedOffset string
}

func NewPushResult(acceptedOffset string) *PushResult {
	return &PushResult{
		AcceptedOffset: acceptedOffset,
	}
}
