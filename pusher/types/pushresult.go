package types

const (
	CodeSuccess                 = "success"
	CodeIncorrectBaseOffset     = "incorrect_base_offset"
	CodeIncorrectSubscriptionId = "incorrect_subscription_id"
)

type PushOutput struct {
	Code                  string
	AcceptedOffset        string
	CorrectSubscriptionId string
	BehindCursors         []string
}

func NewPushOutputIncorrectBaseOffset(correctBaseOffset string) *PushOutput {
	return &PushOutput{
		Code:           CodeIncorrectBaseOffset,
		AcceptedOffset: correctBaseOffset,
	}
}

func NewPushOutputIncorrectSubscriptionId(correctSubscriptionId string) *PushOutput {
	return &PushOutput{
		Code: CodeIncorrectSubscriptionId,
		CorrectSubscriptionId: correctSubscriptionId,
	}
}

func NewPushOutputSuccess(acceptedOffset string, behindCursors []string) *PushOutput {
	return &PushOutput{
		Code:           CodeSuccess,
		AcceptedOffset: acceptedOffset,
		BehindCursors:  behindCursors,
	}
}
