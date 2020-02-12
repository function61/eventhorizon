package types

import (
	rtypes "github.com/function61/eventhorizon/pkg/legacy/reader/types"
)

const (
	CodeSuccess                 = "success"
	CodeIncorrectBaseOffset     = "incorrect_base_offset"
	CodeIncorrectSubscriptionId = "incorrect_subscription_id"
)

type PushInput struct {
	SubscriptionId string
	Push           *rtypes.ReadResult
}

func NewPushInput(subscriptionId string, readResult *rtypes.ReadResult) *PushInput {
	return &PushInput{
		SubscriptionId: subscriptionId,
		Push:           readResult,
	}
}

type PushOutput struct {
	Code                  string
	AcceptedOffset        string
	CorrectSubscriptionId string `json:",omitempty"`
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
		Code:                  CodeIncorrectSubscriptionId,
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
