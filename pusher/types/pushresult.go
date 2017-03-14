package types

const (
	CodeSuccess             = "success"
	CodeIncorrectBaseOffset = "incorrect_base_offset"
)

type PushOutput struct {
	Code           string
	AcceptedOffset string
	BehindCursors  []string
}

func NewPushOutputIncorrectBaseOffset(correctBaseOffset string) *PushOutput {
	return &PushOutput{
		Code:           CodeIncorrectBaseOffset,
		AcceptedOffset: correctBaseOffset,
	}
}

func NewPushOutput(acceptedOffset string, behindCursors []string) *PushOutput {
	return &PushOutput{
		Code:           CodeSuccess,
		AcceptedOffset: acceptedOffset,
		BehindCursors:  behindCursors,
	}
}
