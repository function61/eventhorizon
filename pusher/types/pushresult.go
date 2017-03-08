package types

const (
	CodeSuccess             = "success"
	CodeIncorrectBaseOffset = "incorrect_base_offset"
)

type PushResult struct {
	Code              string
	CorrectBaseOffset string
	AcceptedOffset    string
	BehindCursors     []string
}

func NewPushResultIncorrectBaseOffset(correctBaseOffset string) *PushResult {
	return &PushResult{
		Code:              CodeIncorrectBaseOffset,
		CorrectBaseOffset: correctBaseOffset,
	}
}

func NewPushResult(acceptedOffset string, behindCursors []string) *PushResult {
	return &PushResult{
		Code:           CodeSuccess,
		AcceptedOffset: acceptedOffset,
		BehindCursors:  behindCursors,
	}
}
