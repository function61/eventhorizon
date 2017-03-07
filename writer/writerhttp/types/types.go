package types

type AppendToStreamRequest struct {
	Stream string
	Lines  []string
}
