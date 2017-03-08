package types

type AppendToStreamRequest struct {
	Stream string
	Lines  []string
}

type LiveReadInput struct {
	Cursor string
}
