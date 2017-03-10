package types

type CreateStreamRequest struct {
	Name string
}

type AppendToStreamRequest struct {
	Stream string
	Lines  []string
}

type LiveReadInput struct {
	Cursor string
}
