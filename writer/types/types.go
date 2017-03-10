package types

type CreateStreamRequest struct {
	Name string
}

type AppendToStreamRequest struct {
	Stream string
	Lines  []string
}

type LiveReadInput struct {
	// TODO: max lines to read
	Cursor string
}
