package transaction

type ChunkSpec struct {
	// TODO: calculate this
	ChunkPath string `json:"chunk_path"`

	StreamName  string `json:"stream_name"`
	ChunkNumber int    `json:"chunk_number"`
}
