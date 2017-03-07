package writer

import (
	"github.com/function61/eventhorizon/reader"
)

type LiveReader struct {
	writer *EventstoreWriter
}

func NewLiveReader(writer *EventstoreWriter) *LiveReader {
	return &LiveReader{
		writer: writer,
	}
}

func (l *LiveReader) Read(opts *reader.ReadOptions) (*reader.ReadResult, error) {
	l.writer.mu.Lock()
	defer l.writer.mu.Unlock()

	l.writer.metrics.LiveReaderReadOps.Inc()

	// borrowing must be done completely within the above mutex
	fd, err := l.writer.walManager.BorrowFileForReading(opts.Cursor.ToChunkPath())
	if err != nil {
		return nil, err
	}

	// We are intentionally not closing the fd, as it is under writing

	return reader.ReadFromFD(fd, opts)
}
