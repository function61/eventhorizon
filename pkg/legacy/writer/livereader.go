package writer

import (
	"bufio"
	"errors"
	"io"
	"os"

	rtypes "github.com/function61/eventhorizon/pkg/legacy/reader/types"
)

// When a Pusher (or anybody else for that matter) wants to read from a stream,
// historical reads (= closed blocks) are directly satisfied from S3.
// But when reads to a stream approach the head - the under-writing, "live", block -
// that block is of course not found from S3 as we need high write IOPS to the stream,
// so those reads are served from the Writer itself. This file implements this.

type LiveReader struct {
	writer *EventstoreWriter
}

func NewLiveReader(writer *EventstoreWriter) *LiveReader {
	return &LiveReader{
		writer: writer,
	}
}

// Intentionally dumps raw lines (without parsing it into ReadResult), because
// it would be stupid to implement parsing both at Writer and Reader - those are
// usually separate nodes so code and data structures would have to be 100 % in sync.
func (l *LiveReader) ReadIntoWriter(opts *rtypes.ReadOptions, writer io.Writer) error {
	l.writer.mu.Lock()
	defer l.writer.mu.Unlock()

	l.writer.metrics.LiveReaderReadOps.Inc()

	// borrowing must be done completely within the above mutex
	fd, err := l.writer.walManager.BorrowFileForReading(opts.Cursor.ToChunkPath())
	if err != nil {
		return os.ErrNotExist
	}

	// We are intentionally not closing the fd, as it is under writing

	fileInfo, errStat := fd.Stat()
	if errStat != nil {
		return errStat
	}

	if int64(opts.Cursor.Offset) > fileInfo.Size() {
		return errors.New("Attempt to seek past EOF")
	}

	_, errSeek := fd.Seek(int64(opts.Cursor.Offset), io.SeekStart)
	if errSeek != nil {
		panic(errSeek)
	}

	scanner := bufio.NewScanner(fd)

	for linesRead := 0; linesRead < opts.MaxLinesToRead && scanner.Scan(); linesRead++ {
		rawLine := scanner.Text() + "\n" // trailing \n was trimmed

		// just dump lines to writer
		if _, err := writer.Write([]byte(rawLine)); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
