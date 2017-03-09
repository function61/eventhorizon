package writer

import (
	"bufio"
	"errors"
	"github.com/function61/pyramid/reader/types"
	"io"
	"os"
)

type LiveReader struct {
	writer *EventstoreWriter
}

func NewLiveReader(writer *EventstoreWriter) *LiveReader {
	return &LiveReader{
		writer: writer,
	}
}

func (l *LiveReader) ReadIntoWriter(opts *types.ReadOptions, writer io.Writer) error {
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

	return nil
}
