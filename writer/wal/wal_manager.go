package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/writer/transaction"
	"io"
	"log"
	"os"
	"time"
)

// Uses BoltDB as a backing store for the write-ahead-log.
// Only supports WAL for append-only files.

// TODO: since there is not much state, merge WALGuardedFile and WalManager

type WalManager struct {
	openFiles map[string]*WalGuardedFile
}

func NewWalManager(tx *transaction.EventstoreTransaction) *WalManager {
	w := &WalManager{
		openFiles: make(map[string]*WalGuardedFile),
	}

	w.ensureDataDirectoryExists()

	return w
}

// WARNING: do only call once per file per transaction
func (w *WalManager) AppendToFile(fileName string, content string, tx *transaction.EventstoreTransaction) (int, error) {
	contentLen := uint64(len(content)) // is safe because length is in bytes, not runes

	// Append write to WAL before writing to actual file
	chunkWalBucket := tx.BoltTx.Bucket([]byte(fileName))

	if chunkWalBucket == nil {
		return 0, errors.New(fmt.Sprintf("WalManager: AppendToFile: chunk %s does not exist", fileName))
	}

	fileEntry, exists := w.openFiles[fileName]

	// FIXME: does not support 2x AppendToFile() after OpenNewFile()
	//        (currently not ever done so not a problem)
	writePosition := uint64(0)

	if exists { // might not exist if called within transaction: OpenNewFile() + AppendToFile()
		writePosition = fileEntry.nextFreePosition
	}

	positionAfterWrite := writePosition + contentLen

	// WAL entries look like this:
	// bucket="/foostream/_/0.log" key=123 value="line\n"
	if err := chunkWalBucket.Put(itob(writePosition), []byte(content)); err != nil {
		return 0, err
	}

	tx.QueueWrite(fileName, []byte(content), int64(writePosition))

	// NOTE: walSize + nextFreePosition updated in side effects,
	//       because this whole transaction must be cancellable

	if exists {
		if (fileEntry.walSize + contentLen) > config.WAL_SIZE_THRESHOLD {
			log.Printf("WalManager: AppendToFile: WAL size %d exceeded for chunk %s", config.WAL_SIZE_THRESHOLD, fileName)

			tx.NeedsWALCompaction = append(tx.NeedsWALCompaction, fileName)
		}
	}

	return int(positionAfterWrite), nil
}

// - the file is actually open in write mode. you are responsible for not writing to it.
// - and you are responsible for abandoning the use of the descriptor once you release the Writer's guarding mutex.
// - seeks are OK as we'll seek at the correct position on every write.
func (w *WalManager) BorrowFileForReading(fileName string) (*os.File, error) {
	walFile, has := w.openFiles[fileName]
	if !has {
		return nil, errors.New("No file: " + fileName)
	}

	return walFile.fd, nil
}

func (w *WalManager) OpenNewFile(fileName string, tx *transaction.EventstoreTransaction) error {
	existsCheck := tx.BoltTx.Bucket([]byte(fileName))
	if existsCheck != nil {
		return errors.New(fmt.Sprintf("WalManager: OpenNewFile: chunk %s already exists", fileName))
	}

	// create WAL bucket for file
	_, err := tx.BoltTx.CreateBucket([]byte(fileName))
	if err != nil {
		return err
	}

	log.Printf("WalManager: OpenNewFile: added %s", fileName)

	tx.FilesToOpen = append(tx.FilesToOpen, fileName)

	return nil
}

func (w *WalManager) ApplySideEffects(tx *transaction.EventstoreTransaction) error {
	// queued file opens
	for _, fileName := range tx.FilesToOpen {
		w.openFiles[fileName] = WalGuardedFileOpen(config.WALMANAGER_DATADIR, fileName)
	}

	// queued file writes
	for _, write := range tx.WriteOps {
		walFile := w.openFiles[write.Filename]

		if _, err := walFile.fd.Seek(write.Position, io.SeekStart); err != nil {
			return err
		}
		if _, err := walFile.fd.Write(write.Buffer); err != nil {
			return err
		}

		walFile.nextFreePosition += uint64(len(write.Buffer))
		walFile.walSize += uint64(len(write.Buffer))
	}

	// queued compactions. need transaction for this, but it's not a problem since
	// ApplySideEffects() is called outside of a transaction, and this transaction
	// failure does not compromise any ACID properties as we'll re-try this on startup.
	for _, fileName := range tx.NeedsWALCompaction {
		walFile := w.openFiles[fileName]

		log.Printf("WalManager: fsync() %s", walFile.fileNameFictional)

		syncStarted := time.Now()

		if err := w.openFiles[fileName].fd.Sync(); err != nil {
			return err
		}

		err := tx.Bolt.Update(func(tx *bolt.Tx) error {
			// after sync is done, it's ok to clear WAL records

			if err := tx.DeleteBucket([]byte(walFile.fileNameFictional)); err != nil {
				log.Printf("WalManager: no bucket found for %s", walFile.fileNameFictional)
				return err
			}

			if _, err := tx.CreateBucket([]byte(walFile.fileNameFictional)); err != nil {
				return err
			}

			log.Printf("WalManager: WAL entries purged in %s for %s", time.Since(syncStarted), walFile.fileNameFictional)

			return nil
		})
		if err != nil {
			panic(err)
		}

		log.Printf("WalManager: ApplySideEffects: WAL purged in %s", time.Since(syncStarted))

		walFile.walSize = 0
	}

	// FIXME: chunk WAL bucket is left behind, and this bug is actually the only
	//        thing preventing Writer from re-opening a stream where blockIdx > 0
	for _, fileName := range tx.FilesToDisengageWalFor {
		// this is a promise not to write into the file ever again
		delete(w.openFiles, fileName)
	}

	for _, fileName := range tx.FilesToClose {
		// close logs itself
		if err := w.openFiles[fileName].Close(); err != nil {
			panic(err)
		}
	}

	return nil
}

// this file will never be written into again
// it is the caller's responsibility to call Close() on the returned file (only if function not errored)
func (w *WalManager) CloseActiveFile(fileName string, tx *transaction.EventstoreTransaction) (error, *os.File) {
	guardedFile, exists := w.openFiles[fileName]
	if !exists {
		return errors.New(fmt.Sprintf("WalManager: CloseActiveFile: %s not open", fileName)), nil
	}

	log.Printf("WalManager: CloseActiveFile: closing %s", fileName)

	tx.NeedsWALCompaction = append(tx.NeedsWALCompaction, fileName)
	tx.FilesToDisengageWalFor = append(tx.FilesToDisengageWalFor, fileName)

	return nil, guardedFile.fd
}

func (w *WalManager) Close(tx *transaction.EventstoreTransaction) {
	log.Printf("WalManager: Close: closing all open WAL guarded files")

	for _, openFile := range w.openFiles {
		// compact WAL for each file that have WAL entries,
		// so we don't have to re-write them when we start again
		if openFile.walSize > 0 {
			tx.NeedsWALCompaction = append(tx.NeedsWALCompaction, openFile.fileNameFictional)
		}

		// cannot close yet, because WAL compaction is done as a side effect and needs the open file
		tx.FilesToClose = append(tx.FilesToClose, openFile.fileNameFictional)
	}
}

func (w *WalManager) GetCurrentFileLength(fileName string) (int, error) {
	wgf, exists := w.openFiles[fileName]
	if !exists {
		return 0, errors.New(fmt.Sprintf("WalManager: file %s does not exist", fileName))
	}

	return int(wgf.nextFreePosition), nil
}

// use this after restart to recover and re-open a file that was previously open.
// you cannot re-open file that was CloseActiveFile()'d, because that's final
func (w *WalManager) RecoverAndOpenFile(fileName string, tx *transaction.EventstoreTransaction) error {
	log.Printf("WalManager: recovering %s", fileName)

	// panics if open fails
	walFile := WalGuardedFileOpen(config.WALMANAGER_DATADIR, fileName)

	chunkWalBucket := tx.BoltTx.Bucket([]byte(fileName))
	if chunkWalBucket == nil {
		return errors.New("No WAL bucket: " + fileName)
	}

	walRecordsQueued := 0

	// these are in order from low to high, since the EventStore is append-only
	chunkWalBucket.ForEach(func(key, value []byte) error {
		filePosition := btoi(key) // this is the absolute truth and cannot be questioned

		tx.QueueWrite(walFile.fileNameFictional, value, int64(filePosition))

		walRecordsQueued++

		return nil
	})

	// compact the WAL entries
	if walRecordsQueued > 0 {
		log.Printf("WalManager: %d record(s) queued for recovery", walRecordsQueued)

		tx.NeedsWALCompaction = append(tx.NeedsWALCompaction, walFile.fileNameFictional)
	}

	// FIXME: this should be a side effect
	w.openFiles[fileName] = walFile

	return nil
}

func (w *WalManager) ensureDataDirectoryExists() {
	if _, err := os.Stat(config.WALMANAGER_DATADIR); os.IsNotExist(err) {
		log.Printf("WalManager: ensureDataDirectoryExists: mkdir %s", config.WALMANAGER_DATADIR)

		if err = os.MkdirAll(config.WALMANAGER_DATADIR, 0755); err != nil {
			panic(err)
		}
	}
}

// itob returns an 8-byte big endian representation of v.
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btoi(v []byte) uint64 {
	return binary.BigEndian.Uint64(v)
}
