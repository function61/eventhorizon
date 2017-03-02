package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/writer/transaction"
	"io"
	"log"
	"os"
	"time"
)

// http://stackoverflow.com/questions/36815975/optimal-way-of-writing-to-append-only-files-on-an-ssd

// http://stackoverflow.com/questions/7463925/guarantees-of-order-of-the-operations-on-file
// https://www.postgresql.org/docs/devel/static/wal.html
// https://www.postgresql.org/message-id/Pine.GSO.4.64.0803311618040.15117@westnet.com

// Uses BoltDB as a backing store for the write-ahead-log.
// Only supports WAL for append-only files.

type WalManager struct {
	openFiles map[string]*WalGuardedFile
}

func NewWalManager(tx *transaction.EventstoreTransaction) *WalManager {
	w := &WalManager{
		openFiles: make(map[string]*WalGuardedFile),
	}

	/*	Bolt keys

		activechunks/?

		CHUNK_NAME/BYTE_OFFSET = DATA_TO_WRITE
		0.log/0 = first line

	*/

	w.ensureDataDirectoryExists()
	w.scanAndRecoverActiveWalStores(tx)

	return w
}

func (w *WalManager) AppendToFile(fileName string, content string, tx *transaction.EventstoreTransaction) (int, error) {
	contentLen := uint64(len(content)) // is safe because length is in bytes, not runes

	// Append write to WAL before writing to actual file
	chunkWalBucket := tx.BoltTx.Bucket([]byte(fileName))

	if chunkWalBucket == nil {
		return 0, errors.New(fmt.Sprintf("WalManager: AppendToFile: chunk %s does not exist", fileName))
	}

	if err := chunkWalBucket.Put(itob(w.openFiles[fileName].nextFreePosition), []byte(content)); err != nil {
		return 0, err
	}

	// TODO: side effects
	if err := w.applyWalEntryToFile(w.openFiles[fileName], int64(w.openFiles[fileName].nextFreePosition), []byte(content)); err != nil {
		panic(err)
	}

	w.openFiles[fileName].nextFreePosition += contentLen
	w.openFiles[fileName].walSize += contentLen

	// log.Printf("WalManager: AppendToFile: WAL size %d", w.openFiles[fileName].walSize)

	if w.openFiles[fileName].walSize > config.WAL_SIZE_THRESHOLD {
		// log.Printf("WalManager: AppendToFile: WAL size %d exceeded for chunk %s", config.WAL_SIZE_THRESHOLD, fileName)
		if err := w.compactWriteAheadLog(w.openFiles[fileName], tx); err != nil {
			panic(err)
		}
	}

	return int(w.openFiles[fileName].nextFreePosition), nil
}

// Bucket("activechunks").Put(fileName, fileName)
// CreateBucket(fileName).Put(fileName, fileName)
func (w *WalManager) AddActiveChunk(fileName string, tx *transaction.EventstoreTransaction) error {
	activeFilesBucket := tx.BoltTx.Bucket([]byte("activechunks"))

	existsCheck := activeFilesBucket.Get([]byte(fileName))
	if existsCheck != nil {
		return errors.New(fmt.Sprintf("WalManager: AddActiveChunk: chunk %s already exists", fileName))
	}

	// create bucket for file
	_, err := tx.BoltTx.CreateBucketIfNotExists([]byte(fileName))
	if err != nil {
		return err
	}

	// list file in active files bucket
	if err := activeFilesBucket.Put([]byte(fileName), []byte(fileName)); err != nil {
		return err
	}

	log.Printf("WalManager: AddActiveChunk: added %s", fileName)

	w.openFiles[fileName] = WalGuardedFileOpen(config.WALMANAGER_DATADIR, fileName)

	return nil
}

// this file will never be written into again
// it is the caller's responsibility to call Close() on the returned file (only if function not errored)
func (w *WalManager) SealActiveFile(fileName string, tx *transaction.EventstoreTransaction) (error, *os.File) {
	guardedFile := w.openFiles[fileName]

	log.Printf("WalManager: SealActiveFile: compacting %s", fileName)

	// destroys WAL bucket for file
	if err := w.compactWriteAheadLog(guardedFile, tx); err != nil {
		panic(err)
	}

	log.Printf("WalManager: SealActiveFile: closing %s", fileName)

	// delete file from activechunks
	activeFilesBucket := tx.BoltTx.Bucket([]byte("activechunks"))

	existsCheck := activeFilesBucket.Get([]byte(fileName))
	if existsCheck == nil {
		return errors.New(fmt.Sprintf("WalManager: SealActiveFile: file %s does not exist", fileName)), nil
	}

	if err := activeFilesBucket.Delete([]byte(fileName)); err != nil {
		return err, nil
	}

	delete(w.openFiles, fileName) // TODO: side effect must be done outside of TX

	return nil, guardedFile.fd
}

func (w *WalManager) Close(tx *transaction.EventstoreTransaction) {
	log.Printf("WalManager: Close: closing all open WAL guarded files")

	for _, openFile := range w.openFiles {
		if openFile.walSize > 0 { // call fsync() so we can purge the WAL before exiting
			if err := w.compactWriteAheadLog(openFile, tx); err != nil {
				panic(err)
			}
		}

		openFile.Close() // logs itself
	}
}

func (w *WalManager) GetCurrentFileLength(fileName string) (int, error) {
	wgf, exists := w.openFiles[fileName]
	if !exists {
		return 0, errors.New(fmt.Sprintf("WalManager: file %s does not exist", fileName))
	}

	return int(wgf.nextFreePosition), nil
}

func (w *WalManager) scanAndRecoverActiveWalStores(tx *transaction.EventstoreTransaction) {
	activeFilesBucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte("activechunks"))
	if err != nil {
		panic(err)
	}

	activeFilesBucket.ForEach(func(key, value []byte) error {
		fileName := string(key)

		w.openFiles[fileName] = w.recoverFileStateFromWal(fileName, tx)

		return nil
	})

	if err != nil {
		panic(err)
	}
}

func (w *WalManager) recoverFileStateFromWal(fileName string, tx *transaction.EventstoreTransaction) *WalGuardedFile {
	log.Printf("WalManager: recoverFileStateFromWal: start %s", fileName)

	walFile := WalGuardedFileOpen(config.WALMANAGER_DATADIR, fileName)

	chunkWalBucket := tx.BoltTx.Bucket([]byte(fileName))
	if chunkWalBucket == nil {
		panic(errors.New("No WAL bucket: " + fileName))
	}

	walRecordsWritten := 0

	// these are in order from low to high, since the EventStore is append-only
	chunkWalBucket.ForEach(func(key, value []byte) error {
		filePosition := btoi(key) // this is the absolute truth and cannot be questioned

		// log.Printf("WalManager: record: @%d [%s]", filePosition, string(value))

		if err := w.applyWalEntryToFile(walFile, int64(filePosition), value); err != nil {
			panic(err)
		}

		walValueSize := uint64(len(value))

		walFile.nextFreePosition = filePosition + walValueSize

		walFile.walSize += walValueSize

		walRecordsWritten++

		return nil
	})

	log.Printf("WalManager: recoverFileStateFromWal: %d record(s) written", walRecordsWritten)

	// compact the WAL entries
	// FIXME: cannot use (without refactoring) as we're already inside a transaction
	/*
		if walRecordsWritten > 0 {
			if err := w.compactWriteAheadLog(walFile); err != nil {
				panic(err)
			}
		}
	*/

	return walFile
}

func (w *WalManager) applyWalEntryToFile(walFile *WalGuardedFile, position int64, buffer []byte) error {
	// log.Printf("WalManager: applyWalEntryToFile: %s@%d: %s", walFile.fileNameFictional, position, string(buffer))

	if _, err := walFile.fd.Seek(position, io.SeekStart); err != nil {
		return err
	}
	if _, err := walFile.fd.Write(buffer); err != nil {
		return err
	}

	return nil
}

// Compacts WAL for a given chunk by fsync()'ing the active chunk and only
// after that purging the WAL entries
func (w *WalManager) compactWriteAheadLog(walFile *WalGuardedFile, tx *transaction.EventstoreTransaction) error {
	syncStarted := time.Now()

	log.Printf("WalManager: compactWriteAheadLog: %s fsync() starting", walFile.fileNameFictional)

	err := walFile.fd.Sync()
	if err != nil {
		return err
	}

	// after sync is done, it's ok to clear WAL records

	if err := tx.BoltTx.DeleteBucket([]byte(walFile.fileNameFictional)); err != nil {
		log.Printf("WalManager: compactWriteAheadLog: No bucket found for %s", walFile.fileNameFictional)
		return err
	}

	_, err = tx.BoltTx.CreateBucket([]byte(walFile.fileNameFictional))
	if err != nil {
		return err
	}

	log.Printf("WalManager: compactWriteAheadLog: WAL entries purged in %s for %s", time.Since(syncStarted), walFile.fileNameFictional)

	walFile.walSize = 0

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
