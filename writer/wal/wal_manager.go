package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/function61/eventhorizon/config"
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
	bolt      *bolt.DB
	openFiles map[string]*WalGuardedFile
}

func NewWalManager(bolt *bolt.DB) *WalManager {
	w := &WalManager{
		openFiles: make(map[string]*WalGuardedFile),
	}

	/*	Bolt keys

		activechunks/?

		CHUNK_NAME/BYTE_OFFSET = DATA_TO_WRITE
		0.log/0 = first line

	*/

	w.bolt = bolt

	w.ensureDataDirectoryExists()
	w.scanAndRecoverActiveWalStores()

	return w
}

func (w *WalManager) AppendToFile(fileName string, content string) (int, error) {
	contentLen := uint64(len(content)) // is safe because length is in bytes, not runes

	// log.Printf("WalManager: AppendToFile: record %s", walRecord.Serialize())

	// Append write to WAL before writing to actual file
	err := w.bolt.Update(func(tx *bolt.Tx) error {
		chunkWalBucket := tx.Bucket([]byte(fileName))

		if chunkWalBucket == nil {
			return errors.New(fmt.Sprintf("WalManager: AppendToFile: chunk %s does not exist", fileName))
		}

		return chunkWalBucket.Put(itob(w.openFiles[fileName].nextFreePosition), []byte(content))
	})
	if err != nil {
		return 0, err
	}

	w.applyWalEntryToFile(w.openFiles[fileName], int64(w.openFiles[fileName].nextFreePosition), []byte(content))

	w.openFiles[fileName].nextFreePosition += contentLen
	w.openFiles[fileName].walSize += contentLen

	// log.Printf("WalManager: AppendToFile: WAL size %d", w.openFiles[fileName].walSize)

	if w.openFiles[fileName].walSize > config.WAL_SIZE_THRESHOLD {
		// log.Printf("WalManager: AppendToFile: WAL size %d exceeded for chunk %s", config.WAL_SIZE_THRESHOLD, fileName)
		w.compactWriteAheadLog(w.openFiles[fileName])
	}

	return int(w.openFiles[fileName].nextFreePosition), nil
}

// Bucket("activechunks").Put(fileName, fileName)
// CreateBucket(fileName).Put(fileName, fileName)
func (w *WalManager) AddActiveChunk(fileName string) error {
	err := w.bolt.Update(func(tx *bolt.Tx) error {
		activeFilesBucket := tx.Bucket([]byte("activechunks"))

		existsCheck := activeFilesBucket.Get([]byte(fileName))
		if existsCheck != nil {
			return errors.New(fmt.Sprintf("WalManager: AddActiveChunk: chunk %s already exists", fileName))
		}

		_, err := tx.CreateBucketIfNotExists([]byte(fileName))
		if err != nil {
			return err
		}

		err = activeFilesBucket.Put([]byte(fileName), []byte(fileName))

		return err
	})

	if err != nil {
		return err
	}

	log.Printf("WalManager: AddActiveChunk: added %s", fileName)

	w.openFiles[fileName] = WalGuardedFileOpen(config.WALMANAGER_DATADIR, fileName)

	return nil
}

// this file will never be written into again
// it is the caller's responsibility to call Close() on the returned file (only if function not errored)
func (w *WalManager) SealActiveFile(fileName string) (error, *os.File) {
	guardedFile := w.openFiles[fileName]

	log.Printf("WalManager: SealActiveFile: compacting %s", fileName)

	// destroys WAL bucket for file
	w.compactWriteAheadLog(guardedFile)

	log.Printf("WalManager: SealActiveFile: closing %s", fileName)

	// delete file from activechunks
	err := w.bolt.Update(func(tx *bolt.Tx) error {
		activeFilesBucket := tx.Bucket([]byte("activechunks"))

		existsCheck := activeFilesBucket.Get([]byte(fileName))
		if existsCheck == nil {
			return errors.New(fmt.Sprintf("WalManager: SealActiveFile: file %s does not exist", fileName))
		}

		_, err := tx.CreateBucketIfNotExists([]byte(fileName))
		if err != nil {
			return err
		}

		return activeFilesBucket.Delete([]byte(fileName))
	})

	if err != nil {
		return err, nil
	}

	delete(w.openFiles, fileName)

	return nil, guardedFile.fd
}

func (w *WalManager) Close() {
	log.Printf("WalManager: Close: closing all open WAL guarded files")

	for _, openFile := range w.openFiles {
		if openFile.walSize > 0 { // call fsync() so we can purge the WAL before exiting
			w.compactWriteAheadLog(openFile)
		}

		openFile.Close() // logs itself
	}
}

func (w *WalManager) scanAndRecoverActiveWalStores() {
	err := w.bolt.Update(func(tx *bolt.Tx) error {
		activeFilesBucket, err := tx.CreateBucketIfNotExists([]byte("activechunks"))
		if err != nil {
			return err
		}

		activeFilesBucket.ForEach(func(key, value []byte) error {
			fileName := string(key)

			w.openFiles[fileName] = w.recoverFileStateFromWal(fileName, tx)

			return nil
		})

		return nil
	})

	if err != nil {
		panic(err)
	}
}

func (w *WalManager) recoverFileStateFromWal(fileName string, tx *bolt.Tx) *WalGuardedFile {
	log.Printf("WalManager: recoverFileStateFromWal: start %s", fileName)

	walFile := WalGuardedFileOpen(config.WALMANAGER_DATADIR, fileName)

	chunkWalBucket := tx.Bucket([]byte(fileName))
	if chunkWalBucket == nil {
		panic(errors.New("No WAL bucket: " + fileName))
	}

	walRecordsWritten := 0

	// these are in order from low to high, since the EventStore is append-only
	chunkWalBucket.ForEach(func(key, value []byte) error {
		filePosition := btoi(key) // this is the absolute truth and cannot be questioned

		// log.Printf("WalManager: record: @%d [%s]", filePosition, string(value))

		w.applyWalEntryToFile(walFile, int64(filePosition), value)

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
			w.compactWriteAheadLog(walFile)
		}
	*/

	return walFile
}

func (w *WalManager) applyWalEntryToFile(walFile *WalGuardedFile, position int64, buffer []byte) {
	// log.Printf("WalManager: applyWalEntryToFile: %s@%d: %s", walFile.fileNameFictional, position, string(buffer))

	if _, err := walFile.fd.Seek(position, io.SeekStart); err != nil {
		panic(err)
	}
	if _, err := walFile.fd.Write(buffer); err != nil {
		panic(err)
	}
}

// Compacts WAL for a given chunk by fsync()'ing the active chunk and only
// after that purging the WAL entries
func (w *WalManager) compactWriteAheadLog(walFile *WalGuardedFile) {
	syncStarted := time.Now()

	log.Printf("WalManager: compactWriteAheadLog: %s fsync() starting", walFile.fileNameFictional)

	err := walFile.fd.Sync()
	if err != nil {
		panic(err)
	}

	// after sync is done, it's ok to clear WAL records

	err = w.bolt.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(walFile.fileNameFictional))

		if err != nil {
			log.Printf("WalManager: compactWriteAheadLog: No bucket found for %s", walFile.fileNameFictional)
			return err
		}

		_, err = tx.CreateBucket([]byte(walFile.fileNameFictional))
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	log.Printf("WalManager: compactWriteAheadLog: WAL entries purged in %s for %s", time.Since(syncStarted), walFile.fileNameFictional)

	walFile.walSize = 0
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
