package store

import (
	"compress/gzip"
	"fmt"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/cursor"
	"github.com/function61/pyramid/scalablestore"
	"io"
	"log"
	"os"
	"time"
)

/*	Compression & encryption pipeline:

	+-----------------+
	|                 |
	|  Seekable file  |
	|                 |
	+--------+--------+
	         |
	         |
	+--------v--------+
	|                 |
	| Gzip compressor |
	|                 |
	+--------+--------+
	         |
	         |
	+--------v--------+
	|                 |
	|   AES-encrypt   |
	|                 |
	+--------+--------+
	         |
	         |
	+--------v--------+
	|                 |
	|  Enc&comp. file |
	|                 |
	+-----------------+


	ECB/CBC/CFM summary:

	ECB

	- No IV
	- Blocks don't depend on previous blocks (=> supports seeking)
	- Repetition between plaintext can be observed between blocks (=> security concern)
	- Should not be used

	CBC

	- Requires IV
	- Blocks depend on previous blocks (=> no seeking)
	- No repetition can be observed

	CFM

	- About same as CBC?
	- Not particularly well known?

	http://crypto.stackexchange.com/questions/225/should-i-use-ecb-or-cbc-encryption-mode-for-my-block-cipher
	http://crypto.stackexchange.com/questions/2476/cipher-feedback-mode
	http://stackoverflow.com/questions/32329512/golang-file-encryption-with-crypto-aes-lib
*/

// TODO: implement encryption

type CompressedEncryptedStore struct {
}

func NewCompressedEncryptedStore() *CompressedEncryptedStore {
	if _, err := os.Stat(config.COMPRESSED_ENCRYPTED_STORE_PATH); os.IsNotExist(err) {
		log.Printf("CompressedEncryptedStore: mkdir %s", config.COMPRESSED_ENCRYPTED_STORE_PATH)

		if err = os.MkdirAll(config.COMPRESSED_ENCRYPTED_STORE_PATH, 0755); err != nil {
			panic(err)
		}
	}

	return &CompressedEncryptedStore{}
}

// TODO: this has a race condition. remove this
func (c *CompressedEncryptedStore) Has(cur *cursor.Cursor) bool {
	if _, err := os.Stat(c.localPath(cur)); os.IsNotExist(err) {
		return false
	}

	return true
}

// stores the file as compressed & encrypted file from WAL's live file.
// when passing the FD, make sure fseek(0)
func (c *CompressedEncryptedStore) SaveFromLiveFile(cur *cursor.Cursor, fromFd *os.File) error {
	localPath := c.localPath(cur)
	localPathTemp := localPath + ".tmp-fromlive"

	resultingFile, err := os.OpenFile(localPathTemp, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	gzipWriter := gzip.NewWriter(resultingFile)

	// pumps everything from original live file into gzipped file
	if _, err := io.Copy(gzipWriter, fromFd); err != nil {
		return err
	}

	// this is super necessary - it writes some trailing headers without which
	// some gzip decoders work ($ gzip) and some don't (Golang's gzip)
	if err := gzipWriter.Close(); err != nil {
		return err
	}

	// also important because otherwise the seek pointer would be off for next read
	if err := resultingFile.Close(); err != nil {
		return err
	}

	// atomically rename the compressed & encrypted file to the final name
	// with which the whole stored block becomes valid
	if err := os.Rename(localPathTemp, localPath); err != nil {
		return err
	}

	return nil
}

// upload compressed&encrypted to S3. only done once
// (or in rare cases more if upload errors)
func (c *CompressedEncryptedStore) UploadToS3(cur *cursor.Cursor, s3Manager *scalablestore.S3Manager) error {
	localCompressedFile, openErr := os.Open(c.localPath(cur))
	if openErr != nil {
		return openErr
	}

	defer localCompressedFile.Close()

	if err := s3Manager.Put(cur.ToChunkPath(), localCompressedFile); err != nil {
		return err
	}

	return nil
}

func (c *CompressedEncryptedStore) DownloadFromS3(cur *cursor.Cursor, s3Manager *scalablestore.S3Manager) bool {
	fileKey := cur.ToChunkPath() // "/tenants/root/_/0.log"

	downloadStarted := time.Now()

	response, err := s3Manager.Get(fileKey)
	if err != nil { // FIXME: assuming 404, not any other error like network error..
		log.Printf("CompressedEncryptedStore: S3 get error: %s", err.Error())
		return false
	}

	localPath := c.localPath(cur)
	localPathTemp := localPath + ".tmp-froms3"

	localFileTemp, openErr := os.OpenFile(localPathTemp, os.O_RDWR|os.O_CREATE, 0755)
	if openErr != nil {
		panic(openErr)
	}

	defer localFileTemp.Close()

	if _, err := io.Copy(localFileTemp, response.Body); err != nil {
		panic(err)
	}

	if err := os.Rename(localPathTemp, localPath); err != nil {
		panic(err)
	}

	log.Printf("CompressedEncryptedStore: %s download & save took %s", cur.Serialize(), time.Since(downloadStarted))

	return true
}

// extracts compressed file first to temporary filename and then atomically moves it to SeekableStore
func (c *CompressedEncryptedStore) ExtractToSeekableStore(cur *cursor.Cursor, seekableStore *SeekableStore) bool {
	localCompressedFile, openErr := os.Open(c.localPath(cur))
	if openErr != nil {
		panic(openErr)
	}

	defer localCompressedFile.Close()

	decryptionAndExtractionStarted := time.Now()

	gzipReader, err := gzip.NewReader(localCompressedFile)
	if err != nil {
		panic(err)
	}

	localPath := c.localPath(cur)
	localPathTempForSeekable := localPath + ".tmp-toseekable"

	localTempFileForSeekable, openErr := os.OpenFile(localPathTempForSeekable, os.O_RDWR|os.O_CREATE, 0755)
	if openErr != nil {
		panic(openErr)
	}

	if _, err := io.Copy(localTempFileForSeekable, gzipReader); err != nil {
		panic(err)
	}

	localTempFileForSeekable.Close()

	seekableStore.SaveByRenaming(cur, localPathTempForSeekable)

	log.Printf("CompressedEncryptedStore: %s decrypt & extract took %s", cur.Serialize(), time.Since(decryptionAndExtractionStarted))

	return true
}

func (c *CompressedEncryptedStore) localPath(cur *cursor.Cursor) string {
	return fmt.Sprintf("%s/%s", config.COMPRESSED_ENCRYPTED_STORE_PATH, cur.ToChunkSafePath())
}
