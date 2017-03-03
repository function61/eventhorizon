package store

import (
	"compress/gzip"
	"fmt"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/scalablestore"
	"io"
	"log"
	"os"
)

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

func (c *CompressedEncryptedStore) Has(cur *cursor.Cursor) bool {
	if _, err := os.Stat(c.localPath(cur)); os.IsNotExist(err) {
		return false
	}

	return true
}

func (c *CompressedEncryptedStore) DownloadFromS3(cur *cursor.Cursor, s3Manager *scalablestore.S3Manager) bool {
	fileKey := cur.ToChunkPath() // "/tenants/root/_/0.log"

	response, err := s3Manager.Get(fileKey)
	if err != nil { // FIXME: assuming 404, not any other error like network error..
		return false
	}

	localPath := c.localPath(cur)
	localPathTemp := localPath + ".tmp"

	localFile, openErr := os.OpenFile(localPathTemp, os.O_RDWR|os.O_CREATE, 0755)
	if openErr != nil {
		panic(openErr)
	}

	defer localFile.Close()

	if _, err := io.Copy(localFile, response.Body); err != nil {
		panic(err)
	}

	if err := os.Rename(localPathTemp, localPath); err != nil {
		panic(err)
	}

	return true
}

// extracts compressed file first to temporary filename and then atomically moves it to SeekableStore
func (c *CompressedEncryptedStore) ExtractToSeekableStore(cur *cursor.Cursor, seekableStore *SeekableStore) bool {
	localCompressedFile, openErr := os.Open(c.localPath(cur))
	if openErr != nil {
		panic(openErr)
	}

	defer localCompressedFile.Close()

	gzipReader, err := gzip.NewReader(localCompressedFile)
	if err != nil {
		panic(err)
	}

	localPath := c.localPath(cur)
	localPathTempForSeekable := localPath + ".tmp-seekable"

	localTempFileForSeekable, openErr := os.OpenFile(localPathTempForSeekable, os.O_RDWR|os.O_CREATE, 0755)
	if openErr != nil {
		panic(openErr)
	}

	if _, err := io.Copy(localTempFileForSeekable, gzipReader); err != nil {
		panic(err)
	}

	localTempFileForSeekable.Close()

	seekableStore.SaveByRenaming(cur, localPathTempForSeekable)

	return true
}

func (c *CompressedEncryptedStore) localPath(cur *cursor.Cursor) string {
	return fmt.Sprintf("%s/%s", config.COMPRESSED_ENCRYPTED_STORE_PATH, cur.ToChunkSafePath())
}
