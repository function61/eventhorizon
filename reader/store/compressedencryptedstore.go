package store

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
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

	Encrypted file format:

		Header
			Magic bytes
			IV (AES block size)
		Body
			AES
				Gzip
					Content

	NOTES:

	- AES is outer layer so we have to encrypt/decrypt less bytes

	Which mode of operation to use
	------------------------------

	http://stackoverflow.com/a/22958889
	http://crypto.stackexchange.com/questions/18538/aes256-cbc-vs-aes256-ctr-in-ssh

	CTR (stream mode) was chosen because:
	- it does not require padding (no padding oracle attack)
	- it's parallelizable
	- transmission errors do not snowball


	Which bit size to use?
	----------------------

	Users of AES-256:

	- AWS S3
	- Truecrypt/Veracrypt
	- Keepass
	- Included in TLS best practices (though also recommends 128bit version)
		- https://github.com/ssllabs/research/wiki/SSL-and-TLS-Deployment-Best-Practices

	https://security.stackexchange.com/questions/14068/why-most-people-use-256-bit-encryption-instead-of-128-bit

	=> will use AES-256
*/

var (
	headerMagicBytes = []byte("Pyramid-1/AES256_CTR")
)

type CompressedEncryptedStore struct {
	confCtx *config.Context
}

func NewCompressedEncryptedStore(confCtx *config.Context) *CompressedEncryptedStore {
	if _, err := os.Stat(config.CompressedEncryptedStorePath); os.IsNotExist(err) {
		log.Printf("CompressedEncryptedStore: mkdir %s", config.CompressedEncryptedStorePath)

		if err = os.MkdirAll(config.CompressedEncryptedStorePath, 0755); err != nil {
			panic(err)
		}
	}

	return &CompressedEncryptedStore{confCtx}
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

	// truncates if exists (ok because temp file => undefined state)
	resultingFile, err := os.Create(localPathTemp)
	if err != nil {
		return err
	}

	iv := generateRandomAesIv()

	// write header section (magic bytes + IV)
	if _, err := resultingFile.Write(headerMagicBytes); err != nil {
		panic(err)
	}
	if _, err := resultingFile.Write(iv); err != nil {
		panic(err)
	}

	// use the resulting file as a sink for AES stream
	aesWriter := createAesCtrWriterPipe(c.confCtx.GetStreamEncryptionKey(), iv, resultingFile)

	// use AES writer as a sink for gzip stream
	gzipWriter := gzip.NewWriter(aesWriter)

	// pump the original file through the pipeline: gzip -> AES -> result
	if _, err := io.Copy(gzipWriter, fromFd); err != nil {
		return err
	}

	// this is super necessary - it writes some trailing headers without which
	// some gzip decoders work ($ gzip) and some don't (Golang's gzip)
	if err := gzipWriter.Close(); err != nil {
		return err
	}

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

	localPath := c.localPath(cur)
	localPathTempForSeekable := localPath + ".tmp-toseekable"

	// headerPlusIV := [len(headerMagicBytes) + aes.BlockSize]byte{}
	headerPlusIV := make([]byte, len(headerMagicBytes)+aes.BlockSize)
	_, err := io.ReadFull(localCompressedFile, headerPlusIV)
	if err != nil {
		panic(err)
	}

	// verify header
	if !bytes.Equal(headerPlusIV[0:len(headerMagicBytes)], headerMagicBytes) {
		panic("incorrect header")
	}

	// extract IV
	iv := headerPlusIV[len(headerMagicBytes):]

	// truncates if exists (ok because temp file => undefined state)
	localTempFileForSeekable, openErr := os.Create(localPathTempForSeekable)
	if openErr != nil {
		panic(openErr)
	}

	// feed AES stream from the file
	aesReader := createAesCtrReaderPipe(
		c.confCtx.GetStreamEncryptionKey(),
		iv,
		localCompressedFile)

	// feed Gzip decoder stream from AES stream
	gzipReader, err := gzip.NewReader(aesReader)
	if err != nil {
		panic(err)
	}

	// feed seekable file from uncompressed & decrypted stream
	if _, err := io.Copy(localTempFileForSeekable, gzipReader); err != nil {
		panic(err)
	}

	localTempFileForSeekable.Close()

	seekableStore.SaveByRenaming(cur, localPathTempForSeekable)

	log.Printf("CompressedEncryptedStore: %s decrypt & extract took %s", cur.Serialize(), time.Since(decryptionAndExtractionStarted))

	return true
}

func (c *CompressedEncryptedStore) localPath(cur *cursor.Cursor) string {
	return fmt.Sprintf("%s/%s", config.CompressedEncryptedStorePath, cur.ToChunkSafePath())
}

func createAesCtrReaderPipe(encryptionKey []byte, iv []byte, input io.Reader) io.Reader {
	aesCipher, err := aes.NewCipher(encryptionKey)
	if err != nil {
		panic(err)
	}

	decrypterReaderWrapper := &cipher.StreamReader{
		S: cipher.NewCTR(aesCipher, iv),
		R: input,
	}

	return decrypterReaderWrapper
}

func createAesCtrWriterPipe(encryptionKey []byte, iv []byte, writer io.Writer) io.Writer {
	aesCipher, err := aes.NewCipher(encryptionKey)
	if err != nil {
		panic(err)
	}

	encrypterWriterWrapper := &cipher.StreamWriter{
		S: cipher.NewCTR(aesCipher, iv),
		W: writer,
	}

	return encrypterWriterWrapper
}

func generateRandomAesIv() []byte {
	// IV size must equal block size
	iv := make([]byte, aes.BlockSize)

	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		panic(err)
	}

	return iv
}
