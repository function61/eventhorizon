package writer

import (
	"compress/gzip"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/scalablestore"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// TODO: implement encryption
/*	Summary:

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
*/
// http://crypto.stackexchange.com/questions/225/should-i-use-ecb-or-cbc-encryption-mode-for-my-block-cipher
// http://crypto.stackexchange.com/questions/2476/cipher-feedback-mode
// http://stackoverflow.com/questions/32329512/golang-file-encryption-with-crypto-aes-lib

type LongTermShippableFile struct {
	chunkName string // '/tenants/foo/_/28.log'
	fd        *os.File
}

func LongTermShipperWorker(ltsf *LongTermShippableFile, s3Manager *scalablestore.S3Manager, wg *sync.WaitGroup) {
	defer wg.Done()

	started := time.Now()

	safeName := strings.Replace(ltsf.chunkName, "/", "_", -1)

	storageFullPath := config.LONGTERMSHIPPER_PATH + "/" + safeName

	log.Printf("LongTermShipperManager: compressing %s -> %s", ltsf.chunkName, ltsf.chunkName) // storageFullPath)

	storageFile, err := os.OpenFile(storageFullPath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}

	/*	Build an in-memory streaming pipeline:

		+-----------------+
		|                 |
		|      File       |
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
		|       S3        |
		|                 |
		+-----------------+

	*/
	// TODO: is this required? probably
	if _, err := ltsf.fd.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}

	// gzipPipeReader, gzipPipeWriter := io.Pipe()
	gzipWriter := gzip.NewWriter(storageFile)

	if _, err := io.Copy(gzipWriter, ltsf.fd); err != nil {
		panic(err)
	}

	// this is super necessary
	gzipWriter.Close()

	// TODO: is this needed?
	if _, err := storageFile.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}

	fileKey := ltsf.chunkName

	if err := s3Manager.Put(fileKey, storageFile); err != nil {
		panic(err)
	}

	if err := storageFile.Close(); err != nil {
		panic(err)
	}

	/*
		go func() {
			if _, err := io.Copy(gzipWriter, ltsf.fd); err != nil {
				panic(err)
			}

			if err := gzipWriter.Close(); err != nil {
				panic(err)
			}
		}()

		fileKey := ltsf.chunkName

		_, err2 := s3Manager.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: &s3Manager.bucketName,
			Key:    &fileKey,
			Body:   gzipPipeReader,
		})
		if err2 != nil {
			panic(err2)
		}
	*/

	// read from original file and pipe to gzip encoder (which in turn is piped to the output file)
	/*
		if _, err = io.Copy(gzipWriter, ltsf.fd); err != nil {
			panic(err)
		}
	*/

	log.Printf("LongTermShipperManager: completed %s in %s", ltsf.chunkName, time.Since(started))

	if err := ltsf.fd.Close(); err != nil {
		panic(err)
	}
}

func LongTermShipperManager(work chan *LongTermShippableFile, done chan bool) {
	// TODO: $ mkdir /longtermstorage

	log.Printf("LongTermShipperManager: Started")

	LongTermShipperManagerEnsureDirectoryExists()

	s3Manager := scalablestore.NewS3Manager()

	wg := &sync.WaitGroup{}

	for ltsf := range work {
		wg.Add(1)

		go LongTermShipperWorker(ltsf, s3Manager, wg)
	}

	log.Printf("LongTermShipperManager: stopping; waiting for WaitGroup")

	wg.Wait()

	log.Printf("LongTermShipperManager: WaitGroup done")

	done <- true
}

func LongTermShipperManagerEnsureDirectoryExists() {
	if _, err := os.Stat(config.LONGTERMSHIPPER_PATH); os.IsNotExist(err) {
		log.Printf("LongTermShipperManager: mkdir %s", config.LONGTERMSHIPPER_PATH)

		if err = os.MkdirAll(config.LONGTERMSHIPPER_PATH, 0755); err != nil {
			panic(err)
		}
	}
}
