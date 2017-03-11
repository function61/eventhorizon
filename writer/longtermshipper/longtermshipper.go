package longtermshipper

import (
	"github.com/function61/pyramid/reader/store"
	"github.com/function61/pyramid/scalablestore"
	wtypes "github.com/function61/pyramid/writer/types"
	"io"
	"log"
	"sync"
	"time"
)

// longtermshipper is responsible for orchestrating compression, encryption and
// uploading processing of sealed blocks.

func shipOne(ltsf *wtypes.LongTermShippableFile, compEnc *store.CompressedEncryptedStore, s3Manager *scalablestore.S3Manager, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	started := time.Now()

	log.Printf("LongTermShipperManager: compressing %s", ltsf.Block.ToChunkPath())

	// this is probably required because the position is wherever WAL writer left it
	// TODO: have WAL writer just close the file (= seek not required) and pass filename?
	if _, err := ltsf.Fd.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}

	if err := compEnc.SaveFromLiveFile(ltsf.Block, ltsf.Fd); err != nil {
		panic(err)
	}

	if err := compEnc.UploadToS3(ltsf.Block, s3Manager); err != nil {
		panic(err)
	}

	log.Printf("LongTermShipperManager: completed %s in %s", ltsf.Block.ToChunkPath(), time.Since(started))

	// TODO: rename from store:live to store:seekable

	if err := ltsf.Fd.Close(); err != nil {
		panic(err) // TODO: not probably worth panic()ing for
	}
}

func RunManager(work chan *wtypes.LongTermShippableFile, done chan bool) {
	log.Printf("LongTermShipperManager: Started")

	compEnc := store.NewCompressedEncryptedStore()

	s3Manager := scalablestore.NewS3Manager()

	wg := &sync.WaitGroup{}

	for ltsf := range work {
		// TODO: do this transactionally
		go shipOne(ltsf, compEnc, s3Manager, wg)
	}

	log.Printf("LongTermShipperManager: stopping; waiting for WaitGroup")

	wg.Wait()

	log.Printf("LongTermShipperManager: WaitGroup done")

	done <- true
}
