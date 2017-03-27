package longtermshipper

import (
	"errors"
	"github.com/boltdb/bolt"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/cursor"
	"github.com/function61/pyramid/reader/store"
	"github.com/function61/pyramid/scalablestore"
	"github.com/function61/pyramid/writer/transaction"
	wtypes "github.com/function61/pyramid/writer/types"
	"log"
	"os"
	"sync"
	"time"
)

/* longtermshipper is responsible for orchestrating compression, encryption and
   uploading processing of sealed blocks in a durable manner.

	Flow per shippable file:

	1) Writer calls MarkFileToBeShipped() which:
	  - writes shipping metadata to database so failed shippings / crashes
	    can still guarantee that the block will get uploaded
	  - attaches the shippable file to the transaction metadata so it can
	    be executed in the side effects after transaction is committed

	2) Writer's side effect processor calls Ship() with every shippable file.

	3) Upon block shipment success the entry is removed from database.


	Reliability: on Writer startup it calls RecoverUnfinishedShipments(), after which
	the side effects processor calls Ship() if any files-to-recover were detected.

*/

type Shipper struct {
	shipmentOperationsDone   *sync.WaitGroup
	compressedEncryptedStore *store.CompressedEncryptedStore
	s3Manager                *scalablestore.S3Manager
}

func New(confCtx *config.Context) *Shipper {
	return &Shipper{
		shipmentOperationsDone:   &sync.WaitGroup{},
		compressedEncryptedStore: store.NewCompressedEncryptedStore(confCtx),
		s3Manager:                scalablestore.NewS3Manager(confCtx),
	}
}

func (s *Shipper) MarkFileToBeShipped(fileToShip *wtypes.LongTermShippableFile, tx *transaction.EventstoreTransaction) error {
	filesToShipBucket, createBucketErr := tx.BoltTx.CreateBucketIfNotExists([]byte("_filestoship"))
	if createBucketErr != nil {
		return createBucketErr
	}

	filesToShipBucket.Put([]byte(fileToShip.Block.Serialize()), []byte(fileToShip.FilePath))

	tx.ShipFiles = append(tx.ShipFiles, fileToShip)

	return nil
}

// need DB reference so we can start transaction once the process finishes
func (s *Shipper) Ship(ltsf *wtypes.LongTermShippableFile, database *bolt.DB) {
	// don't return from Close() until all shipments finish
	s.shipmentOperationsDone.Add(1)

	go func() {
		defer s.shipmentOperationsDone.Done()

		if err := s.shipOne(ltsf); err != nil {
			log.Printf("Shipper: error %s", err.Error())
			return
		}

		// ok shipping succeeded, so delete entry from database
		// so it won't be re-tried
		if err := database.Update(func(boltTx *bolt.Tx) error {
			filesToShipBucket := boltTx.Bucket([]byte("_filestoship"))
			if filesToShipBucket == nil {
				return errors.New("Unable to get _filestoship bucket")
			}

			return filesToShipBucket.Delete([]byte(ltsf.Block.Serialize()))
		}); err != nil {
			panic(err)
		}
	}()
}

func (s *Shipper) shipOne(ltsf *wtypes.LongTermShippableFile) error {
	started := time.Now()

	log.Printf("Shipper: compressing & encrypting %s", ltsf.Block.ToChunkPath())

	fd, err := os.Open(ltsf.FilePath)
	if err != nil {
		return err
	}
	defer fd.Close()

	if err := s.compressedEncryptedStore.SaveFromLiveFile(ltsf.Block, fd); err != nil {
		return err
	}

	if err := s.compressedEncryptedStore.UploadToS3(ltsf.Block, s.s3Manager); err != nil {
		return err
	}

	log.Printf("Shipper: completed %s in %s", ltsf.Block.ToChunkPath(), time.Since(started))

	return nil
}

func (s *Shipper) RecoverUnfinishedShipments(tx *transaction.EventstoreTransaction) error {
	filesToShipBucket, createBucketErr := tx.BoltTx.CreateBucketIfNotExists([]byte("_filestoship"))
	if createBucketErr != nil {
		return createBucketErr
	}

	// do essentially the same as MarkFileToBeSent() for each recovered file,
	// except for the fact that we have to deserialize this and not save it to DB
	err := filesToShipBucket.ForEach(func(key, value []byte) error {
		ltsf := &wtypes.LongTermShippableFile{
			Block:    cursor.CursorFromserializedMust(string(key)),
			FilePath: string(value),
		}

		log.Printf("Shipper: recovering unshipped file %s", ltsf.FilePath)

		tx.ShipFiles = append(tx.ShipFiles, ltsf)

		return nil
	})
	if err != nil {
		panic(err)
	}

	return nil
}

func (s *Shipper) Close() {
	log.Printf("Shipper: stopping")

	s.shipmentOperationsDone.Wait()

	log.Printf("Shipper: stopped")
}
