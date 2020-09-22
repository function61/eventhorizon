package ehdynamodb

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/eheventencryption"
	"github.com/function61/eventhorizon/pkg/keyserver"
	"github.com/function61/eventhorizon/pkg/system/ehsettingsdomain"
	"github.com/function61/gokit/crypto/cryptoutil"
	"github.com/function61/gokit/crypto/envelopeenc"
)

// creates core streams required for EventHorizon to work
func Bootstrap(ctx context.Context, e *Client) error {
	seqs := map[string]int64{}
	cur := func(stream eh.StreamName) eh.Cursor {
		curr := seqs[stream.String()] // zero value conveniently works
		seqs[stream.String()] = curr + 1
		return stream.At(curr + 1)
	}

	clusterWideKey, err := newClusterWideKey()
	if err != nil {
		return err
	}

	// generate with:
	// $ ssh-keygen -f default.key -m PEM -t rsa -b 4096
	defaultPubPem, defaultPub, err := loadPublicKeyFromPrivateKey("default.key")
	if err != nil {
		return err
	}

	backupPubPem, backupPub, err := loadPublicKeyFromPrivateKey("backup.key")
	if err != nil {
		return err
	}

	cwkEncrypter := envelopeenc.NaclSecretBoxEncrypter(clusterWideKey, "cwk")

	now := time.Now()
	meta := ehevent.MetaSystemUser(now)

	defaultKey := envelopeenc.RsaOaepSha256Encrypter(defaultPub)
	backupKey := envelopeenc.RsaOaepSha256Encrypter(backupPub)

	defGroup := ehsettingsdomain.NewKeygroupCreated("default", []string{defaultKey.KekId(), backupKey.KekId()}, "[internal]", meta)

	keyServer := ehsettingsdomain.NewKeyserverCreated("internal", "Internal", meta)

	pubAdded := ehsettingsdomain.NewKekAdded(defaultKey.KekId(), "rsa", "Default key", defaultPubPem, meta)

	settingsEvents := []ehevent.Event{
		defGroup,
		pubAdded,
		ehsettingsdomain.NewKekAdded(backupKey.KekId(), "rsa", "Backup key", backupPubPem, meta),
		keyServer,
		ehsettingsdomain.NewKeyserverKeyAttached(keyServer.Id, pubAdded.Id, meta),
	}

	defaultGroupEncrypters := []envelopeenc.SlotEncrypter{
		defaultKey,
		backupKey,
	}

	txItems := []*dynamodb.TransactWriteItem{}
	for _, streamToCreate := range eh.InternalStreamsToCreate {
		dek, err := keyserver.NewDek()
		if err != nil {
			return err
		}

		creatingSysSettings := streamToCreate.Equal(eh.SysSettings)

		// TODO: make DEK envelope locally only for streams where we need to add encrypted data for
		dekEnvelope, err := func() (*envelopeenc.Envelope, error) {
			if !creatingSysSettings {
				return keyserver.MakeDekEnvelope(
					dek,
					streamToCreate.ResourceName(),
					defaultGroupEncrypters)
			} else {
				// TODO: does the append have side effects?
				return keyserver.MakeDekEnvelope(
					dek,
					streamToCreate.ResourceName(),
					append(defaultGroupEncrypters, cwkEncrypter))
			}
		}()
		if err != nil {
			return err
		}

		txItem, err := e.entryAsTxPut(streamCreationEntry(streamToCreate, *dekEnvelope, defGroup.Id, now))
		if err != nil {
			return err
		}

		txItems = append(txItems, txItem)

		if creatingSysSettings {
			entry, err := eheventencryption.Encrypt(ehevent.Serialize(settingsEvents...), dek)
			if err != nil {
				return err
			}

			txItem, err := e.entryAsTxPut(mkLogEntryRaw(cur(streamToCreate), *entry))
			if err != nil {
				return err
			}

			txItems = append(txItems, txItem)
		}

		parent := streamToCreate.Parent()
		if parent != nil {
			notifyParent, err := e.entryAsTxPut(metaEntry(
				eh.NewStreamChildStreamCreated(streamToCreate, meta),
				cur(*parent)))
			if err != nil {
				return err
			}

			txItems = append(txItems, notifyParent)
		}
	}

	_, err = e.dynamo.TransactWriteItemsWithContext(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: txItems,
	})
	if err != nil {
		// TODO: retry if TransactionCanceledException?
		//       OTOH, there is no traffic in the table by definition in the bootstrap phase..
		return err
	}

	fmt.Printf("Cluster-wide key: %s\n", base64.RawURLEncoding.EncodeToString(clusterWideKey[:]))

	return nil
}

// to read & decrypt data in EventHorizon cluster, you need to know the cluster settings.
// but like all streams, the cluster settings stream is encrypted. this key is used to
// bootstrap knowledge for reading data from the cluster
func newClusterWideKey() ([32]byte, error) {
	var key [32]byte
	_, err := rand.Read(key[:])
	return key, err
}

func loadPublicKeyFromPrivateKey(filename string) (string, *rsa.PublicKey, error) {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", nil, err
	}
	privKey, err := cryptoutil.ParsePemPkcs1EncodedRsaPrivateKey(bytes)
	if err != nil {
		return "", nil, err
	}

	return string(cryptoutil.MarshalPemPkcs1EncodedRsaPublicKey(&privKey.PublicKey)), &privKey.PublicKey, nil
}
