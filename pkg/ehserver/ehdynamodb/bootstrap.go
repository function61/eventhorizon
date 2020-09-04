package ehdynamodb

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/function61/eventhorizon/pkg/cryptosvc"
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
)

// creates core streams required for EventHorizon to work
func Bootstrap(ctx context.Context, e *Client) error {
	seqs := map[string]int64{}
	cur := func(stream eh.StreamName) eh.Cursor {
		curr := seqs[stream.String()] // zero value conveniently works
		seqs[stream.String()] = curr + 1
		return stream.At(curr + 1)
	}

	cryptoSvc := cryptosvc.New(nil)

	now := time.Now()

	txItems := []*dynamodb.TransactWriteItem{}
	for _, streamToCreate := range eh.InternalStreamsToCreate {
		dekEnvelope, err := cryptoSvc.NewAes256DekInEnvelope(
			ctx,
			streamToCreate.ResourceName())
		if err != nil {
			return err
		}

		txItem, err := e.entryAsTxPut(streamCreationEntry(streamToCreate, *dekEnvelope, now))
		if err != nil {
			return err
		}

		txItems = append(txItems, txItem)

		parent := streamToCreate.Parent()
		if parent != nil {
			notifyParent, err := e.entryAsTxPut(metaEntry(
				eh.NewStreamChildStreamCreated(streamToCreate.String(), ehevent.MetaSystemUser(now)),
				cur(*parent)))
			if err != nil {
				return err
			}

			txItems = append(txItems, notifyParent)
		}
	}

	_, err := e.dynamo.TransactWriteItemsWithContext(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: txItems,
	})
	if err != nil {
		// TODO: retry if TransactionCanceledException?
		//       OTOH, there is no traffic in the table by definition in the bootstrap phase..
		return err
	}

	return nil
}
