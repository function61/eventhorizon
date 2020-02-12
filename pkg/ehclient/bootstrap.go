package ehclient

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"time"
)

// creates the root ("/") stream and "/_sub" sub-stream
func Bootstrap(ctx context.Context, e *Client) error {
	root := "/"
	subs := "/_sub"

	now := time.Now()

	createRoot, err := e.entryAsTxPut(streamCreationEntry(root, "", now))
	if err != nil {
		return err
	}

	createSubs, err := e.entryAsTxPut(streamCreationEntry(subs, root, now))
	if err != nil {
		return err
	}

	notifyRoot, err := e.entryAsTxPut(metaEntry(NewChildStreamCreated(subs, ehevent.MetaSystemUser(now)), At(root, 1)))
	if err != nil {
		return err
	}

	_, err = e.dynamo.TransactWriteItemsWithContext(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []*dynamodb.TransactWriteItem{
			createRoot,
			createSubs,
			notifyRoot,
		},
	})
	if err != nil {
		// TODO: retry if TransactionCanceledException?
		//       OTOH, there is no traffic in the table by definition in the bootstrap phase..
		return err
	}

	return nil
}
