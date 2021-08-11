// handles DynamoDB trigger with inserted events by feeding it to ehsubscriptionactivity task
package ehdynamodbtrigger

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehserver/ehsubscriptionactivity"
)

func Handle(
	ctx context.Context,
	event *events.DynamoDBEvent,
	client *ehclient.SystemClient,
	logger *log.Logger,
) error {
	discovered, err := changedStreamsFromDynamoDbEvent(event, logger)
	if err != nil {
		return err
	}

	return ehsubscriptionactivity.PublishStreamChangesToSubscribers(
		ctx,
		discovered,
		client,
		logger)
}

func changedStreamsFromDynamoDbEvent(
	event *events.DynamoDBEvent,
	logger *log.Logger,
) (*ehsubscriptionactivity.DiscoveredMaxCursors, error) {
	discovered := ehsubscriptionactivity.NewDiscoveredMaxCursors()

	for _, record := range event.Records {
		// we're only expecting INSERT, because:
		// - MODIFY doesn't happen for keys (we're subscribing to key-only stream)
		// - REMOVE is not supported to be used with eventsourcing
		if record.EventName != "INSERT" {
			logger.Printf("unsupported event: %s", record.EventName)
			continue
		}

		// TODO: would like to unmarshal to LogEntryRaw, but events package has different
		// structs than dynamodb package

		version, err := record.Change.Keys["v"].Integer()
		if err != nil {
			return nil, err
		}

		stream, err := eh.DeserializeStreamName(record.Change.Keys["s"].String())
		if err != nil {
			return nil, err
		}

		discovered.Add(stream.At(version))
	}

	return discovered, nil
}
