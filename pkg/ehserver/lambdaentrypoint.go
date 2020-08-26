package ehserver

import (
	"context"
	"errors"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/eventhorizon/pkg/ehserver/ehdynamodb/ehdynamodbtrigger"
	"github.com/function61/gokit/aws/lambdautils"
	"github.com/function61/gokit/logex"
)

// starts Lambda handler for handling EventHorizon HTTP API or DynamoDB triggers
func LambdaEntrypoint() error {
	logger := logex.StandardLogger()

	client, err := ehreader.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	lambda.StartHandler(lambdautils.NewMultiEventTypeHandler(func(ctx context.Context, ev interface{}) ([]byte, error) {
		switch e := ev.(type) {
		case *events.DynamoDBEvent:
			return nil, ehdynamodbtrigger.Handle(ctx, e, client, logger)
		default:
			return nil, errors.New("unsupported event")
		}

		return nil, nil
	}))

	// doesn't reach here
	return nil
}
