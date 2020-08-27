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

	systemClient, err := ehreader.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	bgCtx := context.Background() // don't have teardown mechanism available

	httpHandler, err := createHttpHandler(bgCtx, systemClient, func(task func(context.Context) error) {
		go func() {
			if err := task(bgCtx); err != nil {
				logex.Levels(logger).Error.Printf("mqtt task: %v", err)
			}
		}()
	}, logger)
	if err != nil {
		return err
	}

	lambda.StartHandler(lambdautils.NewMultiEventTypeHandler(func(ctx context.Context, ev interface{}) ([]byte, error) {
		switch e := ev.(type) {
		case *events.DynamoDBEvent:
			return nil, ehdynamodbtrigger.Handle(ctx, e, systemClient, logger)
		case *events.APIGatewayProxyRequest:
			return lambdautils.ServeApiGatewayProxyRequestUsingHttpHandler(
				ctx,
				e,
				httpHandler)
		case *events.CloudWatchEvent:
			return nil, nil // assume just a warm-up event
		default:
			return nil, errors.New("unsupported event")
		}

		return nil, nil
	}))

	// doesn't reach here
	return nil
}
