package ehserver

import (
	"context"
	"errors"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehclientfactory"
	"github.com/function61/eventhorizon/pkg/ehserver/ehdynamodb/ehdynamodbtrigger"
	"github.com/function61/gokit/app/aws/lambdautils"
	"github.com/function61/gokit/log/logex"
)

// starts Lambda handler for handling EventHorizon HTTP API or DynamoDB triggers
func LambdaEntrypoint() error {
	logger := logex.StandardLogger()

	systemClient, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	bgCtx := context.Background() // don't have teardown mechanism available

	httpHandler, notifier, err := createHttpHandler(bgCtx, systemClient, func(task func(context.Context) error) {
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
			resp, err := lambdautils.ServeApiGatewayProxyRequestUsingHttpHandler(
				ctx,
				e,
				httpHandler)

			// Lambda seems to pause the process immediately after we return (TODO: citation),
			// so wait here that the async publishes to realtime backend were sent
			if notifier != nil {
				notifier.WaitInFlight()
			}

			return resp, err
		case *events.CloudWatchEvent:
			return nil, nil // assume just a warm-up event
		default:
			return nil, errors.New("unsupported event")
		}
	}))

	// doesn't reach here
	return nil
}
