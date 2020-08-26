package ehcli

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/eventhorizon/pkg/system/ehstreamsubscribers"
	"github.com/function61/eventhorizon/pkg/system/ehsubscription"
	"github.com/function61/gokit/logex"
	"github.com/function61/gokit/osutil"
	"github.com/spf13/cobra"
)

func subscriptionsEntrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "sub",
		Short: "Subscriptions management",
	}

	parentCmd.AddCommand(&cobra.Command{
		Use:   "ls [stream]",
		Short: "List subscriptions for a stream",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(subscriptionsList(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "mk [stream] [id]",
		Short: "Subscribe to a stream",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(subscriptionSubscribe(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				args[1],
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "rm [stream] [id]",
		Short: "Unsubscribe from a stream",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(subscriptionUnsubscribe(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				args[1],
				rootLogger))
		},
	})

	return parentCmd
}

func subscriptionsList(ctx context.Context, streamNameRaw string, logger *log.Logger) error {
	client, err := ehreader.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	_, subscriptions, err := loadSubscriptions(ctx, streamNameRaw, client, logger)
	if err != nil {
		return err
	}

	for _, subscription := range subscriptions.State.Subscriptions() {
		fmt.Println(subscription)
	}

	return nil
}

func subscriptionSubscribe(ctx context.Context, streamNameRaw string, idRaw string, logger *log.Logger) error {
	id := eh.NewSubscriptionId(idRaw)

	client, err := ehreader.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	stream, subscriptions, err := loadSubscriptions(ctx, streamNameRaw, client, logger)
	if err != nil {
		return err
	}

	// validate existence
	if _, err := ehsubscription.LoadUntilRealtime(ctx, id, client, nil, logger); err != nil {
		return err
	}

	return subscriptions.Reader.TransactWrite(ctx, func() error {
		if subscriptions.State.Subscribed(id) {
			return fmt.Errorf("%s is already subscribed to %s", id.String(), stream.String())
		}

		subscribed := eh.NewSubscriptionSubscribed(
			id,
			ehevent.MetaSystemUser(time.Now()))

		_, err := subscriptions.Writer.AppendAfter(
			ctx,
			subscriptions.State.Version(),
			ehevent.Serialize(subscribed))
		return err
	})
}

func subscriptionUnsubscribe(ctx context.Context, streamNameRaw string, idRaw string, logger *log.Logger) error {
	id := eh.NewSubscriptionId(idRaw)

	client, err := ehreader.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	stream, subscriptions, err := loadSubscriptions(ctx, streamNameRaw, client, logger)
	if err != nil {
		return err
	}

	// validate existence
	if _, err := ehsubscription.LoadUntilRealtime(ctx, id, client, nil, logger); err != nil {
		return err
	}

	return subscriptions.Reader.TransactWrite(ctx, func() error {
		if !subscriptions.State.Subscribed(id) {
			return fmt.Errorf("%s is not subscribed to %s", id, stream.String())
		}

		unsubscribed := eh.NewSubscriptionUnsubscribed(
			id,
			ehevent.MetaSystemUser(time.Now()))

		_, err := subscriptions.Writer.AppendAfter(
			ctx,
			subscriptions.State.Version(),
			ehevent.Serialize(unsubscribed))
		return err
	})
}

func loadSubscriptions(
	ctx context.Context,
	streamNameRaw string,
	client *ehreader.SystemClient,
	logger *log.Logger,
) (eh.StreamName, *ehstreamsubscribers.App, error) {
	stream, err := eh.DeserializeStreamName(streamNameRaw)
	if err != nil {
		return stream, nil, err
	}

	subscriptions, err := ehstreamsubscribers.LoadUntilRealtime(
		ctx,
		stream,
		client,
		logex.Prefix(ehstreamsubscribers.LogPrefix, logger))
	return stream, subscriptions, err
}
