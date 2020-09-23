// CLI for administering EventHorizon
package ehcli

import (
	"context"
	"log"

	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehclientfactory"
	"github.com/function61/eventhorizon/pkg/ehserver"
	"github.com/function61/eventhorizon/pkg/ehserver/ehdynamodb"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/os/osutil"
	"github.com/spf13/cobra"
)

func Entrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "eventhorizon",
		Short: "Manage EventHorizon",
	}

	parentCmd.AddCommand(serverEntrypoint())

	parentCmd.AddCommand(streamEntrypoint())

	parentCmd.AddCommand(snapshotEntrypoint())

	parentCmd.AddCommand(iamEntrypoint())

	parentCmd.AddCommand(realtimeEntrypoint())

	parentCmd.AddCommand(subscriptionsEntrypoint())

	return parentCmd
}

func serverEntrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "server",
		Short: "Server management",
	}

	parentCmd.AddCommand(&cobra.Command{
		Use:   "run",
		Short: "Run the server",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(ehserver.Server(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap the database",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(bootstrap(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				rootLogger))
		},
	})

	return parentCmd
}

func iamEntrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "iam",
		Short: "Identity & access management",
	}

	parentCmd.AddCommand(credentialsEntrypoint())
	parentCmd.AddCommand(policiesEntrypoint())

	return parentCmd
}

func bootstrap(ctx context.Context, logger *log.Logger) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	return ehdynamodb.Bootstrap(ctx, client.EventLog.(*ehdynamodb.Client))
}
