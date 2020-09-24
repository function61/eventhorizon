package ehcli

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehclientfactory"
	"github.com/function61/eventhorizon/pkg/system/ehsettings"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/os/osutil"
	"github.com/function61/gokit/time/timeutil"
	"github.com/scylladb/termtables"
	"github.com/spf13/cobra"
)

func kekEntrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "kek",
		Short: "Key Encryption Keys management",
	}

	parentCmd.AddCommand(&cobra.Command{
		Use:   "ls",
		Short: "List KEKs",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(kekList(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				rootLogger))
		},
	})

	return parentCmd
}

func kekList(ctx context.Context, logger *log.Logger) error {
	settings, _, err := loadSettings(ctx, logger)
	if err != nil {
		return err
	}

	view := termtables.CreateTable()
	view.AddHeaders("Id", "Kind", "Registered", "Label")

	for _, kek := range settings.State.Keks() {
		view.AddRow(
			kek.Id,
			kek.Kind,
			timeutil.HumanizeDuration(time.Since(kek.Registered)),
			kek.Label,
		)
	}

	fmt.Println(view.Render())

	return nil
}

func loadSettings(ctx context.Context, logger *log.Logger) (*ehsettings.App, *ehclient.SystemClient, error) {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return nil, nil, err
	}

	settings, err := ehsettings.LoadUntilRealtime(ctx, client)
	if err != nil {
		return nil, nil, err
	}

	return settings, client, nil
}
