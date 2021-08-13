package ehcli

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/function61/eventhorizon/pkg/system/ehsettings"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/os/osutil"
	"github.com/function61/gokit/time/timeutil"
	"github.com/scylladb/termtables"
	"github.com/spf13/cobra"
)

func keyServerEntrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "keyserver",
		Short: "Key server management",
	}

	parentCmd.AddCommand(&cobra.Command{
		Use:   "ls",
		Short: "List keyservers",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(keyServerList(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				rootLogger))
		},
	})

	return parentCmd
}

func keyServerList(ctx context.Context, logger *log.Logger) error {
	settings, _, err := loadSettings(ctx, logger)
	if err != nil {
		return err
	}

	keks := settings.State.KEKs()

	findKek := func(id string) *ehsettings.Kek {
		for _, kek := range keks {
			if kek.Id == id {
				return &kek
			}
		}

		return nil
	}

	view := termtables.CreateTable()
	view.AddHeaders("Id", "Created", "Label", "KEKs")

	for _, keyServer := range settings.State.KeyServers() {
		kekNames := []string{}
		for _, kekId := range keyServer.AttachedKekIds {
			kek := findKek(kekId)
			if kek == nil {
				return fmt.Errorf("KEK not found: %s", kekId)
			}

			kekNames = append(kekNames, kek.Label)
		}

		view.AddRow(
			keyServer.Id,
			timeutil.HumanizeDuration(time.Since(keyServer.Created)),
			keyServer.Label,
			strings.Join(kekNames, ", "),
		)
	}

	fmt.Println(view.Render())

	return nil
}
