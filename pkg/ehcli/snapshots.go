package ehcli

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehclientfactory"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/os/osutil"
	"github.com/spf13/cobra"
)

func snapshotEntrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "snap",
		Short: "Snapshot management",
	}

	parentCmd.AddCommand(&cobra.Command{
		Use:   "cat [streamName] [context]",
		Short: "Inspect a snapshot",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(snapshotCat(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				args[1],
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "rm [streamName] [context]",
		Short: "Delete a snapshot",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(snapshotRm(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				args[1],
				rootLogger))
		},
	})

	encrypted := false

	snapshotPutCmd := &cobra.Command{
		Use:   "put [cursor] [context]",
		Short: "Replace a snapshot (dangerous)",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(snapshotPut(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				args[1],
				os.Stdin,
				encrypted,
				rootLogger))
		},
	}
	snapshotPutCmd.Flags().BoolVarP(&encrypted, "encrypted", "", encrypted, "Whether to encrypt the snapshot")
	parentCmd.AddCommand(snapshotPutCmd)

	return parentCmd
}

func snapshotCat(
	ctx context.Context,
	streamNameRaw string,
	perspective string,
	logger *log.Logger,
) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	streamName, err := eh.DeserializeStreamName(streamNameRaw)
	if err != nil {
		return err
	}

	// the Beginning() is non-important, as the store only uses the stream component of Cursor
	snapPersisted, err := client.SnapshotStore.ReadSnapshot(
		ctx,
		streamName,
		eh.ParseSnapshotPerspective(perspective))
	if err != nil {
		return err
	}

	fmt.Fprintf(
		os.Stderr,
		"Snapshot (kind=%s) @ %s\n--------\n",
		snapPersisted.Kind().String(),
		snapPersisted.Cursor.Serialize())

	snap, err := snapPersisted.DecryptIfRequired(func() ([]byte, error) {
		return client.LoadDEKv0(ctx, snapPersisted.Cursor.Stream())
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(
		os.Stdout,
		"%s",
		snap.Data)

	return nil
}

func snapshotPut(
	ctx context.Context,
	cursorSerialized string,
	perspective string,
	contentReader io.Reader,
	encrypted bool,
	logger *log.Logger,
) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	cursor, err := eh.DeserializeCursor(cursorSerialized)
	if err != nil {
		return err
	}

	content, err := ioutil.ReadAll(contentReader)
	if err != nil {
		return err
	}

	snapshot := eh.NewSnapshot(cursor, content, eh.ParseSnapshotPerspective(perspective))

	persisted, err := func() (*eh.PersistedSnapshot, error) {
		if encrypted {
			dek, err := client.LoadDEKv0(ctx, cursor.Stream())
			if err != nil {
				return nil, err
			}

			return snapshot.Encrypted(dek)
		} else {
			return snapshot.Unencrypted(), nil
		}
	}()
	if err != nil {
		return err
	}

	return client.SnapshotStore.WriteSnapshot(ctx, *persisted)
}

func snapshotRm(
	ctx context.Context,
	streamName string,
	perspective string,
	logger *log.Logger,
) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	stream, err := eh.DeserializeStreamName(streamName)
	if err != nil {
		return err
	}

	return client.SnapshotStore.DeleteSnapshot(ctx, stream, eh.ParseSnapshotPerspective(perspective))
}
