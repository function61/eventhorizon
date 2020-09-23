// CLI for administering EventHorizon
package ehcli

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehclientfactory"
	"github.com/function61/eventhorizon/pkg/ehdebug"
	"github.com/function61/eventhorizon/pkg/ehserver"
	"github.com/function61/eventhorizon/pkg/ehserver/ehdynamodb"
	"github.com/function61/eventhorizon/pkg/system/ehstreammeta"
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

func streamEntrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "stream",
		Short: "Stream management",
	}

	parentCmd.AddCommand(&cobra.Command{
		Use:   "mk [path]",
		Short: "Create new stream, as a child of parent",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(streamCreate(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "cat [stream] [pos]",
		Short: "Read events from the stream",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			version, err := strconv.Atoi(args[1])
			osutil.ExitIfError(err)

			osutil.ExitIfError(streamReadDebug(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				int64(version),
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "append [stream] [event]",
		Short: "Append event to the stream",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(streamAppend(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				args[1],
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "ls [streamName]",
		Short: "List child streams",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(listChildStreams(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				rootLogger))
		},
	})

	return parentCmd
}

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

func listChildStreams(ctx context.Context, streamNameRaw string, logger *log.Logger) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	streamName, err := eh.DeserializeStreamName(streamNameRaw)
	if err != nil {
		return err
	}

	streamMeta, err := ehstreammeta.LoadUntilRealtime(ctx, streamName, client, nil)
	if err != nil {
		return err
	}

	childStreams, truncated := streamMeta.State.ChildStreams()
	if truncated {
		fmt.Fprintln(os.Stderr, "WARNING: Returned child stream list was truncated")
	}

	for _, childStream := range childStreams {
		fmt.Println(childStream.String())
	}

	return nil
}

func snapshotCat(
	ctx context.Context,
	streamNameRaw string,
	snapshotContext string,
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
		snapshotContext)
	if err != nil {
		return err
	}

	fmt.Fprintf(
		os.Stderr,
		"Snapshot (kind=%s) @ %s\n--------\n",
		snapPersisted.Kind().String(),
		snapPersisted.Cursor.Serialize())

	snap, err := snapPersisted.DecryptIfRequired(func() ([]byte, error) {
		return client.LoadDek(ctx, snapPersisted.Cursor.Stream())
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
	snapshotContext string,
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

	snapshot := eh.NewSnapshot(cursor, content, snapshotContext)

	persisted, err := func() (*eh.PersistedSnapshot, error) {
		if encrypted {
			dek, err := client.LoadDek(ctx, cursor.Stream())
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
	snapshotContext string,
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

	return client.SnapshotStore.DeleteSnapshot(ctx, stream, snapshotContext)
}

func streamCreate(ctx context.Context, streamPath string, logger *log.Logger) error {
	stream, err := eh.DeserializeStreamName(streamPath)
	if err != nil {
		return err
	}

	if stream.IsUnder(eh.SysSubscriptions) {
		// this is because subscription stream creation does extra: it subscribes to itself
		// (for realtime notifications, not activity events which would be an infinite loop)
		return fmt.Errorf("please use subscription stream creation to create %s", stream.String())
	}

	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	_, err = client.CreateStream(ctx, stream, nil)
	return err
}

func streamReadDebug(ctx context.Context, streamNameRaw string, version int64, logger *log.Logger) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	streamName, err := eh.DeserializeStreamName(streamNameRaw)
	if err != nil {
		return err
	}

	return ehdebug.Debug(ctx, streamName.At(version), os.Stdout, client, logger)
}

func streamAppend(ctx context.Context, streamNameRaw string, event string, logger *log.Logger) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	streamName, err := eh.DeserializeStreamName(streamNameRaw)
	if err != nil {
		return err
	}

	return client.AppendStrings(ctx, streamName, []string{event})
}
