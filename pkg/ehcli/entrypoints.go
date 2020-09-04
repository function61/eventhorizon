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
	"github.com/function61/eventhorizon/pkg/ehdebug"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/eventhorizon/pkg/ehreaderfactory"
	"github.com/function61/eventhorizon/pkg/ehserver"
	"github.com/function61/eventhorizon/pkg/ehserver/ehdynamodb"
	"github.com/function61/eventhorizon/pkg/system/ehchildstreams"
	"github.com/function61/gokit/logex"
	"github.com/function61/gokit/osutil"
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

	parentCmd.AddCommand(credentialsEntrypoint())

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
			osutil.ExitIfError(bootstrap(osutil.CancelOnInterruptOrTerminate(nil)))
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
			osutil.ExitIfError(streamCreate(osutil.CancelOnInterruptOrTerminate(nil), args[0]))
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
			osutil.ExitIfError(streamAppend(
				osutil.CancelOnInterruptOrTerminate(nil), args[0], args[1]))
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
			osutil.ExitIfError(snapshotCat(
				osutil.CancelOnInterruptOrTerminate(logex.StandardLogger()),
				args[0],
				args[1]))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "rm [streamName] [context]",
		Short: "Delete a snapshot",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			osutil.ExitIfError(snapshotRm(
				osutil.CancelOnInterruptOrTerminate(logex.StandardLogger()),
				args[0],
				args[1]))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "put [cursor] [context]",
		Short: "Replace a snapshot (dangerous)",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			osutil.ExitIfError(snapshotPut(
				osutil.CancelOnInterruptOrTerminate(logex.StandardLogger()),
				args[0],
				args[1],
				os.Stdin))
		},
	})

	return parentCmd
}

func bootstrap(ctx context.Context) error {
	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	return ehdynamodb.Bootstrap(ctx, client.EventLog.(*ehdynamodb.Client))
}

func listChildStreams(ctx context.Context, streamNameRaw string, logger *log.Logger) error {
	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	streamName, err := eh.DeserializeStreamName(streamNameRaw)
	if err != nil {
		return err
	}

	childStreams, err := ehchildstreams.LoadUntilRealtime(ctx, streamName, client, logger)
	if err != nil {
		return err
	}

	for _, childStream := range childStreams.State.ChildStreams() {
		fmt.Println(childStream)
	}

	return nil
}

func snapshotCat(ctx context.Context, streamNameRaw string, snapshotContext string) error {
	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	streamName, err := eh.DeserializeStreamName(streamNameRaw)
	if err != nil {
		return err
	}

	// the Beginning() is non-important, as the store only uses the stream component of Cursor
	snap, err := client.SnapshotStore.ReadSnapshot(
		ctx,
		streamName,
		snapshotContext)
	if err != nil {
		return err
	}

	fmt.Fprintf(
		os.Stderr,
		"Snapshot @ %s\n--------\n",
		snap.Cursor.Serialize())

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
) error {
	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
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

	return client.SnapshotStore.WriteSnapshot(ctx, *snapshot)
}

func snapshotRm(ctx context.Context, streamName string, snapshotContext string) error {
	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	stream, err := eh.DeserializeStreamName(streamName)
	if err != nil {
		return err
	}

	return client.SnapshotStore.DeleteSnapshot(ctx, stream, snapshotContext)
}

func streamCreate(ctx context.Context, streamPath string) error {
	stream, err := eh.DeserializeStreamName(streamPath)
	if err != nil {
		return err
	}

	if stream.IsUnder(eh.SysSubscriptions) {
		// this is because subscription stream creation does extra: it subscribes to itself
		// (for realtime notifications, not activity events which would be an infinite loop)
		return fmt.Errorf("please use subscription stream creation to create %s", stream.String())
	}

	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	_, err = client.CreateStream(ctx, stream, nil)
	return err
}

func streamReadDebug(ctx context.Context, streamNameRaw string, version int64, logger *log.Logger) error {
	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	streamName, err := eh.DeserializeStreamName(streamNameRaw)
	if err != nil {
		return err
	}

	return ehdebug.Debug(ctx, streamName.At(version), os.Stdout, client, logger)
}

func streamAppend(ctx context.Context, streamNameRaw string, event string) error {
	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	streamName, err := eh.DeserializeStreamName(streamNameRaw)
	if err != nil {
		return err
	}

	return client.AppendStrings(ctx, streamName, []string{event})
}
