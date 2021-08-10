package ehcli

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehclientfactory"
	"github.com/function61/eventhorizon/pkg/ehdebug"
	"github.com/function61/eventhorizon/pkg/system/ehstreammeta"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/os/osutil"
	"github.com/spf13/cobra"
)

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

	return ehdebug.Debug(ctx, streamName.At(version), os.Stdout, client)
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
