// CLI for administering EventHorizon
package ehcli

import (
	"context"
	"fmt"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehdebug"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/gokit/ossignal"
	"github.com/spf13/cobra"
	"os"
	"path"
	"strconv"
)

func Entrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "eventhorizon",
		Short: "Manage EventHorizon",
	}

	parentCmd.AddCommand(&cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap the database",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			exitIfError(bootstrap(ossignal.InterruptOrTerminateBackgroundCtx(nil)))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "stream-create [path]",
		Short: "Create new stream, as a child of parent",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			exitIfError(streamCreate(ossignal.InterruptOrTerminateBackgroundCtx(nil), args[0]))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "read [stream] [pos]",
		Short: "Read events from the stream",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			version, err := strconv.Atoi(args[1])
			exitIfError(err)

			exitIfError(streamRead(ossignal.InterruptOrTerminateBackgroundCtx(nil), args[0], int64(version)))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "append [stream] [event]",
		Short: "Append event to the stream",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			exitIfError(streamAppend(ossignal.InterruptOrTerminateBackgroundCtx(nil), args[0], args[1]))
		},
	})

	return parentCmd
}

func bootstrap(ctx context.Context) error {
	horizon, err := buildClient()
	if err != nil {
		return err
	}

	return ehclient.Bootstrap(ctx, horizon)
}

func streamCreate(ctx context.Context, streamPath string) error {
	// "/foo" => "/"
	// "/foo/bar" => "/foo"
	parent := path.Dir(streamPath)
	// "/foo" => "foo"
	// "/foo/bar" => "bar"
	name := path.Base(streamPath)

	horizon, err := buildClient()
	if err != nil {
		return err
	}

	return horizon.CreateStream(ctx, parent, name)
}

func streamRead(ctx context.Context, streamPath string, version int64) error {
	horizon, err := buildClient()
	if err != nil {
		return err
	}

	resp, err := horizon.Read(
		ctx,
		ehclient.At(streamPath, version))
	if err != nil {
		return err
	}

	return ehdebug.Debug(resp, os.Stdout)
}

func streamAppend(ctx context.Context, streamPath string, event string) error {
	horizon, err := buildClient()
	if err != nil {
		return err
	}

	_, err = horizon.Append(ctx, streamPath, []string{event})
	return err
}

func buildClient() (*ehclient.Client, error) {
	conf, err := ehreader.GetConfig(ehreader.ConfigFromEnv)
	if err != nil {
		return nil, err
	}

	return ehreader.ClientFromConfig(conf), nil
}

func exitIfError(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
