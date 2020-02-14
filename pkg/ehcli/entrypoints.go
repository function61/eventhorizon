// Command line interface for administering EventHorizon
package ehcli

import (
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehdebug"
	"github.com/function61/gokit/ossignal"
	"github.com/spf13/cobra"
	"os"
	"path"
	"strconv"
)

func Entrypoints() []*cobra.Command {
	prod := &cobra.Command{
		Use:   "eh-prod",
		Short: "EventHorizon subcommands (production)",
	}

	dev := &cobra.Command{
		Use:   "eh-dev",
		Short: "EventHorizon subcommands (development)",
	}

	attachSubcommands(prod, ehclient.New(ehclient.Production()))
	attachSubcommands(dev, ehclient.New(ehclient.Development()))

	return []*cobra.Command{prod, dev}
}

func attachSubcommands(parentCmd *cobra.Command, eh *ehclient.Client) {
	parentCmd.AddCommand(&cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap the database",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			if err := ehclient.Bootstrap(ossignal.InterruptOrTerminateBackgroundCtx(nil), eh); err != nil {
				panic(err)
			}
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "stream-create [path]",
		Short: "Create new stream, as a child of parent",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			// "/foo" => "/"
			// "/foo/bar" => "/foo"
			parent := path.Dir(args[0])
			// "/foo" => "foo"
			// "/foo/bar" => "bar"
			name := path.Base(args[0])

			if err := eh.CreateStream(ossignal.InterruptOrTerminateBackgroundCtx(nil), parent, name); err != nil {
				panic(err)
			}
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "read [stream] [pos]",
		Short: "Read events from the stream",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			version, err := strconv.Atoi(args[1])
			if err != nil {
				panic(err)
			}

			resp, err := eh.Read(
				ossignal.InterruptOrTerminateBackgroundCtx(nil),
				ehclient.At(args[0], int64(version)))
			if err != nil {
				panic(err)
			}

			if err := ehdebug.Debug(resp, os.Stdout); err != nil {
				panic(err)
			}
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "append [stream] [event]",
		Short: "Append event to the stream",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			if err := eh.Append(
				ossignal.InterruptOrTerminateBackgroundCtx(nil),
				args[0],
				[]string{args[1]},
			); err != nil {
				panic(err)
			}
		},
	})
}
