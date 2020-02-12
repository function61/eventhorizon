package ehdebug

import (
	"fmt"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"io"
)

func Debug(result *ehclient.ReadResult, output io.Writer) error {
	entries := result.Entries

	var err error
	printfln := func(formatStr string, args ...interface{}) {
		if err != nil {
			return
		}

		_, err = fmt.Fprintf(output, formatStr+"\n", args...)
	}

	printfln("%s => %d entrie(s)", entries[0].Stream, len(entries))

	for _, entry := range entries {
		printfln("<entry v=%d>", entry.Version)

		if entry.MetaEvent != nil {
			ev, err := ehevent.Deserialize(*entry.MetaEvent, ehclient.MetaTypes)
			if err != nil {
				return err
			}

			switch e := ev.(type) {
			case *ehclient.ChildStreamCreated:
				printfln("  /ChildStreamCreated: %s", e.Stream)
			case *ehclient.StreamStarted:
				printfln("  /StreamStarted")
			default:
				printfln("  unknown meta event: %s", ev.MetaType())
			}
		}

		for _, event := range entry.Events {
			printfln("  %s", event)
		}

		printfln("</entry>")
	}

	return err
}
