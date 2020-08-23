package ehdebug

import (
	"fmt"
	"io"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
)

func Debug(result *eh.ReadResult, output io.Writer) error {
	entries := result.Entries

	var err error
	printfln := func(formatStr string, args ...interface{}) {
		if err != nil {
			return
		}

		_, err = fmt.Fprintf(output, formatStr+"\n", args...)
	}

	printfln("%s => %d entrie(s)", result.LastEntry.Stream(), len(entries))

	for _, entry := range entries {
		printfln("<entry v=%d>", entry.Version.Version())

		if entry.MetaEvent != nil {
			ev, err := ehevent.Deserialize(*entry.MetaEvent, eh.MetaTypes)
			if err != nil {
				return err
			}

			switch e := ev.(type) {
			case *eh.StreamChildStreamCreated:
				printfln("  /%s: %s", ev.MetaType(), e.Stream)
			case *eh.StreamStarted:
				printfln("  /%s", ev.MetaType())
			case *eh.SubscriptionSubscribed:
				printfln("  /%s: %s", ev.MetaType(), e.Id)
			case *eh.SubscriptionUnsubscribed:
				printfln("  /%s: %s", ev.MetaType(), e.Id)
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
