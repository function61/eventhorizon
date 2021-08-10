package ehdebug

import (
	"context"
	"fmt"
	"io"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
)

func Debug(
	ctx context.Context,
	cursor eh.Cursor,
	output io.Writer,
	client *ehclient.SystemClient,
) error {
	debugStore, err := loadUntilRealtime(ctx, cursor, client)
	if err != nil {
		return err
	}

	printfln := func(formatStr string, args ...interface{}) {
		if err != nil {
			return
		}

		_, err = fmt.Fprintf(output, formatStr+"\n", args...)
	}

	printfln("%s => %d entrie(s)", cursor.Stream(), len(debugStore.entries))

	for _, entry := range debugStore.entries {
		printfln("<entry v=%d>", entry.cursor.Version())

		for _, line := range entry.lines {
			printfln("  %s", line)
		}

		printfln("</entry>")
	}

	return err
}
