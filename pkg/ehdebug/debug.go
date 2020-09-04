package ehdebug

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehreader"
)

func Debug(
	ctx context.Context,
	cursor eh.Cursor,
	output io.Writer,
	client *ehreader.SystemClient,
	logger *log.Logger,
) error {
	debugStore, err := loadUntilRealtime(ctx, cursor, client, logger)
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
