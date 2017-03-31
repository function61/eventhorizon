package main

import (
	"fmt"
	"github.com/function61/eventhorizon/config/configfactory"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/reader"
	rtypes "github.com/function61/eventhorizon/reader/types"
	"github.com/function61/eventhorizon/writer/writerclient"
	"strconv"
)

func readerRead(args []string) error {
	if len(args) != 2 {
		return usage("<Cursor> <LinesToRead>")
	}

	maxLinesToRead, atoiErr := strconv.Atoi(args[1])
	if atoiErr != nil {
		return atoiErr
	}

	confCtx := configfactory.BuildMust()

	rdr := reader.New(confCtx, writerclient.New(confCtx))

	result, err := rdr.Read(&rtypes.ReadOptions{
		Cursor:         cursor.CursorFromserializedMust(args[0]),
		MaxLinesToRead: maxLinesToRead,
	})

	if err != nil {
		return err
	}

	for _, line := range result.Lines {
		if line.MetaType == "" {
			fmt.Println(line.Content)
		} else {
			fmt.Printf("/%s %s\n", line.MetaType, line.Content)
		}
	}

	return nil
}
