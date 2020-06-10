package main

import (
	"fmt"
	"strconv"

	"github.com/function61/eventhorizon/pkg/legacy/config/configfactory"
	"github.com/function61/eventhorizon/pkg/legacy/cursor"
	"github.com/function61/eventhorizon/pkg/legacy/reader"
	rtypes "github.com/function61/eventhorizon/pkg/legacy/reader/types"
	"github.com/function61/eventhorizon/pkg/legacy/writer/writerclient"
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
