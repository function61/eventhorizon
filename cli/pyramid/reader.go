package main

import (
	"fmt"
	"github.com/function61/pyramid/config/configfactory"
	"github.com/function61/pyramid/cursor"
	"github.com/function61/pyramid/reader"
	rtypes "github.com/function61/pyramid/reader/types"
	"github.com/function61/pyramid/writer/writerclient"
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

	confCtx := configfactory.Build()

	rdr := reader.New(confCtx, writerclient.New(confCtx))

	result, err := rdr.Read(&rtypes.ReadOptions{
		Cursor:         cursor.CursorFromserializedMust(args[0]),
		MaxLinesToRead: maxLinesToRead,
	})

	if err != nil {
		return err
	}

	for _, line := range result.Lines {
		fmt.Println(line.Content)
	}

	return nil
}
