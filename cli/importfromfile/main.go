package main

import (
	"bufio"
	"github.com/function61/eventhorizon/writer"
	"os"
)

func importLinesFromFile(filename string, stream string, esServer *writer.EventstoreWriter) int {
	exportFile, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer exportFile.Close()

	exportFileScanner := bufio.NewScanner(exportFile)

	linesScanned := 0

	batch := []string{}

	evaluateBatch := func() {
		if len(batch) == 0 {
			return
		}

		if err := esServer.AppendToStream(stream, batch); err != nil {
			panic(err)
		}

		batch = []string{}
	}

	for exportFileScanner.Scan() {
		line := exportFileScanner.Text()

		// fmt.Println(line)

		linesScanned++

		batch = append(batch, line)

		if len(batch) >= 100 {
			evaluateBatch()
		}

		/*
			if linesScanned >= 10 {
				break
			}
		*/
	}

	evaluateBatch()

	return linesScanned
}

func main() {
	esWriter := writer.NewEventstoreWriter()
	defer esWriter.Close()

	importLinesFromFile("/app/test-dump/export.txt", "/tenants/foo", esWriter)
}
