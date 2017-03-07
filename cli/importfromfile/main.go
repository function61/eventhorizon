package main

import (
	"bufio"
	"github.com/function61/eventhorizon/writer"
	"github.com/function61/eventhorizon/writer/writerclient"
	"github.com/function61/eventhorizon/writer/writerhttp"
	"log"
	"os"
)

type batchImporter func([]string) error

const (
	batchAmount = 1000
)

func importLinesFromFile(filename string, importFn batchImporter) int {
	exportFile, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer exportFile.Close()

	exportFileScanner := bufio.NewScanner(exportFile)

	linesScanned := 0

	batch := []string{}

	for exportFileScanner.Scan() {
		line := exportFileScanner.Text()

		linesScanned++

		batch = append(batch, line)

		if len(batch) >= batchAmount {
			if err := importFn(batch); err != nil {
				panic(err)
			}
			batch = []string{}
		}
	}

	if len(batch) > 0 {
		if err := importFn(batch); err != nil {
			panic(err)
		}
	}

	return linesScanned
}

func printUsageAndExit() {
	log.Fatalf("Usage: %s <mode=server|client> <stream> <filename>", os.Args[0])
}
func main() {
	if len(os.Args) != 4 {
		printUsageAndExit()
	}

	mode := os.Args[1]
	stream := os.Args[2]
	filename := os.Args[3]

	if mode == "server" {
		esWriter := writer.NewEventstoreWriter()
		defer esWriter.Close()

		evaluateBatch := func(batch []string) error {
			return esWriter.AppendToStream(stream, batch)
		}

		importLinesFromFile(filename, evaluateBatch)
	} else if mode == "client" {
		client := writerclient.NewClient()

		evaluateBatch := func(batch []string) error {
			appendRequest := &writerhttp.AppendToStreamRequest{
				Stream: stream,
				Lines:  batch,
			}

			return client.Append(appendRequest)
		}

		importLinesFromFile(filename, evaluateBatch)
	} else {
		printUsageAndExit()
	}
}
