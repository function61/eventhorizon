package main

// Imports events into a stream from a file with one line per event.

import (
	"bufio"
	"github.com/function61/pyramid/writer/writerclient"
	"github.com/function61/pyramid/writer/writerhttp/types"
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
	log.Fatalf("Usage: %s <stream> <filename>", os.Args[0])
}
func main() {
	if len(os.Args) != 3 {
		printUsageAndExit()
	}

	stream := os.Args[1]
	filename := os.Args[2]

	client := writerclient.NewClient()

	evaluateBatch := func(batch []string) error {
		appendRequest := &types.AppendToStreamRequest{
			Stream: stream,
			Lines:  batch,
		}

		return client.Append(appendRequest)
	}

	importLinesFromFile(filename, evaluateBatch)
}
