package main

// Imports events into a stream from a file with one line per event.

import (
	"bufio"
	"os"
)

type batchProcessor func([]string) error

const (
	batchAmount = 1000
)

func readLinebatchesFromFile(filename string, processFn batchProcessor) int {
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
			if err := processFn(batch); err != nil {
				panic(err)
			}
			batch = []string{}
		}
	}
	if err := exportFileScanner.Err(); err != nil {
		panic(err)
	}

	if len(batch) > 0 {
		if err := processFn(batch); err != nil {
			panic(err)
		}
	}

	return linesScanned
}
