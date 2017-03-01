package main

import (
	"fmt"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/importfromfile"
	"github.com/function61/eventhorizon/reader"
	"github.com/function61/eventhorizon/writer"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		log.Printf("Usage: %s foo", os.Args[0])
		os.Exit(1)
	}

	checkForS3AccessKeys()

	command := os.Args[1]

	esServer := writer.NewEventstoreWriter()
	defer esServer.Close()

	if command == "CreateStream" {
		esServer.CreateStream("/tenants/foo")
	} else if command == "read" {
		reader := reader.NewEventstoreReader()

		readResult, err := reader.Read(cursor.CursorFromserializedMust("/tenants/foo:0:0"))
		if err != nil {
			panic(err)
		}

		fmt.Printf("%v\n", readResult)
	} else if command == "serve" {
		httpCloser := make(chan bool)
		httpCloserDone := make(chan bool)
		writer.HttpServe(esServer, httpCloser, httpCloserDone)
		defer func() { <-httpCloserDone }() // defers are executed in reverse order
		defer func() { httpCloser <- true }()
	} else if command == "fill" {
		// 16 MB chunks should yield 15 pieces for 237.6M

		linesScanned := importfromfile.Run("/app/test-dump/export.txt", "/tenants/foo", esServer)
		// linesScanned := CliLoadFromFile("/app/test-dump/1k.txt", "/tenants/foo", esServer)
		// linesScanned := CliLoadFromFile("/app/test-dump/4k.txt", "/tenants/foo", esServer)

		log.Printf("main: %d line(s) appended", linesScanned)
	} else if command == "AppendToStream" {
		contentToAppend := []string{fmt.Sprintf("time is %s", time.Now())}
		if err := esServer.AppendToStream("/tenants/foo", contentToAppend); err != nil {
			panic(err)
		}
	} else {
		log.Printf("main: Unknown command: %s", command)
	}

	log.Printf("main: waiting for stop signal")

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)
}

func checkForS3AccessKeys() {
	if val := os.Getenv("AWS_ACCESS_KEY_ID"); val == "" {
		log.Fatalf("main: Need AWS_ACCESS_KEY_ID for S3 access. See README.md")
	}

	if val := os.Getenv("AWS_SECRET_ACCESS_KEY"); val == "" {
		log.Fatalf("main: Need AWS_SECRET_ACCESS_KEY for S3 access. See README.md")
	}
}
