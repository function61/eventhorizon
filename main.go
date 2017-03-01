package main

import (
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/pubsub/server"
	"github.com/function61/eventhorizon/writer"
	"github.com/function61/eventhorizon/writer/writerhttp"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	checkForS3AccessKeys()

	// start pub/sub server
	pubSubServer := server.NewESPubSubServer("0.0.0.0:" + strconv.Itoa(config.PUBSUB_PORT))

	esServer := writer.NewEventstoreWriter()

	httpCloser := make(chan bool)
	httpCloserDone := make(chan bool)
	writerhttp.HttpServe(esServer, httpCloser, httpCloserDone)

	log.Printf("main: waiting for stop signal")

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)

	// stop serving HTTP

	httpCloser <- true
	<-httpCloserDone

	// stop the main writer server

	esServer.Close()

	// stop pub/sub
	pubSubServer.Close()
}

func checkForS3AccessKeys() {
	if val := os.Getenv("AWS_ACCESS_KEY_ID"); val == "" {
		log.Fatalf("main: Need AWS_ACCESS_KEY_ID for S3 access. See README.md")
	}

	if val := os.Getenv("AWS_SECRET_ACCESS_KEY"); val == "" {
		log.Fatalf("main: Need AWS_SECRET_ACCESS_KEY for S3 access. See README.md")
	}
}
