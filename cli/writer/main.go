package main

import (
	"github.com/function61/eventhorizon/cli"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/pubsub/server"
	"github.com/function61/eventhorizon/writer"
	"github.com/function61/eventhorizon/writer/writerhttp"
	"log"
	"strconv"
)

func main() {
	if err := cli.CheckForS3AccessKeys(); err != nil {
		log.Fatalf("main: %s", err.Error())
	}

	// start pub/sub server
	pubSubServer := server.NewESPubSubServer("0.0.0.0:" + strconv.Itoa(config.PUBSUB_PORT))

	esServer := writer.NewEventstoreWriter()

	httpCloser := make(chan bool)
	httpCloserDone := make(chan bool)
	writerhttp.HttpServe(esServer, httpCloser, httpCloserDone)

	log.Printf("main: waiting for stop signal")

	log.Println(cli.WaitForInterrupt())

	// stop serving HTTP

	httpCloser <- true
	<-httpCloserDone

	// stop the main writer server

	esServer.Close()

	// stop pub/sub
	pubSubServer.Close()
}
