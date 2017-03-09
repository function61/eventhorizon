package main

import (
	"github.com/function61/pyramid/cli"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/pubsub/server"
	"github.com/function61/pyramid/writer"
	"github.com/function61/pyramid/writer/writerhttp"
	"log"
	"strconv"
)

func main() {
	if err := cli.CheckForS3AccessKeys(); err != nil {
		log.Fatalf("main: %s", err.Error())
	}

	log.Println("       .")
	log.Println("      /=\\\\       PyramidDB")
	log.Println("     /===\\ \\     function61.com")
	log.Println("    /=====\\  \\")
	log.Println("   /=======\\  /")
	log.Println("  /=========\\/")

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
