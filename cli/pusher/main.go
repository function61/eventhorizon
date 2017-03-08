package main

import (
	"github.com/function61/eventhorizon/cli"
	"github.com/function61/eventhorizon/pusher"
	"log"
)

func main() {
	if err := cli.CheckForS3AccessKeys(); err != nil {
		log.Fatalf("main: %s", err.Error())
	}

	psh := pusher.NewPusher()
	go psh.Run()

	log.Println(cli.WaitForInterrupt())

	psh.Close()
}
