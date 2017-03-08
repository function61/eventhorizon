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

	r := NewReceiver()

	psh := pusher.NewPusher(r)
	go psh.Run()

	log.Println(cli.WaitForInterrupt())

	psh.Close()
}
