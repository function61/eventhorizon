package main

import (
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/pubsub/client"
	"log"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <topic>", os.Args[0])
	}

	topic := os.Args[1]

	pubSubClient := client.NewPubSubClient("127.0.0.1:" + strconv.Itoa(config.PUBSUB_PORT))
	pubSubClient.Subscribe(topic)

	for {
		msg := <-pubSubClient.Notifications

		log.Printf("Recv: %v", msg)
	}

	// TODO: no graceful quit mechanism
	pubSubClient.Close()
}
