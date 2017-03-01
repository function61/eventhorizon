package main

import (
	"strconv"
	"github.com/function61/eventhorizon/pubsub/client"
)

func testPublish(serverPort int, topic string, message string) {
	pubSubClient := client.NewPubSubClient("127.0.0.1:" + strconv.Itoa(serverPort))
	// for i := 0; i < 10000; i++ {
	for {
		pubSubClient.Publish(topic, message)
	}
	// pubSubClient.Publish(topic, message)

	pubSubClient.Close()
}
