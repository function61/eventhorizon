package main

import (
	"github.com/function61/eventhorizon/config/configfactory"
	"github.com/function61/eventhorizon/pubsub/client"
)

func testPublish(topic string, message string) {
	pubSubClient := client.New(configfactory.Build())
	// for i := 0; i < 10000; i++ {
	for {
		pubSubClient.Publish(topic, message)
	}
	// pubSubClient.Publish(topic, message)

	pubSubClient.Close()
}
