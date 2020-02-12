package main

import (
	"github.com/function61/eventhorizon/pkg/legacy/config/configfactory"
	"github.com/function61/eventhorizon/pkg/legacy/pubsub/client"
)

func testPublish(topic string, message string) {
	pubSubClient := client.New(configfactory.BuildMust())
	defer pubSubClient.Close()

	// for i := 0; i < 10000; i++ {
	for {
		pubSubClient.Publish(topic, message)
	}
	// pubSubClient.Publish(topic, message)
}
