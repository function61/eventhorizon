package main

import (
	"github.com/function61/pyramid/config/configfactory"
	"github.com/function61/pyramid/pubsub/client"
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
