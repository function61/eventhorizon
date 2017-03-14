package main

import (
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/pubsub/client"
)

func testPublish(topic string, message string) {
	pubSubClient := client.New(config.NewContext())
	// for i := 0; i < 10000; i++ {
	for {
		pubSubClient.Publish(topic, message)
	}
	// pubSubClient.Publish(topic, message)

	pubSubClient.Close()
}
