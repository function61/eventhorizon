package main

import (
	"log"
	"time"

	"github.com/function61/eventhorizon/pkg/legacy/config/configfactory"
	"github.com/function61/eventhorizon/pkg/legacy/pubsub/client"
)

type Stats struct {
	messagesProcessed int
}

func startSubscriber(topic string) {
	pubSubClient := client.New(configfactory.BuildMust())
	pubSubClient.Subscribe(topic)

	defer pubSubClient.Close()
	defer log.Printf("Closing")

	stats := Stats{}

	ticker := time.NewTicker(10 * time.Second)
	quit := make(chan struct{})
	go func() {
		previous := stats

		for {
			select {
			case <-ticker.C:
				snapshot := stats

				delta := Stats{messagesProcessed: snapshot.messagesProcessed - previous.messagesProcessed}

				log.Printf("Stats: %d", delta.messagesProcessed)

				previous = snapshot

			case <-quit:
				ticker.Stop()
				log.Printf("Ticker stopped")
				return
			}
		}
	}()

	// close(quit)

	for {
		// msg := <-pubSubClient.notifications
		<-pubSubClient.Notifications

		stats.messagesProcessed++
		// log.Printf("Incoming: %v", msg)
	}
}
