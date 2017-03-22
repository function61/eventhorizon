package main

import (
	"github.com/function61/pyramid/config/configfactory"
	"github.com/function61/pyramid/pubsub/client"
	"log"
	"time"
)

type Stats struct {
	messagesProcessed int
}

func startSubscriber(topic string) {
	pubSubClient := client.New(configfactory.Build())
	pubSubClient.Subscribe(topic)

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

	log.Printf("Closing")

	pubSubClient.Close()
}
