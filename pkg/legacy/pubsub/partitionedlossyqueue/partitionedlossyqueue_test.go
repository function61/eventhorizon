package partitionedlossyqueue

import (
	"github.com/function61/gokit/assert"
	"strconv"
	"testing"
)

func TestMain(t *testing.T) {
	type StatStruct struct {
		NumReceives         int
		LastReceivedMessage string
	}

	plq := New()

	consumerDone := make(chan map[string]*StatStruct)

	producer := func() {
		for i := 0; i <= 100000; i++ {
			plq.Put("topic A", strconv.Itoa(i))
			plq.Put("topic B", strconv.Itoa(i))

			if i == 42356 {
				plq.Put("topic C", strconv.Itoa(i))
			}
		}

		plq.Close()
	}

	consumer := func() {
		stats := make(map[string]*StatStruct)

		// used only as a signal channel
		for range plq.ReceiveAvailable {
			latestMessagesForTopics := plq.ReceiveAndClear()

			for topic, message := range latestMessagesForTopics {
				if _, exists := stats[topic]; !exists {
					stats[topic] = &StatStruct{}
				}

				stats[topic].NumReceives++
				stats[topic].LastReceivedMessage = message
			}
		}

		/*	Stats are expected to look like:

			Partition=topic A NumReceives=2774 LastReceivedMessage=1000000
			Partition=topic B NumReceives=2764 LastReceivedMessage=1000000
			Partition=topic C NumReceives=1 LastReceivedMessage=42356
		*/
		consumerDone <- stats
	}

	go consumer()
	go producer()

	stats := <-consumerDone

	// I'm tempted to put test for NumReceives < LastReceivedMessage but
	// there's no guarantee that the queue should drop messages, so I don't want
	// to build a flaky test. Instead we'll just test that we received the latest
	// messages correctly.

	assert.EqualString(t, stats["topic A"].LastReceivedMessage, "100000")
	assert.EqualString(t, stats["topic B"].LastReceivedMessage, "100000")
	assert.EqualString(t, stats["topic C"].LastReceivedMessage, "42356")

	/*
		statsFormatted := []string{}

		for partition, stat := range stats {
			statsFormatted = append(
				statsFormatted,
				fmt.Sprintf("Partition=%s NumReceives=%d LastReceivedMessage=%s", partition, stat.NumReceives, stat.LastReceivedMessage))
		}

		fmt.Printf("Stats:\n\t%s\n", strings.Join(statsFormatted, "\n\t"))
	*/
}
