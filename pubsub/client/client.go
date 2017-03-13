package client

import (
	"bufio"
	"github.com/function61/pyramid/pubsub"
	"github.com/function61/pyramid/pubsub/partitionedlossyqueue"
	"github.com/jpillora/backoff"
	"io"
	"log"
	"net"
	"time"
)

type PubSubClient struct {
	writeCh          chan string
	done             chan bool
	Notifications    chan []string
	quitting         bool
	connected        bool
	sendQueue        *partitionedlossyqueue.Queue
	subscribedTopics []string
}

func New(serverAddress string) *PubSubClient {
	this := &PubSubClient{
		Notifications:    make(chan []string),
		writeCh:          make(chan string),
		done:             make(chan bool),
		subscribedTopics: []string{},
		connected:        false,
		sendQueue:        partitionedlossyqueue.New(),
		quitting:         false,
	}

	go this.manageConnectivity(serverAddress)

	return this
}

func (this *PubSubClient) Subscribe(topic string) {
	this.subscribedTopics = append(this.subscribedTopics, topic)

	if this.connected { // otherwise will be sent on connect/reconnect
		this.sendSubscriptionMessage(topic)
	}
}

// guaranteed to never block, but can lose all but the latest message per topic
func (this *PubSubClient) Publish(topic string, message string) {
	this.sendQueue.Put(topic, pubsub.MsgformatEncode([]string{"PUB", topic, message}))
}

func (this *PubSubClient) Close() {
	this.quitting = true

	close(this.Notifications)

	// don't send bye unless connected
	if !this.connected {
		return
	}

	packet := pubsub.MsgformatEncode([]string{"BYE"})

	this.writeCh <- packet

	<-this.done
}

func (this *PubSubClient) manageConnectivity(serverAddress string) {
	reconnectBackoff := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	stopWriting := make(chan bool)

	for {
		conn, err := net.Dial("tcp", serverAddress)
		if err != nil {
			// don't try to forever connect on connection errors if
			// we're trying to close connection anyway
			if this.quitting {
				return
			}

			backoffDuration := reconnectBackoff.Duration()
			log.Printf("PubSubClient: manageConnectivity: reconnecting in %s: %s", backoffDuration, err)
			time.Sleep(backoffDuration)
			continue
		}

		log.Printf("PubSubClient: manageConnectivity: connected to %s", serverAddress)

		reconnectBackoff.Reset()

		go this.handleWrites(conn, stopWriting)

		// re-subscribe
		for _, topic := range this.subscribedTopics {
			this.sendSubscriptionMessage(topic)
		}

		this.connected = true

		reader := bufio.NewReader(conn)

		continueStuffs := true

		for continueStuffs {
			// will listen for message to process ending in newline (\n)
			messageRaw, errRead := reader.ReadString('\n')
			if errRead != nil {
				this.connected = false
				stopWriting <- true

				if errRead == io.EOF {
					log.Printf("PubSubClient: manageConnectivity: EOF encountered")
				} else {
					log.Printf("PubSubClient: manageConnectivity: error")
				}

				if this.quitting {
					log.Printf("PubSubClient: manageConnectivity: quitting: signalling done & stopping loop")
					this.done <- true
					return
				}

				continueStuffs = false
			} else {
				message := pubsub.MsgformatDecode(messageRaw)

				if !this.quitting {
					this.Notifications <- message
				}
			}
		}
	}
}

func (this *PubSubClient) handleWrites(conn net.Conn, stop chan bool) {
	defer log.Printf("PubSubClient: handleWrites: stopping")

	for {
		select {
		case <-this.sendQueue.ReceiveAvailable:
			packet := ""
			for _, message := range this.sendQueue.ReceiveAndClear() {
				packet += message
			}

			if _, err := conn.Write([]byte(packet)); err != nil {
				log.Printf("PubSubClient: handleWrites: %s", err.Error())
				return
			}
		case packet := <-this.writeCh:
			if _, err := conn.Write([]byte(packet)); err != nil {
				log.Printf("PubSubClient: handleWrites: %s", err.Error())
				return
			}
		case <-stop:
			return
		}
	}

}

func (this *PubSubClient) sendSubscriptionMessage(topic string) {
	log.Printf("PubSubClient: sendSubscriptionMessage: subscribing to %s", topic)

	packet := pubsub.MsgformatEncode([]string{"SUB", topic})

	this.writeCh <- packet
}
