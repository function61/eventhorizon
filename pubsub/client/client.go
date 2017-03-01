package client

import (
	"bufio"
	"github.com/function61/eventhorizon/pubsub"
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
	subscribedTopics []string
}

func NewPubSubClient(serverAddress string) *PubSubClient {
	this := &PubSubClient{
		Notifications:    make(chan []string),
		writeCh:          make(chan string),
		done:             make(chan bool),
		subscribedTopics: []string{},
		connected:        false,
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

func (this *PubSubClient) Publish(topic string, message string) {
	packet := pubsub.MsgformatEncode([]string{"PUB", topic, message})

	this.writeCh <- packet
}

func (this *PubSubClient) Close() {
	this.quitting = true
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
			backoffDuration := reconnectBackoff.Duration()
			log.Printf("manageConnectivity: reconnecting in %s: %s", backoffDuration, err)
			time.Sleep(backoffDuration)
			continue
		}

		log.Printf("manageConnectivity: connected to %s", serverAddress)

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
					log.Printf("manageConnectivity: EOF encountered")
				} else {
					log.Printf("manageConnectivity: error")
				}

				if this.quitting {
					log.Printf("manageConnectivity: quitting: signalling done & stopping loop")
					this.done <- true
					return
				}

				continueStuffs = false
			} else {
				message := pubsub.MsgformatDecode(messageRaw)

				this.Notifications <- message
			}
		}
	}
}

func (this *PubSubClient) handleWrites(conn net.Conn, stop chan bool) {
	shouldContinue := true

	for shouldContinue {
		select {
		case packet := <-this.writeCh:
			conn.Write([]byte(packet))
		case <-stop:
			log.Printf("handleWrites: stop requested")
			shouldContinue = false
		}
	}

	log.Printf("handleWrites: stopping")
}

func (this *PubSubClient) sendSubscriptionMessage(topic string) {
	log.Printf("sendSubscriptionMessage: subscribing to %s", topic)

	packet := pubsub.MsgformatEncode([]string{"SUB", topic})

	this.writeCh <- packet
}
