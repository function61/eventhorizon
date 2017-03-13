package client

import (
	"bufio"
	"github.com/function61/pyramid/config"
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
	p := &PubSubClient{
		Notifications:    make(chan []string),
		writeCh:          make(chan string),
		done:             make(chan bool),
		subscribedTopics: []string{},
		connected:        false,
		sendQueue:        partitionedlossyqueue.New(),
		quitting:         false,
	}

	go p.manageConnectivity(serverAddress)

	return p
}

func (p *PubSubClient) Subscribe(topic string) {
	p.subscribedTopics = append(p.subscribedTopics, topic)

	if p.connected { // otherwise will be sent on connect/reconnect
		p.sendSubscriptionMessage(topic)
	}
}

// guaranteed to never block, but can lose all but the latest message per topic
func (p *PubSubClient) Publish(topic string, message string) {
	p.sendQueue.Put(topic, pubsub.MsgformatEncode([]string{"PUB", topic, message}))
}

func (p *PubSubClient) Close() {
	p.quitting = true

	close(p.Notifications)

	// don't send bye unless connected
	if !p.connected {
		return
	}

	packet := pubsub.MsgformatEncode([]string{"BYE"})

	p.writeCh <- packet

	<-p.done
}

func (p *PubSubClient) manageConnectivity(serverAddress string) {
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
			if p.quitting {
				return
			}

			backoffDuration := reconnectBackoff.Duration()
			log.Printf("PubSubClient: manageConnectivity: reconnecting in %s: %s", backoffDuration, err)
			time.Sleep(backoffDuration)
			continue
		}

		log.Printf("PubSubClient: manageConnectivity: connected to %s", serverAddress)

		reconnectBackoff.Reset()

		p.connected = true

		go p.handleWrites(conn, stopWriting)

		// send auth token
		p.writeCh <- pubsub.MsgformatEncode([]string{"AUTH", config.AUTH_TOKEN})

		// re-subscribe
		for _, topic := range p.subscribedTopics {
			p.sendSubscriptionMessage(topic)
		}

		reader := bufio.NewReader(conn)

		continueStuffs := true

		for continueStuffs {
			// will listen for message to process ending in newline (\n)
			messageRaw, errRead := reader.ReadString('\n')
			if errRead != nil {
				p.connected = false
				stopWriting <- true

				if errRead == io.EOF {
					log.Printf("PubSubClient: manageConnectivity: EOF encountered")
				} else {
					log.Printf("PubSubClient: manageConnectivity: error")
				}

				if p.quitting {
					log.Printf("PubSubClient: manageConnectivity: quitting: signalling done & stopping loop")
					p.done <- true
					return
				}

				continueStuffs = false
			} else {
				message := pubsub.MsgformatDecode(messageRaw)

				if !p.quitting {
					p.Notifications <- message
				}
			}
		}
	}
}

func (p *PubSubClient) handleWrites(conn net.Conn, stop chan bool) {
	defer log.Printf("PubSubClient: handleWrites: stopping")

	for {
		select {
		case <-p.sendQueue.ReceiveAvailable:
			packet := ""
			for _, message := range p.sendQueue.ReceiveAndClear() {
				packet += message
			}

			if _, err := conn.Write([]byte(packet)); err != nil {
				log.Printf("PubSubClient: handleWrites: %s", err.Error())
				return
			}
		case packet := <-p.writeCh:
			if _, err := conn.Write([]byte(packet)); err != nil {
				log.Printf("PubSubClient: handleWrites: %s", err.Error())
				return
			}
		case <-stop:
			return
		}
	}

}

func (p *PubSubClient) sendSubscriptionMessage(topic string) {
	log.Printf("PubSubClient: sendSubscriptionMessage: subscribing to %s", topic)

	packet := pubsub.MsgformatEncode([]string{"SUB", topic})

	p.writeCh <- packet
}
