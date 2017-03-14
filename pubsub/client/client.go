package client

import (
	"bufio"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/pubsub"
	"github.com/function61/pyramid/pubsub/partitionedlossyqueue"
	"github.com/function61/pyramid/util/stringslicediff"
	"github.com/jpillora/backoff"
	"log"
	"net"
	"sync"
	"time"
)

type PubSubClient struct {
	Notifications chan []string

	writeCh              chan string
	done                 chan bool
	quitting             bool
	subscribedTopics     []string
	sendQueue            *partitionedlossyqueue.Queue
	intentionalClose     chan bool
	disconnected         chan error
	subscriptionsChanged chan bool
	incomingMessages     chan []string
}

func New(serverAddress string) *PubSubClient {
	p := &PubSubClient{
		Notifications: make(chan []string, 100),

		writeCh:              make(chan string, 10),
		done:                 make(chan bool),
		quitting:             false,
		subscribedTopics:     []string{},
		sendQueue:            partitionedlossyqueue.New(),
		intentionalClose:     make(chan bool, 1),
		disconnected:         make(chan error, 1),
		subscriptionsChanged: make(chan bool, 1),
		incomingMessages:     make(chan []string, 10),
	}

	go p.reconnectForeverUntilStopped(serverAddress)

	return p
}

// this runs in a separate goroutine forever (regardless of disconnects), unless
// told to stop by Close(). signals done by posting to "done" channel
func (p *PubSubClient) reconnectForeverUntilStopped(serverAddress string) {
	defer func() { p.done <- true }()

	reconnectBackoff := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	// keep trying to connect forever, unless intentional disconnect
	for {
		if err := p.reconnect(serverAddress); err != nil {
			backoffDuration := reconnectBackoff.Duration()
			log.Printf("PubSubClient: reconnecting in %s: %s", backoffDuration, err)
			time.Sleep(backoffDuration)
		} else { // no error => intentional disconnect => quitting
			return
		}
	}
}

// returns when disconnected.
// error = nil if intentional disconnect.
// error != nil if I/O error.
func (p *PubSubClient) reconnect(serverAddress string) error {
	conn, err := net.Dial("tcp", serverAddress)
	if err != nil {
		return err
	}

	writerAndReaderStopped := &sync.WaitGroup{}
	writerAndReaderStopped.Add(2)
	stopWriter := make(chan bool, 2)

	// channels are safe to drain because we know for a fact that
	// writer and reader are stopped at this point
	p.drainChannelsBecauseReconnected()

	// will quit on I/O error and post to disconnected
	go p.handleWrites(conn, stopWriter, writerAndReaderStopped)
	// reader doesn't need a stop channel, because socket closing
	// immediately unblocks the reader. reader publishes to incomingMessages
	go p.handleReads(conn, writerAndReaderStopped)

	authMsg := pubsub.MsgformatEncode([]string{"AUTH", config.AUTH_TOKEN})
	p.writeCh <- authMsg

	// trigger resync of subscriptionsChanged
	select {
	case p.subscriptionsChanged <- true:
	default:
		// subscriptionsChanged message already in flight. we need to prepare for this
		// even though we drained this channel - another thread could race to Subscribe()
	}

	subscriptionsSentToServer := []string{}

	for {
		select {
		case <-p.intentionalClose:
			// let us know that the following I/O error is expected
			// and that we should not try to reconnect
			p.quitting = true

			// makes writer send BYE and call conn.Close()
			// also makes the writer stop
			stopWriter <- true
		case err := <-p.disconnected:
			log.Printf("PubSubClient: socket I/O error: %s. Disconnecting.", err.Error())

			// we could've received this from either writer or reader I/O error.
			// reader always notices its I/O error so we don't need to signal it,
			// but writer we need to signal specifically for stopping. it doesn't
			// matter if writer already exited because this is a buffered channel.
			stopWriter <- true

			writerAndReaderStopped.Wait()

			if p.quitting {
				return nil
			}

			// now we know that socket reader and writer has stopped, that we
			// are the only goroutine managing this stuff so return error so
			// we'll try reconnecting
			return err
		case message := <-p.incomingMessages:
			// TODO: we could have messages buffered in this channel and handled
			// disconnect before. draing messages somehow, so after reconnect
			// we don't encounter messages that belonged to previous connection?

			// just blindly emit to Notifications
			p.Notifications <- message
		case <-p.subscriptionsChanged:
			diff := stringslicediff.Diff(subscriptionsSentToServer, p.subscribedTopics)

			for _, topic := range diff.Added {
				p.writeCh <- pubsub.MsgformatEncode([]string{"SUB", topic})
			}

			for range diff.Removed {
				panic("Unsubscribe not implemented yet")
			}
		}
	}

	// indicates we should stop
	return nil
}

func (p *PubSubClient) Subscribe(topic string) {
	p.subscribedTopics = append(p.subscribedTopics, topic)

	select {
	case p.subscriptionsChanged <- true:
		// noop
	default:
		// subscriptionsChanged message already in flight
	}
}

// guaranteed to never block, but can lose all but the latest message per topic
func (p *PubSubClient) Publish(topic string, message string) {
	p.sendQueue.Put(topic, pubsub.MsgformatEncode([]string{"PUB", topic, message}))
}

// FIXME: close while disconnected not working
func (p *PubSubClient) Close() {
	log.Printf("PubSubClient: disconnecting")

	p.intentionalClose <- true

	log.Printf("PubSubClient: closed")

	<-p.done
}

func (p *PubSubClient) handleReads(conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()

	reader := bufio.NewReader(conn)

	for {
		// will listen for message to process ending in newline (\n)
		messageRaw, errRead := reader.ReadString('\n')
		if errRead != nil {
			p.disconnected <- errRead
			return
		}

		message := pubsub.MsgformatDecode(messageRaw)

		p.incomingMessages <- message
	}
}

func (p *PubSubClient) handleWrites(conn net.Conn, stop chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Printf("PubSubClient: handleWrites: stopping")

	for {
		select {
		case <-p.sendQueue.ReceiveAvailable: // sends from lossy queue
			packet := ""
			for _, message := range p.sendQueue.ReceiveAndClear() {
				packet += message
			}

			if _, err := conn.Write([]byte(packet)); err != nil {
				p.disconnected <- err
				return
			}
		case packet := <-p.writeCh:
			if _, err := conn.Write([]byte(packet)); err != nil {
				p.disconnected <- err
				return
			}
		case <-stop:
			// not interested about I/O errors here because we want
			// to disconnect
			bye := pubsub.MsgformatEncode([]string{"BYE"})
			conn.Write([]byte(bye))
			conn.Close()
			return
		}
	}
}

func (p *PubSubClient) drainChannelsBecauseReconnected() {
	p.sendQueue.ReceiveAndClear()

	for {
		select {
		case <-p.writeCh:
		case <-p.sendQueue.ReceiveAvailable:
		case <-p.disconnected:
		case <-p.incomingMessages:
		case <-p.subscriptionsChanged:
		default:
			return
		}
	}
}
