package client

import (
	"bufio"
	"crypto/tls"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/pubsub/msgformat"
	"github.com/function61/eventhorizon/pubsub/partitionedlossyqueue"
	"github.com/function61/eventhorizon/util/stringslice"
	"github.com/function61/eventhorizon/util/stringslicediff"
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
	confCtx              *config.Context
}

func New(confCtx *config.Context) *PubSubClient {
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
		confCtx:              confCtx,
	}

	go p.reconnectForeverUntilStopped(confCtx.GetPubSubServerAddr())

	return p
}

// this runs in a separate goroutine forever (regardless of disconnects), unless
// told to stop by Close(). signals done by posting to "done" channel
func (p *PubSubClient) reconnectForeverUntilStopped(serverAddress string) {
	defer func() { p.done <- true }()

	log.Printf("PubSubClient: connecting to %s", serverAddress)

	reconnectBackoff := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	// keep trying to connect forever, unless intentional disconnect
	for {
		if p.quitting {
			return
		}

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
	tlsConfig := tls.Config{
		RootCAs: p.confCtx.GetCaCertificates(),
	}

	conn, err := tls.Dial("tcp", serverAddress, &tlsConfig)
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

	authMsg := msgformat.Serialize([]string{"AUTH", p.confCtx.AuthToken()})
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
			// just blindly emit to Notifications
			p.Notifications <- message
		case <-p.subscriptionsChanged:
			diff := stringslicediff.Diff(subscriptionsSentToServer, p.subscribedTopics)

			for _, topic := range diff.Added {
				p.writeCh <- msgformat.Serialize([]string{"SUB", topic})
			}

			for range diff.Removed {
				panic("Unsubscribe not implemented yet")
			}
		}
	}
}

func (p *PubSubClient) Subscribe(topic string) {
	if stringslice.ItemIndex(topic, p.subscribedTopics) != -1 {
		log.Printf("PubSubClient: Subscribe: was already subscribed to topic %s", topic)
		return
	}

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
	p.sendQueue.Put(topic, msgformat.Serialize([]string{"PUB", topic, message}))
}

func (p *PubSubClient) Close() {
	log.Printf("PubSubClient: disconnecting")

	// let us know that the following I/O errors are expected
	// and that we should not try to reconnect
	p.quitting = true

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

		message := msgformat.Deserialize(messageRaw)

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
			bye := msgformat.Serialize([]string{"BYE"})
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
